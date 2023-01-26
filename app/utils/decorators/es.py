import logging
from functools import wraps
from typing import Callable

from aiohttp import ClientConnectorError

from app import es_hosts, base_es_url, es_headers, ssl
from app.model import ESData

logger = logging.getLogger(__name__)

DEFAULT_HOST = 'localhost:9200'

def use_multiple_es_hosts(fn: Callable) -> Callable:
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        i = 0
        n = len(es_hosts) if es_hosts and isinstance(es_hosts, list) else 1 
        condition = True if es_hosts and isinstance(es_hosts, list) and len(es_hosts) > 0 else False
        if not condition:
            logger.info(f'Elasticsearch host NOT specified, default host used: {DEFAULT_HOST}')
        while i < n:
            try:
                es_url = es_hosts[i] if condition else base_es_url
                es_data = ESData(es_url, es_headers, ssl)
                es_func_response = await fn(es_data, *args, **kwargs)
                break
            except ClientConnectorError as _:
                host = es_hosts[i] if condition else DEFAULT_HOST
                logger.error(f'ClientConnectorError Elasticsearch host {host} is unreachable')
                if i == n - 1: 
                    raise Exception('ClientConnectorError can not connect to any of the specified Elasticsearch hosts')
                i += 1
        return es_func_response
    return wrapper