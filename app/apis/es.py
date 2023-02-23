import json
import logging
import aiohttp
from typing import Union
from app.model import ESData, BulkResult, File, DataIndexer
from app.utils.decorators.es import use_multiple_es_hosts
from .utils.helpers import parse_bulk_result

logger = logging.getLogger(__name__)


@use_multiple_es_hosts
async def get_last_indexed_timestamp(es: ESData) -> Union[float, None]:
    body_file = 'last-indexed-timestamp.json'
    with open(es.queries + body_file) as f:
        body = json.load(f)

    url = f'{es.url}/st-data-indexer/_search'
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=es.headers, json=body, ssl=es.ssl) as resp:
            data = await resp.json()
    
    if not (data and 'hits' in data and 'hits' in data['hits'] and data['hits']['hits'] and len(data['hits']['hits']) > 0):
        return None
    return data['hits']['hits'][0]['_source'].get('timestamp', None)


@use_multiple_es_hosts
async def post_last_indexed_timestamp(es: ESData, data_indexer: DataIndexer) -> bool:
    body = data_indexer.serialize()
    url = f'{es.url}/st-data-indexer/_doc'
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=es.headers, json=body, ssl=es.ssl) as resp:
            data = await resp.json()
    if data and 'result' in data and data['result'] == 'created':
        return True
    return False


@use_multiple_es_hosts
async def bulk(es: ESData, bulk_data: str, file: File, n_items: int) -> BulkResult:
    url = f'{es.url}/{file.source.value.get("index")}/_bulk'
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=es.headers, data=bulk_data, ssl=es.ssl) as resp:
            data = await resp.json()
    bulk_result = parse_bulk_result(data, file, n_items)
    return bulk_result
        