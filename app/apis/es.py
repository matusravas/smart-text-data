import json
import logging
import aiohttp

from app.model import ESData, BulkResult, EBulkResult, File, DataIndexer
from app.utils.helpers import evaluate_bulk_results
from app.utils.decorators.es import use_multiple_es_hosts

logger = logging.getLogger(__name__)


@use_multiple_es_hosts
async def get_last_indexed_timestamp(es: ESData) -> bool:
    body_file = 'last-indexed-timestamp.json'
    with open(es.queries + body_file) as f:
        body = json.load(f)

    url = f'{es.url}/data-indexer/_search'
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=es.headers, json=body, ssl=es.ssl) as resp:
            data = await resp.json()
    
    if not (data and 'hits' in data and 'hits' in data['hits'] and data['hits']['hits'] and len(data['hits']['hits']) > 0):
        return None


@use_multiple_es_hosts
async def save_last_indexed_timestamp(es: ESData, data_indexer: DataIndexer):
    body = data_indexer.serialize()
    url = f'{es.url}/data-indexer/_search'
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=es.headers, json=body, ssl=es.ssl) as resp:
            data = await resp.json()
    
    if data and 'status' in data and data['status'] == 'created':
        return True
    return False


@use_multiple_es_hosts
async def bulk(es: ESData, bulk_data: str, file: File, n_items: int) -> BulkResult:
    url = f'{es.url}/bekaert/_bulk'
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=es.headers, data=bulk_data, ssl=es.ssl) as resp:
            data = await resp.json()
            
    if data and (('errors' in data and not data['errors']) and 'items' in data and len(data['items']) == n_items):
        return BulkResult(file, EBulkResult.INDEXED)
    
    # elif data and (('errors' in data and data['errors']) and 'items' in data and data['items'] and isinstance(data['items'], list)):
    result = EBulkResult.ERROR if not ('items' in data and data['items'] and isinstance(data['items'], list)) else EBulkResult.UNKNOWN
    return BulkResult(file, result, items=evaluate_bulk_results(data['items']))
        