import hashlib
import logging
from datetime import datetime as dt
from typing import Dict, Iterator, List, Optional, Union, Any

import pandas as pd

from app import ACTION, TIMESTAMP
from app.model import (NULL_VALUES, BulkResult, BulkResultPartial, EBulkResult,
                       EDocResult, File)

logger = logging.getLogger(__name__)


def parse_bulk_result(data: Dict, file: File, n_items: int) -> BulkResult:
    if 'errors' in data and data['errors'] and 'items' in data and len(data['items']):
        error_counter = 0
        for item in data['items']:
            if 'index' in item and item['index'] and 'error' in item['index']: 
                error_counter += 1
                logger.error(item)
            if error_counter >= 5: break
    
    if data and (('errors' in data and not data['errors']) and 'items' in data and len(data['items']) == n_items):
        result = EBulkResult.INDEXED #EBulkResult.INDEXED_UPDATE if ACTION == BULK_ACTION.INDEX else EBulkResult.INDEXED
    else:
        result = EBulkResult.ERROR if not ('items' in data and data['items'] and isinstance(data['items'], list))\
            else EBulkResult.UNKNOWN
    bulk_hash = generate_timestamp_hash(TIMESTAMP)
    return BulkResult(TIMESTAMP, bulk_hash, file, result, n_items, items=evaluate_every_bulk_result(data['items']))


def evaluate_every_bulk_result(items: List[Dict]) -> Iterator[BulkResultPartial]:
    for item in items:
        obj = item.get(ACTION.value, {})
        _id = obj.get('_id', None)
        yield \
            BulkResultPartial(EDocResult.DOC_INSERTED, _id) \
                if obj.get('status', None) == 201\
            else BulkResultPartial(EDocResult.DOC_UPDATED, _id) \
                if obj.get('status', None) == 200 and obj.get('result', None) == 'updated'\
            else BulkResultPartial(EDocResult.DOC_CONFLICT, _id) \
                if obj.get('status', None) == 409 or (obj.get('status', None) == 200 and obj.get('result', None) == 'noop')\
            else BulkResultPartial(EDocResult.DOC_ERROR, _id)
            

def normalize_row(row: Dict, file: File):
    normalized_row = {}
    for key, value in row.items():
        if value in NULL_VALUES or pd.isnull(value) or pd.isna(value):
            value = None
        elif isinstance(value, pd.Timestamp) or isinstance(value, dt): 
            value = value.isoformat()
        elif file.normalizer:
            value = file.normalizer(key, value)  
        normalized_row[' '.join(key.strip().split())] = value
    normalized_row['@uid'] = file.uid
    return normalized_row


def generate_timestamp_hash(timestamp: Optional[Union[int, float]]=None):
    timestamp_str = str(int(timestamp if timestamp else dt.now().timestamp()))
    timestamp_bytes = timestamp_str.encode('utf-8')
    hash_object = hashlib.sha256()
    hash_object.update(timestamp_bytes)
    hash_hex = hash_object.hexdigest()[:20]
    return hash_hex