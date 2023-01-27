from typing import Dict, List, Iterator
from app import ACTION
from app.model import BulkResult, BulkResultPartial, EBulkResult, EDocResult, File


def parse_bulk_result(data: Dict, file: File, n_items: int) -> BulkResult:
    if data and (('errors' in data and not data['errors']) and 'items' in data and len(data['items']) == n_items):
        result = EBulkResult.INDEXED #EBulkResult.INDEXED_UPDATE if ACTION == BULK_ACTION.INDEX else EBulkResult.INDEXED
    else:
        result = EBulkResult.ERROR if not ('items' in data and data['items'] and isinstance(data['items'], list))\
            else EBulkResult.UNKNOWN
    
    return BulkResult(file, result, n_items, items=evaluate_every_bulk_result(data['items']))


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