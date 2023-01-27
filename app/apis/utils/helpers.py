from typing import Dict, List, Iterator
from app import BULK_ACTION
from app.model import BulkResult, BulkResultPartial, EBulkResult, EDocResult, File


def parse_bulk_result(data: Dict, file: File, n_items: int) -> BulkResult:
    if BULK_ACTION == 'index':
        if data and (('errors' in data and not data['errors']) and 'items' in data and len(data['items']) == n_items):
            return BulkResult(file, EBulkResult.INDEXED_UPDATE, n_items, items=evaluate_every_bulk_result(data['items']))
        else:
            result = EBulkResult.ERROR if not ('items' in data and data['items'] and isinstance(data['items'], list))\
                else EBulkResult.UNKNOWN
            return BulkResult(file, result, n_items, items=evaluate_every_bulk_result(data['items']))
            
    elif BULK_ACTION == 'create':
        if data and (('errors' in data and not data['errors']) and 'items' in data and len(data['items']) == n_items):
            return BulkResult(file, EBulkResult.INDEXED, n_items)
        else:
            result = EBulkResult.ERROR if not ('items' in data and data['items'] and isinstance(data['items'], list))\
                else EBulkResult.UNKNOWN
            return BulkResult(file, result, n_items, items=evaluate_every_bulk_result(data['items']))


def evaluate_every_bulk_result(items: List[Dict]) -> Iterator[BulkResultPartial]:
    for item in items:
        obj = item.get(BULK_ACTION, {})
        _id = obj.get('_id', None)
        yield \
            BulkResultPartial(EDocResult.DOC_INDEXED, _id) if obj.get('status', None) == 201\
            else BulkResultPartial(EDocResult.DOC_UPDATED, _id) if obj.get('status', None) == 200\
            else BulkResultPartial(EDocResult.DOC_CONFLICT, _id) if obj.get('status', None) == 409\
            else BulkResultPartial(EDocResult.DOC_ERROR, _id) # 409 