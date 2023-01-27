from typing import Dict, Any, List, Iterator
import pandas as pd
from app.model import EBulkResult, BulkResultPartial


def parse_field(field: Any):
    #! Todo Koniec_poruchy could be NaT. It is the date attribut, how to handle it?
    if isinstance(field, pd.Timestamp): return field.isoformat()
    elif pd.isnull(field): return None
    else: return field
    

def evaluate_bulk_results(items: List[Dict]) -> Iterator[BulkResultPartial]:
    for item in items:
        obj = item.get('create', {})
        _id = obj.get('_id', None)
        yield BulkResultPartial(EBulkResult.INDEXED, _id) if obj.get('status', None) == 201\
            else BulkResultPartial(EBulkResult.INTEGRITY, _id) if obj.get('status', None) == 409\
            else BulkResultPartial(EBulkResult.ERROR, _id) # 409 = integrity error, resource already exists