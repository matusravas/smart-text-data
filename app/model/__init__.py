from asyncio import AbstractEventLoop, ProactorEventLoop
from enum import Enum
from ssl import SSLContext
from typing import Dict, Iterator, List, Optional, Union, Callable
from .validators import sap_analyzer_validator


EventLoop = Union[AbstractEventLoop, ProactorEventLoop]


class BULK_ACTION(Enum):
    INDEX = 'index'
    CREATE = 'create'

class EBulkResult(Enum):
    INDEXED = 'INDEXED'
    UNKNOWN = 'UNKNOWN'
    ERROR = 'ERROR'
    FATAL = 'FATAL' # error in code. should NEVER happen.


class EDocResult(Enum):
    DOC_INSERTED = 'DOC_INSERTED'
    DOC_UPDATED = 'DOC_UPDATED'
    DOC_CONFLICT = 'DOC_CONFLICT'
    DOC_ERROR = 'DOC_ERROR'


class ESData():
    def __init__(self, url: str, headers: Dict, ssl: SSLContext) -> None:
        self.url = url
        self.headers = headers
        self.ssl = ssl
        self.queries = 'app/queries/'


VALIDATOR_FIELD = 'validator'
NULL_VALUES = ['#'] 

class Source(Enum):
    SAP_ANALYZER = {
        'alias': 'SAP Analyzer'
        , 'index': 'st-sap-analyzer'
        , '_id': 'Maintenance Order'
        , 'search_field': 'Maintenance Order (Desc)'
        , 'date_field': None
        , VALIDATOR_FIELD: sap_analyzer_validator
        }
    VAS = {
        'alias': 'VAS'
        , 'index': 'st-vas'
        , '_id': 'Údržbárska zákazka'
        , 'search_field': 'Popis poruchy'
        , 'date_field': 'Ukončenie VAS'
        }
    SAP = {
        'alias': 'SAP'
        , 'index': 'st-sap'
        , '_id': 'Zákazka'
        , 'search_field': 'Kr_text'
        , 'date_field': 'Koniec poruchy'
        }


SOURCES_EXPECTED_COLUMNS = {
        Source.SAP_ANALYZER: {'columns': ['Maintenance Order (Desc)', 'Employee', 'Company ID-EMP'], 'header_idx': 0}
        ,Source.VAS: {'columns': ['Popis poruchy', 'VAS číslo', 'Stroj'], 'header_idx': 0}
        ,Source.SAP: {'columns': ['Kr.text', 'Zákazka'], 'header_idx': 0}
}

class File():
    def __init__(self, path: str, name: str, ctime: float) -> None:
        self.path = path
        self.name = name
        self.ctime = ctime
        self.source: Source = None
        self.id_field: Union[None, str] = None
        self.row_validator: Union[None, Callable[[Dict], bool]] = None


class BulkResultPartial():
    def __init__(self, result: EDocResult, _id: Union[str, None]) -> None:
        self.result = result
        self._id = _id


class BulkResult():
    def __init__(self, bulk_timestamp: float, bulk_hash: str, file: File
                 , result: EBulkResult, n_items: int
                 , items: Optional[Iterator[BulkResultPartial]]=None) -> None:
        self.bulk_timestamp = bulk_timestamp
        self.bulk_hash = bulk_hash
        self.file = file
        self.result = result
        self.n_items = n_items
        self.items = items
        self.n_inserted = None
        self.n_updated = None
        self.n_conflicted = None
        self.n_errors = None
    
    def serialize(self):
        source = self.file.source.value
        source.pop(VALIDATOR_FIELD, None)
        obj = {
            "timestamp": self.bulk_timestamp
            ,"bulk": self.bulk_hash
            ,"file": self.file.name
            # ,"file_path": self.file.path
            ,"source": source
            ,"file_ctime": self.file.ctime
            ,"result": self.result.value
            ,"n_items": self.n_items
        }
        if self.n_inserted is not None:
            obj["inserts"] = self.n_inserted
        if self.n_updated is not None:
            obj["updates"] = self.n_updated
        if self.n_conflicted is not None:
            obj["conflicts"] = self.n_conflicted
        if self.n_errors is not None:
            obj["errors"] = self.n_errors
        return obj
        

# class DataIndexer():
#     def __init__(self, timestamp: float, results: List[BulkResult]) -> None:
#         self.timestamp = timestamp
#         self.results = results
        
#     def serialize(self):
#         return {
#             "timestamp": self.timestamp
#             ,"files": [q.serialize() for q in self.results]
#         }