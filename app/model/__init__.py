from asyncio import AbstractEventLoop, ProactorEventLoop
from enum import Enum
from ssl import SSLContext
from typing import Dict, Iterator, List, Optional, Union

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


class Source(Enum):
    SAP_ANALYZER = {'alias': 'SAP Analyzer', 'index': 'st-sap-analyzer', '_id': 'Maintenance Order'}
    VAS = {'alias': 'VAS', 'index': 'st-vas', '_id': 'Údržbárska zákazka'}
    SAP = {'alias': 'SAP', 'index': 'st-sap', '_id': 'Hlásenie'}
    

class File():
    def __init__(self, path: str, name: str, ctime: float) -> None:
        self.path = path
        self.name = name
        self.ctime = ctime
        self.source: Source = None
        self.id_field: str = None


class BulkResultPartial():
    def __init__(self, result: EDocResult, _id: Union[str, None]) -> None:
        self.result = result
        self._id = _id


class BulkResult():
    def __init__(self, file: File, result: EBulkResult, n_items: int,
                 items: Optional[Iterator[BulkResultPartial]]=None) -> None:
        self.file = file
        self.result = result
        self.n_items = n_items
        self.items = items
        self.n_inserted = None
        self.n_updated = None
        self.n_conflicted = None
        self.n_errors = None
    
    def serialize(self):
        obj = {
            "file_name": self.file.name
            # ,"file_path": self.file.path
            ,"source": self.file.source.value
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
        

class DataIndexer():
    def __init__(self, timestamp: float, results: List[BulkResult]) -> None:
        self.timestamp = timestamp
        self.results = results
        
    def serialize(self):
        return {
            "timestamp": self.timestamp
            ,"files": [q.serialize() for q in self.results]
        }