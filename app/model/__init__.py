from asyncio import AbstractEventLoop, ProactorEventLoop
from enum import Enum
from ssl import SSLContext
from typing import Dict, Iterator, List, Optional, TypeVar, Union

EventLoop = Union[AbstractEventLoop, ProactorEventLoop]


class EBulkResult(Enum):
    INDEXED = 'INDEXED'
    INDEXED_UPDATE = 'INDEXED_UPDATE'
    INDEXED_CONFLICTS = 'INDEXED_CONFLICTS'
    UPDATED = 'UPDATED'
    CONFLICTED = 'CONFLICTED'
    UNKNOWN = 'UNKNOWN'
    ERROR = 'ERROR'
    FATAL = 'FATAL' # error in code. NEVER happens.


class EDocResult(Enum):
    DOC_INDEXED = 'DOC_INDEXED'
    DOC_UPDATED = 'DOC_UPDATED'
    DOC_CONFLICT = 'DOC_CONFLICT'
    DOC_ERROR = 'DOC_ERROR'


class ESData():
    def __init__(self, url: str, headers: Dict, ssl: SSLContext) -> None:
        self.url = url
        self.headers = headers
        self.ssl = ssl
        self.queries = 'app/queries/'


class File():
    def __init__(self, path: str, name: str, ctime: float) -> None:
        self.path = path
        self.name = name
        self.ctime = ctime


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
        self.n_errors = None
        self.n_integrity_conflicts = None
        self.n_indexed_updates = None
    
    def serialize(self):
        obj = {
            "file_name": self.file.name
            # ,"file_path": self.file.path
            ,"file_ctime": self.file.ctime
            ,"result": self.result.value,
            "n_items": self.n_items
        }
        if self.n_errors is not None:
            obj["n_errors"] = self.n_errors
        if self.n_integrity_conflicts is not None:
            obj["n_conflicts"] = self.n_integrity_conflicts
        if self.n_indexed_updates is not None:
            obj["n_updates"] = self.n_indexed_updates
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