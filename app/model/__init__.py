from asyncio import AbstractEventLoop, ProactorEventLoop
from enum import Enum
from ssl import SSLContext
from typing import Dict, Iterator, List, Optional, TypeVar, Union

EventLoop = Union[AbstractEventLoop, ProactorEventLoop]


class EBulkResult(Enum):
    INDEXED = 'INDEXED'
    INTEGRITY = 'INTEGRITY'
    INDEXED_INTEGRITY = 'INDEXED_INTEGRITY'
    ERROR = 'ERROR'
    UNKNOWN = 'UNKNOWN'
    FATAL = 'FATAL' # error in code. NEVER happens.


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
    def __init__(self, result: EBulkResult, _id: Union[str, None]) -> None:
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
        self.n_integrity_errors = None
    
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
        if self.n_integrity_errors is not None:
            obj["n_integrity_errors"] = self.n_integrity_errors
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