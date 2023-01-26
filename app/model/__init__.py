from asyncio import AbstractEventLoop, ProactorEventLoop
from enum import Enum
from ssl import SSLContext
from typing import Dict, Generic, List, Optional, TypeVar, Union

EventLoop = Union[AbstractEventLoop, ProactorEventLoop]

class EBulkResult(Enum):
    INDEXED = 'INDEXED'
    INTEGRITY = 'INTEGRITY'
    ERROR = 'ERROR'
    UNKNOWN = 'UNKNOWN'


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
    def __init__(self, file: File, result: EBulkResult, items: Optional[List[BulkResultPartial]]=None) -> None:
        self.file = file
        self.result = result
        self.items = items
        

class DataIndexer():
    def __init__(self, file: File, timestamp: float) -> None:
        self.file = file
        self.timestamp = timestamp
        
    def serialize(self):
        return {
            "file_name": self.file.name
            ,"file_path": self.file.path
            ,"file_ctime": self.file.ctime
            ,"timestamp": self.timestamp
        }
        