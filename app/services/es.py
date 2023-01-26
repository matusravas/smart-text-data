import asyncio
import json
import logging
from datetime import datetime as dt
from typing import Coroutine, List, Tuple, Iterator

from app import ID_FIELD, INTEGRITY_THRESHOLD
from app.apis.es import get_last_indexed_timestamp, bulk
from app.model import EventLoop, File, BulkResult, EBulkResult, DataIndexer
from app.utils.decorators.services import service

logger = logging.getLogger(__name__)

def obtain_last_indexed_timestamp() -> float:
    return __obtain_last_indexed_timestamp()

def bulk_files_to_es() -> List[BulkResult]:
    return __bulk_files_to_es()

def check_results_and_post_last_timestamp(results: List[BulkResult]) -> None:
    now = dt.now().timestamp()
    file = results[0].file if len(results) > 0 else None
    # Todo check if all results are OK, or Integrity. 
    # Todo if Integrity check if all items were integrty errors or not.
    # Todo If not all Inegrity send info email to check logs and do not save last timestamp !
    # todo if ERROR make the error file timestamp as last indexed timestamp
    # results MUST be sorted by file ctimes "desc" oldest should be last
    for result in results:
        logger.info(f'{result.file.path}, result = {result.result.value}')
        if result.result == EBulkResult.INDEXED: pass
        elif result.result == EBulkResult.ERROR:
            #? send info mail
            logger.warning(f'File {result.file.name} finished with error')
            now = result.file.ctime
        else: #! result is EBulkResult.UNKNOWN
            integrity_counter = 0
            for partial_result in result.items: #! partial_result stores _id of item as well
                if partial_result.result == EBulkResult.INTEGRITY:
                    integrity_counter += 1
                elif partial_result.result == EBulkResult.ERROR: # no other option else could be used
                    now = result.file.ctime
                    break
                
                if integrity_counter > INTEGRITY_THRESHOLD:
                    now = result.file.ctime
                    break
    timestamp = now
    if file:
        data_indexer = DataIndexer(file, timestamp)
        logger.info('Data prepared to be indexed: ')
        logger.info(data_indexer.serialize())
        status = __post_last_indexed_timestamp(data_indexer)
        if status:
            logger.info('Data indexed successfully stored to elasticsearch')
        else:
            logger.warning('Data not indexed to elasticsearch')
    else:
        logger.warning('Data not indexed: ')
        
    return None


@service
def __obtain_last_indexed_timestamp(loop: EventLoop) -> float:
    last_timestamp = loop.run_until_complete(get_last_indexed_timestamp())
    return last_timestamp


@service
def __post_last_indexed_timestamp(loop: EventLoop, data_indexer: DataIndexer) -> bool:
    last_timestamp = loop.run_until_complete(get_last_indexed_timestamp())
    return last_timestamp


@service
def __bulk_files_to_es(loop: EventLoop) -> List[BulkResult]:
    coroutines: List[Coroutine] = []
    files: List[File] = []
    for data, file in read_files():
        logger.info(file.path)
        logger.info(file.ctime)
        files.append(file)
        rows = []
        for i, row in enumerate(data, start=1):
            _id = row.get(ID_FIELD, i)
            rows.append(json.dumps({'create': {'_id': _id}})) # use index instead of create to update existing docs
            rows.append(json.dumps(row, ensure_ascii=False))
        bulk_data = '\n'.join(rows) + '\n'
        coroutines.append(bulk(bulk_data, file, i))
    
    tasks = asyncio.gather(*coroutines)
    results: List[BulkResult] = loop.run_until_complete(tasks)
    return results 

        

from app.services.reader import read_files