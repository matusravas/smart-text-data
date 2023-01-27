import asyncio
import json
import logging
from datetime import datetime as dt
from typing import Coroutine, List, Union

from app import BULK_ACTION, ID_FIELD, TIMESTAMP
from app.apis.es import (bulk, get_last_indexed_timestamp,
                         post_last_indexed_timestamp)
from app.model import (BulkResult, DataIndexer, EBulkResult,
                       EDocResult, EventLoop, File)
from app.utils.decorators.services import service

logger = logging.getLogger(__name__)

def obtain_last_indexed_timestamp() -> Union[float, None]:
    return __obtain_last_indexed_timestamp()

def bulk_files_to_es() -> bool:
    results: List[BulkResult] = __bulk_files_to_es()
    status = __check_results_and_post_last_timestamp(results)
    return status


@service
def __obtain_last_indexed_timestamp(loop: EventLoop) -> Union[float, None]:
    last_timestamp = loop.run_until_complete(get_last_indexed_timestamp())
    return last_timestamp


@service
def __save_last_indexed_timestamp(loop: EventLoop, data_indexer: DataIndexer) -> bool:
    status = loop.run_until_complete(post_last_indexed_timestamp(data_indexer))
    return status


@service
def __bulk_files_to_es(loop: EventLoop) -> List[BulkResult]:
    coroutines: List[Coroutine] = []
    files: List[File] = []
    for data, file in read_files():
        logger.info(file.path)
        logger.info(dt.fromtimestamp(file.ctime).isoformat())
        files.append(file)
        rows = []
        for i, row in enumerate(data, start=1):
            _id = row.get(ID_FIELD, i)
            rows.append(json.dumps({BULK_ACTION: {'_id': _id}})) # use index instead of create to update existing docs
            rows.append(json.dumps(row, ensure_ascii=False))
        bulk_data = '\n'.join(rows) + '\n'
        coroutines.append(bulk(bulk_data, file, i))
    
    tasks = asyncio.gather(*coroutines)
    results: List[BulkResult] = loop.run_until_complete(tasks)
    return results 


def __check_results_and_post_last_timestamp(results: List[BulkResult]) -> bool:
    # results MUST be sorted by file ctimes "asc" oldest should be last
    for result in results:
        logger.info(f'File: {result.file.name}, result: {result.result.value}')
        if result.result == EBulkResult.INDEXED: pass
        
        elif result.result == EBulkResult.ERROR:
            #? send info mail
            logger.error(f'Error indexing whole file: {result.file.name}')
        
        elif result.result == EBulkResult.INDEXED_UPDATE and result.items:
            error_counter = 0
            update_counter = 0
            for partial_result in result.items:
                if partial_result.result == EDocResult.DOC_UPDATED:
                    update_counter += 1
                    logger.warning(f'Indexing update on _id: {partial_result._id}')
                elif partial_result.result == EDocResult.DOC_ERROR: # no other option else could be used
                    error_counter += 1
                    logger.error(f'Indexing error on _id: {partial_result._id}')
            result.n_errors = error_counter
            result.n_indexed_updates = update_counter
            if update_counter == 0:
                result.result = EBulkResult.INDEXED
            elif result.n_items//2 > update_counter:
                result.result = EBulkResult.INDEXED_UPDATE
            else:
                result.result = EBulkResult.UPDATED if update_counter > error_counter \
                    else EBulkResult.ERROR if error_counter > update_counter else EBulkResult.UNKNOWN
        
        elif result.result == EBulkResult.UNKNOWN and result.items:
            error_counter = 0
            conflict_counter = 0
            for partial_result in result.items:
                if partial_result.result == EDocResult.DOC_CONFLICT:
                    conflict_counter += 1
                    logger.warning(f'Indexing integrity_conflict on _id: {partial_result._id}')
                elif partial_result.result == EDocResult.ERROR: # no other option else could be used
                    error_counter += 1
                    logger.error(f'Indexing error on _id: {partial_result._id}')
            result.n_errors = error_counter
            result.n_integrity_conflicts = conflict_counter
            if result.n_items//2 > conflict_counter:
                result.result = EBulkResult.INDEXED_CONFLICTS
            else:
                result.result = EBulkResult.CONFLICTED if conflict_counter > error_counter \
                    else EBulkResult.ERROR if error_counter > conflict_counter else EBulkResult.UNKNOWN
        else:
            result.result = EBulkResult.FATAL
            logger.error(f'Unexpected processing error. File: {result.file.name}')

    data_indexer = DataIndexer(TIMESTAMP, results)
    logger.info('Output data prepared to be indexed...')
    logger.info(data_indexer.serialize())
    status = __save_last_indexed_timestamp(data_indexer)
    if status:
        logger.info('Output data indexed to elasticsearch')
    else:
        logger.error('Output data not indexed to elasticsearch')
        
    return status
     

from app.services.reader import read_files
