import asyncio
import json
import logging
from datetime import datetime as dt
from typing import Coroutine, List, Union

import pandas as pd

from app import ACTION, BULK_ACTION
from app.apis.es import (bulk, get_last_indexed_timestamp,
                         post_bulk_result)
from app.apis.utils.helpers import normalize_row
from app.model import BulkResult, EBulkResult, EDocResult, EventLoop
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
def __save_bulk_results(loop: EventLoop, bulk_results: List[BulkResult]) -> bool:
    tasks = asyncio.gather(*(post_bulk_result(bulk_result) for bulk_result in bulk_results))
    statuses = loop.run_until_complete(tasks)
    return all(statuses)


@service
def __bulk_files_to_es(loop: EventLoop) -> List[BulkResult]:
    coroutines: List[Coroutine] = []
    for data, file in read_files():
        logger.info('Bulking data for file:')
        logger.info(file.path)
        logger.info(dt.fromtimestamp(file.ctime).isoformat())
        rows = []
        for i, row in enumerate(data, start=1):
            _id = row.get(file.id_field, None)
            if not _id or pd.isna(_id): continue
            id_with_timestamp = '{}-{}'.format(_id, file.rtime)
            if file.row_validator is not None and not file.row_validator(row): continue
            rows.append(json.dumps({ACTION.value: {'_id': id_with_timestamp}})) # use index instead of create to update existing docs
            normalized_row = normalize_row(row, file)
            rows.append(json.dumps(normalized_row, ensure_ascii=False))
        bulk_data = '\n'.join(rows) + '\n'
        coroutines.append(bulk(bulk_data, file, i))
    
    tasks = asyncio.gather(*coroutines)
    results: List[BulkResult] = loop.run_until_complete(tasks)
    return results 


def __check_results_and_post_last_timestamp(bulk_results: List[BulkResult]) -> bool:
    # results MUST be sorted by file creation times - ctimes "asc" oldest should be last
    for bulk_result in bulk_results:
        logger.info(f'File: {bulk_result.file.name}, result: {bulk_result.status.value}')
        if bulk_result.status in [EBulkResult.INDEXED, EBulkResult.UNKNOWN] and bulk_result.items:
            insert_counter = 0
            update_counter = 0
            error_counter = 0
            conflict_counter = 0
            for partial_result in bulk_result.items:
                if partial_result.result == EDocResult.DOC_INSERTED:
                    insert_counter += 1
                if partial_result.result == EDocResult.DOC_UPDATED:
                    update_counter += 1
                    logger.warning(f'Indexing update on _id: {partial_result._id}')
                if partial_result.result == EDocResult.DOC_CONFLICT:
                    conflict_counter += 1
                    logger.warning(f'Indexing integrity_conflict on _id: {partial_result._id}')
                elif partial_result.result == EDocResult.DOC_ERROR: # no other option else could be used
                    error_counter += 1
                    logger.error(f'Indexing error on _id: {partial_result._id}')
            bulk_result.n_inserted = insert_counter
            bulk_result.n_updated = update_counter
            bulk_result.n_errors = error_counter
            if ACTION == BULK_ACTION.CREATE:
                bulk_result.n_conflicted = conflict_counter
        
        elif bulk_result.status == EBulkResult.ERROR:
            #? send info mail
            logger.error(f'Error indexing whole file: {bulk_result.file.name}')
        
        else:
            bulk_result.status = EBulkResult.FATAL
            logger.error(f'Unexpected processing error. File: {bulk_result.file.name}')
    
    # data_indexer = DataIndexer(TIMESTAMP, results)
    if not bulk_results: 
        logger.info('No bulk data to be indexed...')
        # logger.info(data_indexer.serialize())
        return
    
    logger.info('Bulk data prepared to be indexed...')
    for bulk_result in bulk_results:
        logger.info(bulk_result.serialize())
    status = __save_bulk_results(bulk_results)
    if status:
        logger.info('Bulk data indexed to elasticsearch')
    else:
        logger.error('Bulk data not indexed to elasticsearch')
        
    return status


from app.services.reader import read_files
