import logging
import os
from typing import Generator, Iterator, List, Tuple, Union
from datetime import datetime as dt
import pandas as pd

from app import FILE_EXTENSIONS, WORK_DIR
from app.model import VALIDATOR_FIELD, File, Source, SOURCES_EXPECTED_COLUMNS
from app.apis.utils.helpers import generate_timestamp_hash


logger = logging.getLogger(__name__)

def scan_for_new_files() -> List[File] :
    files: List[File] = []
    timestamp = obtain_last_indexed_timestamp()
    logger.info(f'Last indexed time: {timestamp if not timestamp else dt.fromtimestamp(timestamp).isoformat()}')
    
    for file_name in os.listdir(WORK_DIR):
        if not file_name.endswith(FILE_EXTENSIONS) or file_name.startswith('~$'): continue
        
        file_path = os.path.join(WORK_DIR, file_name)
        file_ctime = os.path.getctime(file_path)
        file_mtime = os.path.getmtime(file_path)
        
        if timestamp and file_ctime < timestamp and file_mtime < timestamp: continue
        
        file = File(file_path, file_name, file_ctime)
        files.append(file)
    
    files = sorted(files, key= lambda f: f.ctime, reverse=True)
    return files


def get_df_with_source(file: File) -> Union[Tuple[pd.DataFrame, Source], Union[None, None]]:
    df = pd.read_excel(file.path, header=None, dtype=object)
    for source, item in SOURCES_EXPECTED_COLUMNS.items():
        header_idx = item.get('header_idx')
        expected_columns = item.get('columns')
        try:
            df_columns = df.iloc[header_idx].values
        except IndexError:
            continue
        if all(expected_column in df_columns for expected_column in expected_columns): 
            new_df = df.iloc[header_idx + 1:]
            if source == Source.SAP_ANALYZER:
                new_df.columns = [col.replace('.', '') for col in df_columns]
            else: 
                new_df.columns = [col.replace('.', '_') for col in df_columns]
                
            return new_df, source
    return None, None


def read_files() -> Generator[Tuple[Iterator, File], None, None]:
    files = scan_for_new_files()
    logger.info(f'Number of files to index: {len(files)}')
    for i, file in enumerate(files):
        try:
            df, source = get_df_with_source(file)
            if source is None or source.value.get('_id') is None:
                logger.warning(f'File {file.name}, could not be identified as valid source file.')
                continue
            file.source = source
            #! +i because if file read very fast, so timestamps would be same, so adding + i = + 1 second between files
            timestamp = int(dt.now().timestamp()) + i
            file.rtime = timestamp
            file.uid = generate_timestamp_hash(timestamp)
            file.id_field = source.value.get('_id')
            file.row_validator = source.value.get(VALIDATOR_FIELD, None)
            data = iter(df.to_dict(orient='records', ))
            yield (data, file)
        except Exception as e:
            logger.error(f'File {file.name} can not be read')
            logger.error(str(e))
            continue     
        
from app.services.es import obtain_last_indexed_timestamp
