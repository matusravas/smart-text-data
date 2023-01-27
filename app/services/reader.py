import logging
import os
from typing import Generator, Iterator, List, Tuple
from datetime import datetime as dt
import pandas as pd

from app import FILE_EXTENSIONS, WORK_DIR
from app.model import File
from app.utils.helpers import parse_field

logger = logging.getLogger(__name__)

def scan_for_new_files() -> List[File] :
    files: List[File] = []
    timestamp = obtain_last_indexed_timestamp()
    logger.info(f'Last indexed time: {timestamp if not timestamp else dt.fromtimestamp(timestamp).isoformat()}')
    
    for file_name in os.listdir(WORK_DIR):
        if not file_name.endswith(FILE_EXTENSIONS) or file_name.startswith('~$'): continue
        
        file_path = os.path.join(WORK_DIR, file_name)
        file_ctime = os.path.getctime(file_path)
        
        if timestamp and file_ctime < timestamp: continue
        
        file = File(file_path, file_name, file_ctime)
        files.append(file)
    
    files = sorted(files, key= lambda f: f.ctime, reverse=True)
    return files


def read_files() -> Generator[Tuple[Iterator, File], None, None]:
    files = scan_for_new_files()
    logger.info(f'Number of files to index: {len(files)}')
    for file in files:
        try:
            df = pd.read_excel(file.path)
            df = df.rename(columns=lambda x: x.replace('.', '_'))
            df = df.applymap(parse_field)
            data = iter(df.to_dict(orient = 'records'))
            yield (data, file)
        except Exception as e:
            logger.error(f'File {file.name} can not be read')
            logger.error(str(e))
            continue     
        
from app.services.es import obtain_last_indexed_timestamp
