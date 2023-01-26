import logging
import os
from datetime import datetime as dt
from typing import Generator, Iterator, List, Tuple

import pandas as pd

from app import FILE_EXTENSIONS, WORK_DIR
from app.model import File
from app.utils.helpers import parse_field

logger = logging.getLogger(__name__)

def scan_for_new_files() -> List[File] :
    files: List[File] = []
    timestamp = obtain_last_indexed_timestamp()
    logger.info(f'Last indexed time: {timestamp}')
    
    for file_name in os.listdir(WORK_DIR):
        if not file_name.endswith(FILE_EXTENSIONS): continue
        
        file_path = os.path.join(WORK_DIR, file_name)
        file_ctime = dt.fromtimestamp(os.path.getctime(file_path))
        
        if timestamp and file_ctime < timestamp: continue
        
        file = File(file_path, file_name, file_ctime)
        files.append(file)
    
    files = sorted(files, key= lambda f: f.ctime, reverse=True)
    return files


def read_files() -> Generator[Tuple[Iterator, File], None, None]:
    files = scan_for_new_files()
    logger.info(f'Number of files to index: {len(files)}')
    for file in files:
        df = pd.read_excel(file.path) #.to_dict('records')
        df = df.applymap(parse_field)
        data = iter(df.to_dict(orient = 'records'))
        yield (data, file)
        
        
from app.services.es import obtain_last_indexed_timestamp
