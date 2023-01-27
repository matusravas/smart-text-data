import logging
from app.services.es import bulk_files_to_es

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    status = bulk_files_to_es()    