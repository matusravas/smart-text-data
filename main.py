import logging
from app.services.es import bulk_files_to_es, check_results_and_post_last_timestamp

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    results = bulk_files_to_es()
    check_results_and_post_last_timestamp(results)
    