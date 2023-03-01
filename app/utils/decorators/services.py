import asyncio
import logging
import time
from functools import wraps
import traceback
from typing import Callable

logger = logging.getLogger(__name__)


def service(fn: Callable) -> Callable:
    @wraps(fn)
    def wrapper(*args, **kwargs):
        begin = time.time()
        try:
            loop = asyncio.get_event_loop()
            result = fn(loop, *args, **kwargs)
        except Exception as e:
            logger.error(f'Exception in service wrapper: {str(e)}')
            traceback.print_exc()
            raise e
        finally:
            pass
            # logger.info('Event loop at {} finished after: {}'.format(fn.__name__, time.time() - begin))
        return result
    return wrapper