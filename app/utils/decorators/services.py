import asyncio
import logging
import time
from functools import wraps
import traceback
from typing import Callable

logger = logging.getLogger(__name__)


def service(fn: Callable) -> Callable:
    '''
    Decorator for all service functions, that need to initialized event_loop.
    If proactor attribute set to `True`, `ProactorEventLoop` is returned else `AbstractEventLoop` is returned.
    Note: loop has is passed to the service function as 1st argument, 
    hence must be specified as 1st argument of decorated function.
    '''
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
            logger.info('Event loop at {} finished after: {}'.format(fn.__name__, time.time() - begin))
        return result
    return wrapper