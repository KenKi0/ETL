import logging
import time
from contextlib import contextmanager
from functools import wraps
from logging import Logger
from typing import Callable, Iterator, Protocol, TypeAlias, TypeVar

logger = logging.getLogger(__name__)

DatabaseData: TypeAlias = dict[str, Iterator[dict]]
TransformedData: TypeAlias = dict[str, Iterator[dict]]


class Connection(Protocol):
    def close(self):
        ...


T = TypeVar('T', Connection, Connection)

DEFAULT_DELAY = 1


@contextmanager
def db_conn(connection: T) -> T:
    """Context manager to close connection with database.
    @param connection: open connection with database
    """
    try:
        yield connection
    finally:
        connection.close()


def default_backoff_gen(start_delay: int, delay_limit: int):
    """
    Default generator with increase delay logic as start_delay * 2
    :param start_delay: start delay time
    :param delay_limit: limit for delay time
    """
    if start_delay == 0:
        start_delay = DEFAULT_DELAY
    while True:
        yield start_delay
        if start_delay < delay_limit:
            start_delay *= 2
        else:
            start_delay = delay_limit


class Backoff:
    """
    Functur with backoff logic
    """

    def __init__(
        self,
        backoff: Callable[[int, int], Iterator[int]] = default_backoff_gen,
        logger_inst: Logger = logger,
        exception: Exception = Exception,
        start_delay: int = 0.1,
        delay_limit: int = 10,
        num_of_tries: int = 10,
        log_level: int = logging.ERROR,
    ):
        self.delay = start_delay
        self.delay_limit = delay_limit
        self.backoff = backoff
        self.exception = exception
        self.logger = logger_inst
        self.log_level = log_level
        self.max_tries = num_of_tries
        self.tries = 0

    def __call__(self, func):
        @wraps(func)
        def repeater(*args, **kwargs):
            backoff_gen = self.backoff(self.delay, self.delay_limit)
            try:
                while self.tries <= self.max_tries:
                    try:
                        return func(*args, **kwargs)
                    except self.exception as e:
                        message = 'Exception caught.\n\nRetry attempt №{}.'.format(self.tries)

                        self.logger.log(self.log_level, message, exc_info=e)

                        time.sleep(self.delay)
                        self.delay = next(backoff_gen)
                        self.tries += 1

                return func(*args, **kwargs)
            finally:
                if self.tries == self.max_tries:
                    self.logger.info('Retry limit exceeded')
                self.tries = 0

        return repeater
