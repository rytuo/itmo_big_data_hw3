import functools
import time
import logging


def backoff(func, tries=5, sleep=10):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        for i_try in range(tries):
            try:
                func(*args, **kwargs)
                return
            except Exception as e:
                logging.error(f"{e}\nFailed {i_try}/{tries} times, retry in {sleep} seconds...")
                time.sleep(sleep)

    return wrapper
