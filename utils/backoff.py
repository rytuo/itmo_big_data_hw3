import time
import logging


def backoff(tries=5, sleep=10):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i_try in range(tries):
                try:
                    func(*args, **kwargs)
                    return
                except Exception as e:
                    logging.error(f"{e}\nFailed {i_try + 1}/{tries} times, retry in {sleep} seconds...")
                    time.sleep(sleep)
            logging.error("Failed to process message")
        return wrapper
    return decorator
