import time
import logging

from Utils.logging_config import get_logger

logger = get_logger("retry")


def retry_request(func, retries=3, backoff=2):
    """
    Wrap a callable with retries.
    backoff=2 produces exponential delays: 2, 4, 8...
    """
    for attempt in range(1, retries + 1):
        try:
            return func()

        except Exception as e:
            logger.warning(f"Attempt {attempt} failed: {e}")

            if attempt == retries:
                logger.error("All retry attempts failed.")
                raise

            sleep_time = backoff ** attempt
            logger.info(f"Retrying in {sleep_time}s...")
            time.sleep(sleep_time)
