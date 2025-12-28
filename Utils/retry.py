import time
import random
from typing import Callable, Any

from Utils.logging_config import get_logger

logger = get_logger("retry")


RETRYABLE_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    Exception,   # keep last to catch custom retry-triggered exceptions
)


def retry_request(
    func: Callable[[], Any],
    retries: int = 3,
    backoff: float = 2.0,
    jitter: bool = True,
):
    """
    Wrap a callable with retries.

    Strategy:
    - Exponential backoff: backoff^attempt (2, 4, 8 ...)
    - Jitter: randomization to avoid retry storms
    - Retry only for retryable exceptions (HTTP layer decides what is retryable)
    """

    for attempt in range(1, retries + 1):
        try:
            return func()

        except RETRYABLE_EXCEPTIONS as e:
            logger.warning(f"Attempt {attempt} failed: {e}")

            if attempt == retries:
                logger.error("All retry attempts failed.")
                raise

            # exponential backoff
            sleep_time = backoff ** attempt

            # jitter adds randomness so we don't hammer the server at once
            if jitter:
                sleep_time += random.uniform(0, 1.0)

            logger.info(f"Retrying in {sleep_time:.2f}s...")
            time.sleep(sleep_time)
