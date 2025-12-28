import time
import requests

from Exceptions.fpl_exceptions import (
    FPLNetworkError,
    FPLServerError,
    FPLClientError,
)

DEFAULT_TIMEOUT = 20


def safe_get(url, retries=3, backoff=2):
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=DEFAULT_TIMEOUT)

            if 500 <= resp.status_code < 600:
                raise FPLServerError(f"Server error {resp.status_code} for {url}")

            if 400 <= resp.status_code < 500:
                raise FPLClientError(f"Client error {resp.status_code} for {url}")

            return resp

        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt == retries - 1:
                raise FPLNetworkError(f"Network failure for {url}: {e}") from e

            time.sleep(backoff * (attempt + 1))
