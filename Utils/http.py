import requests

from Exceptions.fpl_exceptions import (
    FPLNetworkError,
    FPLServerError,
    FPLClientError,
)

from Utils.retry import retry_request

DEFAULT_TIMEOUT = 20


def _raw_get(url: str):
    """
    Perform a single HTTP GET call.

    Responsibility here:
      • call requests
      • classify response
      • raise retry-triggering errors for transient failures
      • raise hard errors for bad requests
    """

    resp = requests.get(url, timeout=DEFAULT_TIMEOUT)

    # Retryable / temporary server-side issues
    if resp.status_code in (429, 500, 502, 503, 504):
        raise FPLServerError(
            f"Temporary server error {resp.status_code} while requesting {url}"
        )

    # Non-retryable client errors (our fault or invalid params)
    if 400 <= resp.status_code < 500:
        raise FPLClientError(
            f"Client error {resp.status_code} while requesting {url}"
        )

    return resp


def safe_get(url: str, retries: int = 3, backoff: float = 2.0):
    """
    Public-facing HTTP GET wrapper.

    Centralized retry logic lives in retry_request:
      • exponential backoff
      • jitter
      • logs
      • retry only when appropriate
    """

    def op():
        return _raw_get(url)

    try:
        return retry_request(
            op,
            retries=retries,
            backoff=backoff,
            jitter=True,
        )

    except requests.ConnectionError as e:
        raise FPLNetworkError(f"Network failure for {url}: {e}") from e

    except requests.Timeout as e:
        raise FPLNetworkError(f"Timeout fetching {url}: {e}") from e
