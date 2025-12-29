import requests
from Utils.retry import retry_request
from Utils.logging_config import get_logger
from Exceptions.api_errors import APIRequestError

BASE_URL = "https://fantasy.premierleague.com/api"

logger = get_logger("events")


def fetch_event_live(event_id: int):
    url = f"{BASE_URL}/event/{event_id}/live/"

    def call():
        resp = requests.get(url, timeout=25)
        resp.raise_for_status()
        return resp.json()

    logger.info(f"Fetching event {event_id}")

    try:
        return retry_request(call)
    except Exception as e:
        logger.error(f"Event {event_id} failed: {e}")
        raise APIRequestError(f"Could not fetch event {event_id}") from e


def fetch_events_range(start: int, end: int):
    results = []

    for event_id in range(start, end + 1):
        data = fetch_event_live(event_id)

        results.append({
            "event_id": event_id,
            "data": data
        })

    logger.info(f"Fetched {len(results)} events total.")
    return results
