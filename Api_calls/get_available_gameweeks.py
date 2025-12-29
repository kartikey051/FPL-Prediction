import requests
import pandas as pd

from Utils.retry import retry_request
from Utils.logging_config import get_logger
from Utils.json_flattner import json_to_dataframe
from Utils.state import load_last_event, save_last_event
from Exceptions.api_errors import GameweekDiscoveryError, APIRequestError

BASE_URL = "https://fantasy.premierleague.com/api"

logger = get_logger("gameweeks")


def get_available_gameweeks():
    url = f"{BASE_URL}/bootstrap-static/"

    def call():
        resp = requests.get(url, timeout=25)
        resp.raise_for_status()
        return resp.json()

    try:
        payload = retry_request(call)

        events = payload["events"]
        gameweeks = [e["id"] for e in events if e["finished"] or e["data_checked"]]

        logger.info(f"Discovered gameweeks: {gameweeks}")
        return gameweeks

    except Exception as e:
        logger.error(f"Discovery failed: {e}")
        raise GameweekDiscoveryError("Could not load available gameweeks") from e


def fetch_event_live(event_id: int):
    url = f"{BASE_URL}/event/{event_id}/live/"

    def call():
        resp = requests.get(url, timeout=25)
        resp.raise_for_status()
        return resp.json()

    try:
        return retry_request(call)
    except Exception as e:
        raise APIRequestError(f"Failed fetching GW {event_id}") from e


def fetch_new_events_dataframe():
    available = get_available_gameweeks()
    last = load_last_event()

    to_fetch = [e for e in available if e > last]

    if not to_fetch:
        logger.info("No new gameweeks yet.")
        return None

    frames = []

    for event_id in to_fetch:
        logger.info(f"Incrementally fetching GW {event_id}")

        data = fetch_event_live(event_id)
        elements = data.get("elements", [])

        df = json_to_dataframe(elements)
        df["event_id"] = event_id

        frames.append(df)

        save_last_event(event_id)

    return pd.concat(frames, ignore_index=True)
