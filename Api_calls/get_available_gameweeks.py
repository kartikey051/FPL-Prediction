import pandas as pd
import requests

from Utils.json_flattner import json_to_dataframe
from Utils.state import load_last_event, save_last_event

BASE_URL = "https://fantasy.premierleague.com/api"


def get_available_gameweeks():
    resp = requests.get(f"{BASE_URL}/bootstrap-static/", timeout=20)
    resp.raise_for_status()

    events = resp.json()["events"]

    # events that have real, checked data
    return [e["id"] for e in events if e["finished"] or e["data_checked"]]


def fetch_event_live(event_id: int):
    resp = requests.get(f"{BASE_URL}/event/{event_id}/live/", timeout=20)
    resp.raise_for_status()
    return resp.json()


def fetch_new_events_dataframe():
    available = get_available_gameweeks()
    last = load_last_event()

    to_fetch = [e for e in available if e > last]

    if not to_fetch:
        print("No new gameweeks yet. Chill mode engaged 🌴")
        return None

    frames = []

    for event_id in to_fetch:
        print(f"Fetching GW {event_id}...")

        data = fetch_event_live(event_id)
        elements = data.get("elements", [])

        df = json_to_dataframe(elements)
        df["event_id"] = event_id

        frames.append(df)

        # update state only AFTER success
        save_last_event(event_id)

    return pd.concat(frames, ignore_index=True)
