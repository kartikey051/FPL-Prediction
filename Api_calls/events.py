import requests

BASE_URL = "https://fantasy.premierleague.com/api"

def fetch_event_live(event_id: int) -> dict:
    url = f"{BASE_URL}/event/{event_id}/live/"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.json()


def fetch_events_range(start:int , end: int = 18):
    """
    Fetch all event live data from `start` to `end` (inclusive).
    Returns a list of JSON dicts.
    """
    results = []

    for event_id in range(start, end + 1):
        print(f"Fetching event {event_id}...")
        data = fetch_event_live(event_id)
        results.append({
            "event_id": event_id,
            "data": data
        })

    return results