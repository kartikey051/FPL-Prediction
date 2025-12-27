import requests

BASE_URL = "https://fantasy.premierleague.com/api"

def fetch_event_live(event_id: int) -> dict:
    url = f"{BASE_URL}/event/{event_id}/live/"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.json()
