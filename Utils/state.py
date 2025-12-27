import os

STATE_FILE = "state/last_event.txt"


def load_last_event() -> int:
    """
    Returns the last processed event id.
    If the state file does not exist, return 0 meaning 'start fresh'.
    """
    if not os.path.exists(STATE_FILE):
        return 0

    try:
        with open(STATE_FILE) as f:
            return int(f.read().strip() or 0)
    except Exception:
        # corrupted file fallback
        return 0


def save_last_event(event_id: int):
    """
    Persist the most recently processed event.
    """
    os.makedirs("state", exist_ok=True)

    with open(STATE_FILE, "w") as f:
        f.write(str(event_id))
