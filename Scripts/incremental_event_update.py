import os
import json

from Api_calls.get_available_gameweeks import (
    get_available_gameweeks,
    fetch_event_live,
)

from Utils.state import load_last_event, save_last_event
from Utils.logging_config import get_logger
from Utils.db import upsert_events
from Exceptions.api_errors import WriteFileError

OUTPUT_FILE = "Data/events_raw.ndjson"

logger = get_logger("incremental")


def load_existing_event_ids():
    if not os.path.exists(OUTPUT_FILE):
        return set()

    ids = set()

    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                ids.add(rec["event_id"])
            except Exception:
                continue

    return ids


def append_record(record):
    try:
        os.makedirs("Data", exist_ok=True)

        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record))
            f.write("\n")

        # Also upsert into MySQL
        upsert_events([record])

    except Exception as e:
        logger.error(f"Append failed: {e}")
        raise WriteFileError from e


if __name__ == "__main__":
    logger.info("Incremental run started.")

    existing_ids = load_existing_event_ids()
    available = get_available_gameweeks()
    last = load_last_event()

    candidates = [e for e in available if e > last]
    to_fetch = [e for e in candidates if e not in existing_ids]

    if not to_fetch:
        logger.info("Nothing new. All caught up 😴")
        exit()

    for gw in to_fetch:
        logger.info(f"Fetching GW {gw}")

        data = fetch_event_live(gw)

        append_record({
            "event_id": gw,
            "data": data
        })

        save_last_event(gw)

    logger.info("Incremental pipeline finished.")
