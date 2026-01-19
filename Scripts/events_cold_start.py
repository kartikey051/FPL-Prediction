import os
import json

from Api_calls.events import fetch_events_range
from Api_calls.get_available_gameweeks import get_available_gameweeks
from Utils.state import save_last_event
from Utils.logging_config import get_logger
from Exceptions.api_errors import WriteFileError

OUTPUT_FILE = "Data/events_raw.ndjson"

logger = get_logger("cold_start")


def write_records(records):
    try:
        if os.path.exists(OUTPUT_FILE):
            raise WriteFileError(
                f"{OUTPUT_FILE} already exists. "
                "Cold start should run only on an empty system."
            )

        os.makedirs("Data", exist_ok=True)

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r))
                f.write("\n")

        logger.info(f"Written {len(records)} events into {OUTPUT_FILE}")

    except Exception as e:
        logger.error(f"Cold start write failed: {e}")
        raise

if __name__ == "__main__":
    logger.info("Cold start begun.")

    gameweeks = get_available_gameweeks()

    start = min(gameweeks)
    end = max(gameweeks)

    logger.info(f"Fetching range {start} -> {end}")

    data = fetch_events_range(start, end)

    write_records(data)

    save_last_event(end)

    logger.info(f"Cold start complete. Last event set to {end}")
