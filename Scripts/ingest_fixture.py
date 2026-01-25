import os
import csv
import pandas as pd


from Utils.logging_config import get_logger
from Api_calls.fixtures import fetch_fixtures_for_gameweek

logger = get_logger("fixtures_ingest")

OUTPUT_FILE = "data/fixtures.csv"
os.makedirs("data", exist_ok=True)


COLUMNS = [
    "event",
    "id",
    "team_h",
    "team_a",
    "team_h_difficulty",
    "team_a_difficulty",
    "kickoff_time",
    "finished",
    "team_h_score",
    "team_a_score",
]


def get_last_fixture_gw() -> int:
    """
    Reads the fixtures.csv and finds the max GW already stored.
    Returns 0 if file does not exist or is empty.
    """

    if not os.path.exists(OUTPUT_FILE):
        return 0

    try:
        with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            events = [int(row["event"]) for row in reader if row.get("event")]
            return max(events) if events else 0

    except Exception:
        logger.warning(
            "Could not read last GW from fixtures file. Treating as fresh."
        )
        return 0


from Utils.db import upsert_dataframe

def write_fixtures(gw: int, fixtures: list):
    """
    Append fixtures to CSV and upsert to DB.
    Header written only once for CSV.
    """

    if not fixtures:
        logger.warning(f"No fixtures returned for GW {gw}. Skipping write.")
        return

    file_exists = os.path.exists(OUTPUT_FILE)

    rows = [{col: f.get(col) for col in COLUMNS} for f in fixtures]

    try:
        # Write to CSV
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)

            if not file_exists:
                writer.writeheader()

            writer.writerows(rows)

        logger.info(f"Wrote {len(rows)} fixtures for GW {gw} to CSV")

        # Upsert to DB
        df = pd.DataFrame(rows)
        upsert_dataframe(df, "fixtures", primary_keys=["id"])
        logger.info(f"Upserted {len(rows)} fixtures for GW {gw} to DB")

    except Exception:
        logger.exception("Failed writing fixtures")
        raise


def ingest_fixtures():
    """
    Handles both:
    - Cold start
    - Incremental updates based ONLY on fixtures already written
    """

    last_gw_written = get_last_fixture_gw()
    logger.info(f"Last fixture GW in CSV = {last_gw_written}")

    gw = last_gw_written + 1

    while True:
        logger.info(f"Fetching fixtures for GW {gw}")

        fixtures = fetch_fixtures_for_gameweek(gw)

        if not fixtures:
            logger.info("No more fixtures returned. Stopping ingestion.")
            break

        write_fixtures(gw, fixtures)

        gw += 1


if __name__ == "__main__":
    try:
        ingest_fixtures()
        logger.info("Fixture ingestion completed successfully.")
    except Exception:
        logger.exception("Fixture ingestion failed.")
