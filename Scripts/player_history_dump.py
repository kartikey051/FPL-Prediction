import os
import pandas as pd

from Api_calls.player_history import fetch_player_history
from Utils.logging_config import get_logger

logger = get_logger("player_history_dump")

OUTPUT_FILE = "Data/player_history.ndjson"


from Utils.db import upsert_dataframe

def append_records(df):
    os.makedirs("Data", exist_ok=True)

    df.to_json(
        OUTPUT_FILE,
        orient="records",
        lines=True,
        mode="a",
    )
    
    # Upsert to MySQL
    # Primary key should be player_id + fixture combination or something unique for history.
    # The API output uses 'element' as player_id and 'fixture' as fixture_id. 
    # Let's ensure we have a unique key.
    if not df.empty and "element" in df.columns and "fixture" in df.columns:
         upsert_dataframe(df, "player_history", primary_keys=["element", "fixture"])


if __name__ == "__main__":
    try:
        logger.info("Player history dump started")

        players = pd.read_csv("Data/players.csv")

        for pid in players["id"]:
            try:
                hist = fetch_player_history(pid)
                hist["player_id"] = pid
                append_records(hist)

            except Exception as e:
                logger.error(f"Skipped player={pid}: {e}")

        logger.info("Player history dump completed successfully")

    except Exception as e:
        logger.exception(f"player_history_dump failed: {e}")
        raise
