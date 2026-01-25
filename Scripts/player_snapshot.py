import os
from Api_calls.players import fetch_player_snapshot
from Utils.logging_config import get_logger
from Utils.db import upsert_dataframe

logger = get_logger("player_snapshot")

OUTPUT_DIR = "Data"


if __name__ == "__main__":
    try:
        logger.info("Player snapshot job started")

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        players, teams, positions = fetch_player_snapshot()

        players.to_csv(f"{OUTPUT_DIR}/players.csv", index=False)
        teams.to_csv(f"{OUTPUT_DIR}/teams.csv", index=False)
        positions.to_csv(f"{OUTPUT_DIR}/positions.csv", index=False)

        logger.info("Player snapshot written to CSV successfully")

        # Upsert to DB
        upsert_dataframe(players, "players", primary_keys=["id"])
        upsert_dataframe(teams, "teams", primary_keys=["id"])
        upsert_dataframe(positions, "positions", primary_keys=["id"])

        logger.info("Player snapshot upserted to DB successfully")

    except Exception as e:
        logger.exception(f"player_snapshot failed: {e}")
        raise
