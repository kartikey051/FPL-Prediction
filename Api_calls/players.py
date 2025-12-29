from Utils.json_flattner import json_to_dataframe
from Utils.logging_config import get_logger
from Utils.http import safe_get
from Exceptions.player_errors import PlayerFetchError

BASE_URL = "https://fantasy.premierleague.com/api"
logger = get_logger("players")


def fetch_player_snapshot():
    """
    Fetch full bootstrap-static snapshot:
    players, teams, positions.
    Uses centralized retry logic via safe_get.
    """
    try:
        logger.info("Calling bootstrap-static for player snapshot")

        resp = safe_get(f"{BASE_URL}/bootstrap-static/")
        data = resp.json()

        players = data.get("elements", [])
        teams = data.get("teams", [])
        positions = data.get("element_types", [])

        logger.info(
            f"Snapshot received players={len(players)} teams={len(teams)} positions={len(positions)}"
        )

        return (
            json_to_dataframe(players),
            json_to_dataframe(teams),
            json_to_dataframe(positions),
        )

    except Exception as e:
        logger.exception("Bootstrap-static fetch failed")
        raise PlayerFetchError(str(e)) from e
