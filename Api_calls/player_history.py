import pandas as pd

from Utils.logging_config import get_logger
from Utils.http import safe_get
from Exceptions.player_errors import PlayerHistoryError

BASE_URL = "https://fantasy.premierleague.com/api"
logger = get_logger("player_history")


def fetch_player_history(player_id: int) -> pd.DataFrame:
    """
    Fetch match-by-match historical performance for one player.
    Uses centralized retry logic via safe_get.
    """
    try:
        logger.info(f"Fetching history for player={player_id}")

        resp = safe_get(f"{BASE_URL}/element-summary/{player_id}/")
        data = resp.json()

        history = data.get("history", [])
        return pd.DataFrame(history)

    except Exception as e:
        logger.exception(f"History fetch failed for player={player_id}")
        raise PlayerHistoryError(
            f"Failed history fetch for {player_id}"
        ) from e
