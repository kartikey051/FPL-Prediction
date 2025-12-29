from Utils.logging_config import get_logger
from Utils.http import safe_get

BASE_URL = "https://fantasy.premierleague.com/api"
logger = get_logger("fixtures")


def fetch_fixtures_for_gameweek(gameweek: int):
    """
    Fetch fixtures for a specific gameweek.

    Uses centralized retry logic via safe_get.
    Returns JSON list of fixtures.
    """

    try:
        logger.info(f"Fetching fixtures for gameweek={gameweek}")

        resp = safe_get(f"{BASE_URL}/fixtures/?event={gameweek}")
        data = resp.json()

        if not isinstance(data, list):
            raise RuntimeError("Unexpected fixtures payload format")

        logger.info(f"Retrieved {len(data)} fixtures for gameweek={gameweek}")

        return data

    except Exception as e:
        logger.exception(f"Failed fetching fixtures for gameweek={gameweek}")
        raise RuntimeError(
            f"Failed fetching fixtures for gameweek={gameweek}"
        ) from e
