"""
State management for scraping operations.
Extends the base state.py to handle player scraping state.
"""
print(">>> scraper_state LOADED FROM:", __file__)
print(">>> AVAILABLE NAMES AT IMPORT:", dir())

import os
import json
from typing import Dict, List, Optional

from Utils.logging_config import get_logger

logger = get_logger("scraper_state")

STATE_DIR = "state/scraper"
PLAYER_STATE_FILE = os.path.join(STATE_DIR, "player_progress.json")


def _ensure_state_dir():
    """Ensure state directory exists."""
    os.makedirs(STATE_DIR, exist_ok=True)


def load_player_state() -> Dict[str, Dict]:
    """
    Load the scraping state for all players.
    
    Returns:
        Dict mapping player_id to state info:
        {
            "player_id": {
                "player_name": str,
                "last_scraped_season": str,
                "completed_seasons": [str],
                "status": "pending" | "in_progress" | "completed" | "failed"
            }
        }
    """
    _ensure_state_dir()
    
    if not os.path.exists(PLAYER_STATE_FILE):
        return {}
    
    try:
        with open(PLAYER_STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load player state: {e}. Starting fresh.")
        return {}


def save_player_state(state: Dict[str, Dict]):
    """
    Persist the player scraping state.
    
    Args:
        state: Complete state dictionary to save
    """
    _ensure_state_dir()
    
    try:
        with open(PLAYER_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        logger.debug("Player state saved successfully")
    except Exception as e:
        logger.error(f"Failed to save player state: {e}")


def update_player_progress(
    player_id: str,
    player_name: str,
    season: Optional[str] = None,
    status: str = "in_progress"
):
    """
    Update progress for a single player.
    
    Args:
        player_id: Unique player identifier
        player_name: Player's name
        season: Season that was just completed (optional)
        status: Current status of the player scraping
    """
    state = load_player_state()
    
    if player_id not in state:
        state[player_id] = {
            "player_name": player_name,
            "last_scraped_season": None,
            "completed_seasons": [],
            "status": "pending"
        }
    
    # Update season info if provided
    if season:
        state[player_id]["last_scraped_season"] = season
        if season not in state[player_id]["completed_seasons"]:
            state[player_id]["completed_seasons"].append(season)
    
    # Update status
    state[player_id]["status"] = status
    
    save_player_state(state)


def get_player_status(player_id: str) -> Optional[Dict]:
    """
    Get the current state for a specific player.
    
    Args:
        player_id: Player identifier
        
    Returns:
        Player state dict or None if not found
    """
    state = load_player_state()
    return state.get(player_id)


def get_incomplete_players() -> List[str]:
    """
    Get list of player IDs that haven't been fully scraped.
    
    Returns:
        List of player_ids with status != "completed"
    """
    state = load_player_state()
    return [
        pid for pid, info in state.items()
        if info.get("status") != "completed"
    ]


def mark_player_failed(player_id: str, player_name: str, reason: str):
    """
    Mark a player as failed with reason.
    
    Args:
        player_id: Player identifier
        player_name: Player's name
        reason: Failure reason
    """
    state = load_player_state()
    
    if player_id not in state:
        state[player_id] = {
            "player_name": player_name,
            "last_scraped_season": None,
            "completed_seasons": [],
            "status": "failed"
        }
    else:
        state[player_id]["status"] = "failed"
    
    state[player_id]["failure_reason"] = reason
    
    save_player_state(state)
    logger.warning(f"Player {player_name} ({player_id}) marked as failed: {reason}")