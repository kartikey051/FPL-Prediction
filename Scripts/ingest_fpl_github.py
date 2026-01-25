"""
Ingest historical FPL data from GitHub repository.
Normalizes data into 5 core tables.
"""

import pandas as pd
from typing import List

from Utils.logging_config import get_logger
from Utils.db import upsert_dataframe, execute_write
from Utils.github_fetch import fetch_csv_from_github

logger = get_logger("ingest_fpl_github")

# Seasons to process (exclude current if handled by live pipeline, but repo has history)
# The repo has 2016-17 onwards
SEASONS = [
    "2016-17", "2017-18", "2018-19", "2019-20", "2020-21",
    "2021-22", "2022-23", "2023-24", "2024-25" 
]

def ingest_season_teams(season: str):
    """Ingest team ratings for a specific season."""
    path = f"{season}/teams.csv"
    df = fetch_csv_from_github(path)
    
    if df is None:
        return

    df["season"] = season
    df = df.rename(columns={"id": "team_id", "name": "team_name"})
    
    # Columns to keep
    cols = [
        "season", "team_id", "team_name", "short_name", "strength", 
        "strength_overall_home", "strength_overall_away",
        "strength_attack_home", "strength_attack_away",
        "strength_defence_home", "strength_defence_away"
    ]
    
    # Filter valid columns
    valid_cols = [c for c in cols if c in df.columns]
    df = df[valid_cols]
    
    upsert_dataframe(df, "fpl_season_teams", primary_keys=["season", "team_id"])


def ingest_season_players(season: str):
    """Ingest player details for a season."""
    # Use players_raw.csv for full details
    path = f"{season}/players_raw.csv"
    df = fetch_csv_from_github(path)
    
    if df is None:
        return

    df["season"] = season
    
    # Rename id to element_id for consistency
    df = df.rename(columns={"id": "element_id", "team": "team_id"})
    
    # Columns of interest
    cols = [
        "season", "element_id", "first_name", "second_name", 
        "team_id", "element_type", "total_points", "now_cost"
    ]
    
    valid_cols = [c for c in cols if c in df.columns]
    df = df[valid_cols]
    
    upsert_dataframe(df, "fpl_season_players", primary_keys=["season", "element_id"])


def ingest_fixtures(season: str):
    """Ingest fixtures for a season."""
    path = f"{season}/fixtures.csv"
    df = fetch_csv_from_github(path)
    
    if df is None:
        return

    df["season"] = season
    df = df.rename(columns={"id": "fixture_id"})
    
    cols = [
        "season", "fixture_id", "event", "team_h", "team_a", 
        "team_h_score", "team_a_score", "finished", "kickoff_time"
    ]
    
    valid_cols = [c for c in cols if c in df.columns]
    df = df[valid_cols]
    
    upsert_dataframe(df, "fpl_fixtures", primary_keys=["season", "fixture_id"])


def ingest_gameweeks(season: str):
    """Ingest player gameweek stats."""
    path = f"{season}/gws/merged_gw.csv"
    df = fetch_csv_from_github(path, encoding="latin1") # frequent encoding issue
    
    if df is None:
        return

    df["season"] = season
    
    # Ensure element_id column exists
    if "element" in df.columns:
        df = df.rename(columns={"element": "element_id"})
    elif "id" in df.columns:
        # Some old files might use id, but confirm context
        # usually merged_gw has 'element' or 'id' as unique index
        # For now, rely on standard "element" or manual rename if we find patterns
        pass
        
    if "round" in df.columns:
        df = df.rename(columns={"round": "gameweek"})
    elif "GW" in df.columns:
        df = df.rename(columns={"GW": "gameweek"})
        
    if "fixture" in df.columns:
        df = df.rename(columns={"fixture": "fixture_id"})
        
    required_keys = ["season", "element_id", "gameweek"]
    
    if "element_id" not in df.columns:
        logger.warning(f"Season {season} gws missing 'element_id' column. Columns: {df.columns.tolist()}")
        return
    
    # Drop rows where keys are missing
    df = df.dropna(subset=[k for k in required_keys if k in df.columns])

    cols = [
        "season", "element_id", "gameweek", "fixture_id",
        "total_points", "minutes", "goals_scored", "assists",
        "clean_sheets", "goals_conceded", "own_goals", 
        "penalties_saved", "penalties_missed", "yellow_cards", "red_cards",
        "saves", "bonus", "bps", "influence", "creativity", "threat", "ict_index",
        "value", "transfers_balance", "selected", "transfers_in", "transfers_out"
    ]
    
    valid_cols = [c for c in cols if c in df.columns]
    df = df[valid_cols]
    
    # Batch upsert because these files are large (20k+ rows)
    upsert_dataframe(df, "fpl_player_gameweeks", primary_keys=required_keys, batch_size=2000)


def ingest_fpl_github():
    try:
        logger.info("Starting FPL GitHub ingestion...")
        
        # Per Season Data
        for season in SEASONS:
            logger.info(f"Processing season {season}...")
            
            ingest_season_teams(season)
            ingest_season_players(season)
            ingest_fixtures(season)
            ingest_gameweeks(season)
            
            logger.info(f"Completed season {season}")
            
        logger.info("FPL GitHub ingestion completed successfully.")
        
    except Exception as e:
        logger.exception("FPL GitHub ingestion failed")
        raise


if __name__ == "__main__":
    ingest_fpl_github()
