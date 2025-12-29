import os
import pandas as pd

from Utils.logging_config import get_logger
from Api_calls.players import fetch_player_snapshot
from Api_calls.player_history import fetch_player_history

logger = get_logger("fact_table")

OUTPUT_FILE = "data/fact_player_gameweeks.csv"
os.makedirs("data", exist_ok=True)


def get_players_and_teams():
    """
    Fetch players + teams from bootstrap-static.
    """
    try:
        players_df, teams_df, _ = fetch_player_snapshot()

        logger.info(
            f"Loaded snapshot: players={len(players_df)} teams={len(teams_df)}"
        )

        return players_df, teams_df

    except Exception:
        logger.exception("Failed to load player snapshot")
        raise


def build_player_history(players_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch history for every player and combine.
    """

    all_histories = []

    for idx, row in players_df.iterrows():
        player_id = row["id"]

        try:
            history_df = fetch_player_history(player_id)

            # Attach player reference
            history_df["player_id"] = player_id
            history_df["player_name"] = f"{row['first_name']} {row['second_name']}"
            history_df["team_id"] = row["team"]

            all_histories.append(history_df)

            if (idx + 1) % 50 == 0:
                logger.info(f"Fetched history for {idx + 1} players")

        except Exception:
            logger.exception(f"Failed fetching history for player={player_id}")

    if not all_histories:
        raise RuntimeError("No player histories fetched")

    combined = pd.concat(all_histories, ignore_index=True)
    logger.info(f"Combined history rows={len(combined)}")

    return combined


def attach_match_context(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize opponent / difficulty / home-away.
    These already come from the FPL history endpoint.
    """

    df = df.rename(
        columns={
            "round": "event",
            "opponent_team": "opponent_id",
            "difficulty": "fdr",
        }
    )

    # Map home/away
    df["home_away"] = df["was_home"].map({True: "H", False: "A"})

    return df


def enrich_with_teams(df: pd.DataFrame, teams_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add readable opponent and team names.
    """

    teams_lookup = teams_df[["id", "name"]]

    # join main team
    df = df.merge(
        teams_lookup,
        left_on="team_id",
        right_on="id",
        how="left"
    ).rename(columns={"name": "team_name"})

    df = df.drop(columns=["id"], errors="ignore")

    # join opponent
    df = df.merge(
        teams_lookup.rename(columns={"id": "opponent_id", "name": "opponent_name"}),
        on="opponent_id",
        how="left",
    )

    logger.info("Attached team and opponent names")

    return df


def select_fact_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the meaningful modeling columns.
    """

    keep = [
        "player_id",
        "player_name",
        "team_id",
        "team_name",
        "event",
        "minutes",
        "total_points",
        "value",
        "goals_scored",
        "assists",
        "clean_sheets",
        "goals_conceded",
        "yellow_cards",
        "red_cards",
        "own_goals",
        "opponent_id",
        "opponent_name",
        "home_away",
        "fdr",
    ]

    existing = [c for c in keep if c in df.columns]

    return df[existing]


def save_fact_table(df: pd.DataFrame):
    try:
        df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"Saved fact table: {OUTPUT_FILE} rows={len(df)}")
    except Exception:
        logger.exception("Failed saving fact table")
        raise


def build_fact_table():
    logger.info("Starting fact table build")

    players_df, teams_df = get_players_and_teams()

    history_df = build_player_history(players_df)

    fact = attach_match_context(history_df)

    fact = enrich_with_teams(fact, teams_df)

    fact = select_fact_columns(fact)

    save_fact_table(fact)

    logger.info("Fact table build complete")


if __name__ == "__main__":
    try:
        build_fact_table()
    except Exception:
        logger.exception("Fact table build failed")
