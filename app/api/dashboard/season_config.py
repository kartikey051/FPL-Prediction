"""
Season Schema Configuration - Typed, Safe Season-Aware Data Access

This module provides:
1. SeasonSchema dataclass with explicit capability flags
2. Pre-configured schemas for all seasons (2016-17 to 2024-25)
3. SQL builder helper functions for safe query construction

Design principles:
- No unsafe dictionary access
- Explicit capability flags (supports_teams, supports_understat, etc.)
- Graceful degradation when features unavailable
- Fact-first architecture (fpl_player_gameweeks as primary source)
"""

from dataclasses import dataclass
from typing import Optional, Tuple

CURRENT_SEASON = "2024-25"

# Seasons with fpl_season_teams table available (verified from ingestion)
SEASONS_WITH_TEAMS = {"2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25"}

# Seasons with understat data available (INT format, e.g., 2019 for 2019-20)
UNDERSTAT_SEASONS = {2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024}


@dataclass(frozen=True)
class SeasonSchema:
    """
    Typed season schema with explicit capability flags.
    Immutable to prevent accidental modification.
    """
    name: str                       # e.g., "2024-25"
    is_historical: bool             # True for past seasons
    
    # Table names
    table_fact: str                 # Primary fact table
    table_players: str              # Player dimension
    table_teams: str                # Team dimension (if available)
    table_fixtures: str             # Fixtures table
    
    # Column mappings - CRITICAL: distinguish table PK from FK reference
    col_player_id: str              # Player ID in FACT table (element_id or player_id)
    col_player_table_id: str        # Player ID in PLAYER table (element_id or id)
    col_team_id: str                # team_id or id
    col_gameweek: str               # gameweek or event
    col_team_name: str              # team_name or name
    
    # Capability flags - CRITICAL for safe conditional logic
    supports_teams: bool            # False for 2016-17 to 2018-19
    supports_understat: bool        # Based on understat data availability
    supports_standings: bool        # Requires teams + fixtures
    supports_fixtures: bool         # Most seasons have fixtures


def get_season_schema(season: str) -> SeasonSchema:
    """
    Factory function to get the appropriate schema for a season.
    
    Returns a fully-typed SeasonSchema object with all capability flags set.
    Never raises KeyError - always returns a valid schema.
    """
    is_historical = season != CURRENT_SEASON and season is not None
    
    if is_historical:
        # Convert season format (e.g., "2019-20" -> 2019) for understat check
        try:
            understat_year = int(season.split("-")[0])
        except (ValueError, AttributeError):
            understat_year = 0
        
        supports_teams = season in SEASONS_WITH_TEAMS
        supports_understat = understat_year in UNDERSTAT_SEASONS
        
        return SeasonSchema(
            name=season,
            is_historical=True,
            table_fact="fpl_player_gameweeks",
            table_players="fpl_season_players",
            table_teams="fpl_season_teams",
            table_fixtures="fpl_fixtures",
            col_player_id="element_id",           # In fact table
            col_player_table_id="element_id",     # In player table (same for historical)
            col_team_id="team_id",
            col_gameweek="gameweek",
            col_team_name="team_name",
            supports_teams=supports_teams,
            supports_understat=supports_understat,
            supports_standings=supports_teams,  # Standings need teams
            supports_fixtures=True
        )
    else:
        # Current season uses live tables
        # CRITICAL: players table uses 'id', fact table uses 'player_id'
        return SeasonSchema(
            name=season,
            is_historical=False,
            table_fact="fact_player_gameweeks",
            table_players="players",
            table_teams="teams",
            table_fixtures="fixtures",
            col_player_id="player_id",            # In fact table
            col_player_table_id="id",             # In players table (PRIMARY KEY)
            col_team_id="id",
            col_gameweek="event",
            col_team_name="name",
            supports_teams=True,
            supports_understat=True,
            supports_standings=True,
            supports_fixtures=True
        )


def build_season_filter(schema: SeasonSchema, table_alias: str = "") -> str:
    """
    Build a WHERE clause fragment for season filtering.
    
    Returns:
        SQL fragment like "AND f.season = '2019-20'" or empty string for current season.
    """
    if not schema.is_historical:
        return ""
    
    prefix = f"{table_alias}." if table_alias else ""
    return f"AND {prefix}season = '{schema.name}'"


def build_season_where(schema: SeasonSchema, table_alias: str = "") -> str:
    """
    Build a standalone WHERE clause for season filtering.
    
    Returns:
        SQL fragment like "WHERE season = '2019-20'" or "WHERE 1=1" for current season.
    """
    if not schema.is_historical:
        return "WHERE 1=1"
    
    prefix = f"{table_alias}." if table_alias else ""
    return f"WHERE {prefix}season = '{schema.name}'"


def build_team_join(
    schema: SeasonSchema,
    player_alias: str = "p",
    team_alias: str = "t"
) -> Tuple[str, str]:
    """
    Build a team join clause if supported.
    
    Returns:
        Tuple of (join_clause, select_fragment) or ("", "") if teams not supported.
        
    Example:
        join_clause: "LEFT JOIN fpl_season_teams t ON p.team_id = t.team_id AND t.season = '2019-20'"
        select_fragment: "t.team_name"
    """
    if not schema.supports_teams:
        return ("", "")
    
    if schema.is_historical:
        join = (
            f"LEFT JOIN {schema.table_teams} {team_alias} "
            f"ON {player_alias}.team_id = {team_alias}.{schema.col_team_id} "
            f"AND {team_alias}.season = '{schema.name}'"
        )
        select = f"{team_alias}.{schema.col_team_name}"
    else:
        join = (
            f"LEFT JOIN {schema.table_teams} {team_alias} "
            f"ON {player_alias}.team = {team_alias}.{schema.col_team_id}"
        )
        select = f"{team_alias}.{schema.col_team_name}"
    
    return (join, select)


def build_player_join(
    schema: SeasonSchema,
    fact_alias: str = "f",
    player_alias: str = "p"
) -> str:
    """
    Build a player dimension join clause.
    
    Returns:
        SQL JOIN clause fragment.
    
    Note: Uses col_player_id (fact) -> col_player_table_id (player) mapping.
    """
    if schema.is_historical:
        return (
            f"JOIN {schema.table_players} {player_alias} "
            f"ON {fact_alias}.{schema.col_player_id} = {player_alias}.{schema.col_player_table_id} "
            f"AND {player_alias}.season = '{schema.name}'"
        )
    else:
        # Current season: fact.player_id = players.id
        return (
            f"JOIN {schema.table_players} {player_alias} "
            f"ON {fact_alias}.{schema.col_player_id} = {player_alias}.{schema.col_player_table_id}"
        )


def build_understat_join(
    schema: SeasonSchema,
    player_alias: str = "p",
    understat_alias: str = "us"
) -> Tuple[str, str]:
    """
    Build an understat roster metrics join if supported.
    
    Returns:
        Tuple of (join_clause, select_fragment) or ("", "") if understat not supported.
        
    Note: 
    - Understat joins are by player name (loose match), not ID.
    - Select uses aggregation to satisfy GROUP BY requirements.
    """
    if not schema.supports_understat:
        return ("", "")
    
    join = (
        f"LEFT JOIN understat_roster_metrics {understat_alias} "
        f"ON {understat_alias}.player = CONCAT({player_alias}.first_name, ' ', {player_alias}.second_name)"
    )
    # CRITICAL: Use aggregation to avoid GROUP BY violations
    select = f"COALESCE(SUM({understat_alias}.xg), 0) as xG, COALESCE(SUM({understat_alias}.xa), 0) as xA"
    
    return (join, select)


def get_player_team_column(schema: SeasonSchema) -> str:
    """Get the column name for team reference in player table."""
    return "team_id" if schema.is_historical else "team"


def get_understat_season_year(season: str) -> Optional[int]:
    """
    Convert FPL season format to Understat year format.
    
    Examples:
        "2019-20" -> 2019
        "2024-25" -> 2024
    """
    try:
        return int(season.split("-")[0])
    except (ValueError, AttributeError, IndexError):
        return None
