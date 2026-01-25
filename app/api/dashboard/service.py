"""
Dashboard Service - Refactored with Typed Season Schema

This service uses the new SeasonSchema infrastructure for:
- Safe, typed access to season-specific configuration
- Conditional feature support (teams, understat, standings)
- Fact-first architecture (fpl_player_gameweeks as primary source)
- Graceful degradation when features unavailable

NO UNSAFE DICT ACCESS - All configuration via typed SeasonSchema.
"""

from typing import List, Optional
from app.db.session import execute_query
from app.api.dashboard.schemas import (
    SummaryStats, TrendDataPoint, TrendsResponse,
    TeamDistribution, PositionDistribution, DistributionsResponse,
    TopPlayer, DashboardFilters, SquadMember, TeamSquadResponse,
    StandingEntry, StandingsResponse, PlayerTrendPoint, PlayerTrendsResponse,
    GlobalSearchFilters
)
from app.api.dashboard.season_config import (
    SeasonSchema, get_season_schema, build_season_filter, build_season_where,
    build_team_join, build_player_join, build_understat_join,
    get_player_team_column, CURRENT_SEASON
)
from Utils.logging_config import get_logger

logger = get_logger("dashboard_service")


def _resolve_team_player_ids(schema: SeasonSchema, team_id: int) -> List[int]:
    """
    Resolve player IDs for a given team.
    Fact table has no direct team context; we resolve via player dimension.
    """
    team_col = get_player_team_column(schema)
    season_filter = build_season_filter(schema)
    
    query = f"""
        SELECT {schema.col_player_table_id} as pid 
        FROM {schema.table_players} 
        WHERE {team_col} = %s {season_filter}
    """
    
    try:
        results = execute_query(query, (team_id,))
        return [r["pid"] for r in results]
    except Exception as e:
        logger.error(f"Team PID resolution failed ({schema.name}, team={team_id}): {e}")
        return []


def _get_max_gameweek(schema: SeasonSchema) -> int:
    """Get the maximum gameweek for a season."""
    season_where = build_season_where(schema)
    query = f"SELECT MAX({schema.col_gameweek}) as max_gw FROM {schema.table_fact} {season_where}"
    
    try:
        result = execute_query(query)
        return result[0]["max_gw"] or 38 if result else 38
    except Exception:
        return 38


def get_summary_stats(team_id: Optional[int] = None, season: str = CURRENT_SEASON) -> SummaryStats:
    """
    Fact-first summary stats with safe season handling.
    No KeyError possible - all config via typed schema.
    """
    schema = get_season_schema(season)
    
    try:
        # Resolve team player IDs if filtering by team
        player_ids = _resolve_team_player_ids(schema, team_id) if team_id else None
        
        # Player Metadata Query
        team_col = get_player_team_column(schema)
        p_conditions = []
        if schema.is_historical:
            p_conditions.append(f"season = '{schema.name}'")
        if team_id:
            p_conditions.append(f"{team_col} = {team_id}")
        
        p_where = "WHERE " + " AND ".join(p_conditions) if p_conditions else ""
        p_query = f"SELECT COUNT(*) as count, AVG(now_cost) as avg_val FROM {schema.table_players} {p_where}"
        p_result = execute_query(p_query)
        
        # Performance Facts Query
        f_conditions = []
        if schema.is_historical:
            f_conditions.append(f"season = '{schema.name}'")
        if player_ids is not None:
            if not player_ids:
                # No players found for team
                return SummaryStats(
                    total_players=0, total_teams=20, total_fixtures=380, total_gameweeks=38,
                    avg_points_per_player=0, total_goals=0, total_assists=0, avg_player_value=0
                )
            f_conditions.append(f"{schema.col_player_id} IN ({','.join(map(str, player_ids))})")
        
        f_where = "WHERE " + " AND ".join(f_conditions) if f_conditions else ""
        f_query = f"""
            SELECT SUM(total_points) as pts, SUM(goals_scored) as g, SUM(assists) as a 
            FROM {schema.table_fact} {f_where}
        """
        f_result = execute_query(f_query)
        
        total_players = p_result[0]["count"] if p_result else 0
        total_points = f_result[0]["pts"] or 0 if f_result else 0
        total_goals = f_result[0]["g"] or 0 if f_result else 0
        total_assists = f_result[0]["a"] or 0 if f_result else 0
        avg_val = p_result[0]["avg_val"] or 0 if p_result else 0
        
        return SummaryStats(
            total_players=total_players,
            total_teams=20,
            total_fixtures=380,
            total_gameweeks=_get_max_gameweek(schema),
            avg_points_per_player=round(total_points / max(total_players, 1), 2),
            total_goals=int(total_goals),
            total_assists=int(total_assists),
            avg_player_value=round(avg_val / 10, 1)
        )
    except Exception as e:
        logger.error(f"Summary stats error ({season}): {e}")
        return SummaryStats(
            total_players=0, total_teams=0, total_fixtures=0, total_gameweeks=0,
            avg_points_per_player=0, total_goals=0, total_assists=0, avg_player_value=0
        )


def get_gameweek_trends(team_id: Optional[int] = None, season: str = CURRENT_SEASON) -> TrendsResponse:
    """
    Gap-free gameweek trends using recursive CTE.
    Works for all seasons without joining dimension tables.
    """
    schema = get_season_schema(season)
    max_gw = _get_max_gameweek(schema)
    
    # Resolve team player IDs if filtering
    player_ids = _resolve_team_player_ids(schema, team_id) if team_id else None
    
    # Build WHERE conditions
    conditions = []
    if schema.is_historical:
        conditions.append(f"season = '{schema.name}'")
    if player_ids is not None:
        if not player_ids:
            return TrendsResponse(data=[])
        conditions.append(f"{schema.col_player_id} IN ({','.join(map(str, player_ids))})")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    query = f"""
        WITH RECURSIVE gw_axis AS (
            SELECT 1 as gw
            UNION ALL
            SELECT gw + 1 FROM gw_axis WHERE gw < {max_gw}
        ),
        TrendFacts AS (
            SELECT 
                {schema.col_gameweek} as gw,
                SUM(total_points) as pts,
                SUM(goals_scored) as g,
                SUM(assists) as a,
                AVG(minutes) as m
            FROM {schema.table_fact}
            {where_clause}
            GROUP BY {schema.col_gameweek}
        )
        SELECT 
            ax.gw as gameweek,
            COALESCE(tf.pts, 0) as total_points,
            COALESCE(tf.g, 0) as total_goals,
            COALESCE(tf.a, 0) as total_assists,
            COALESCE(tf.m, 0) as avg_minutes
        FROM gw_axis ax
        LEFT JOIN TrendFacts tf ON ax.gw = tf.gw
        ORDER BY ax.gw
    """
    
    try:
        results = execute_query(query)
        data = [TrendDataPoint(**row) for row in results]
        return TrendsResponse(data=data)
    except Exception as e:
        logger.error(f"Trends error ({season}): {e}")
        return TrendsResponse(data=[])


def get_top_players(limit: int = 10, season: str = CURRENT_SEASON) -> List[TopPlayer]:
    """
    Top performers using fact-first approach.
    Team join is OPTIONAL and only used when supports_teams=True.
    """
    schema = get_season_schema(season)
    
    # Build player join
    player_join = build_player_join(schema, "f", "p")
    
    # Conditionally add team join
    team_join_clause, team_select = build_team_join(schema, "p", "t")
    team_name_select = team_select if team_select else "'Unknown'"
    
    # Season filter for fact table
    season_filter = build_season_filter(schema, "f")
    
    query = f"""
        SELECT 
            f.{schema.col_player_id} as player_id,
            CONCAT(p.first_name, ' ', p.second_name) as player_name,
            {team_name_select} as team_name,
            SUM(f.total_points) as total_points,
            SUM(f.goals_scored) as total_goals,
            SUM(f.assists) as total_assists
        FROM {schema.table_fact} f
        {player_join}
        {team_join_clause}
        WHERE 1=1 {season_filter}
        GROUP BY f.{schema.col_player_id}, p.first_name, p.second_name
        ORDER BY total_points DESC
        LIMIT {limit}
    """
    
    try:
        results = execute_query(query)
        return [TopPlayer(**row) for row in results]
    except Exception as e:
        logger.error(f"Top players error ({season}): {e}")
        return []


def get_team_squad(team_id: int, season: str = CURRENT_SEASON) -> TeamSquadResponse:
    """
    Team squad with optional understat enrichment.
    Graceful degradation when teams or understat unavailable.
    """
    schema = get_season_schema(season)
    
    try:
        # Get team name if teams supported
        team_name = "Unknown"
        if schema.supports_teams:
            season_filter = build_season_filter(schema, "")
            t_query = f"""
                SELECT {schema.col_team_name} as name 
                FROM {schema.table_teams} 
                WHERE {schema.col_team_id} = %s {season_filter}
            """
            t_result = execute_query(t_query, (team_id,))
            team_name = t_result[0]["name"] if t_result else "Unknown"
        
        # Get team players
        team_col = get_player_team_column(schema)
        season_filter = build_season_filter(schema, "p")
        
        # Build understat join conditionally
        understat_join, understat_select = build_understat_join(schema, "p", "us")
        xg_select = understat_select if understat_select else "0 as xG, 0 as xA"
        
        # Position mapping for historical data
        position_case = """
            CASE p.element_type 
                WHEN 1 THEN 'GKP' 
                WHEN 2 THEN 'DEF' 
                WHEN 3 THEN 'MID' 
                WHEN 4 THEN 'FWD' 
                ELSE 'N/A' 
            END
        """ if schema.is_historical else "'N/A'"
        
        query = f"""
            SELECT 
                p.{schema.col_player_table_id} as player_id,
                CONCAT(p.first_name, ' ', p.second_name) as name,
                {position_case} as position,
                COALESCE(SUM(f.total_points), 0) as total_points,
                COALESCE(SUM(f.goals_scored), 0) as goals,
                COALESCE(SUM(f.assists), 0) as assists,
                COALESCE(SUM(f.minutes), 0) as minutes,
                COALESCE(MAX(p.now_cost), 0) / 10 as now_cost,
                COALESCE(SUM(f.total_points) / NULLIF(SUM(f.minutes) / 90, 0), 0) as pts_per_90,
                {xg_select}
            FROM {schema.table_players} p
            LEFT JOIN {schema.table_fact} f 
                ON p.{schema.col_player_table_id} = f.{schema.col_player_id} 
                {build_season_filter(schema, "f")}
            {understat_join}
            WHERE p.{team_col} = %s {season_filter}
            GROUP BY p.{schema.col_player_table_id}, p.first_name, p.second_name, p.element_type
            ORDER BY total_points DESC
        """
        
        results = execute_query(query, (team_id,))
        players = [
            SquadMember(
                **row,
                form=0,
                consistency=0,
                xG_per_90=0,
                xA_per_90=0
            ) for row in results
        ]
        
        return TeamSquadResponse(team_name=team_name, season=season, players=players)
        
    except Exception as e:
        logger.error(f"Team squad error ({season}): {e}")
        return TeamSquadResponse(team_name="Error", season=season, players=[])


def get_league_standings(season: str = CURRENT_SEASON) -> StandingsResponse:
    """
    League standings from fixtures.
    Returns empty standings for seasons without teams table support.
    """
    schema = get_season_schema(season)
    
    # Graceful degradation: standings require teams table
    if not schema.supports_standings:
        logger.warning(f"Standings not supported for season {season}")
        return StandingsResponse(season=season, standings=[])
    
    try:
        season_filter = build_season_filter(schema, "")
        
        query = f"""
            WITH MatchPts AS (
                SELECT 
                    team_h as tid, 
                    team_h_score as gf, 
                    team_a_score as ga,
                    CASE 
                        WHEN team_h_score > team_a_score THEN 3 
                        WHEN team_h_score = team_a_score THEN 1 
                        ELSE 0 
                    END as pts
                FROM {schema.table_fixtures} 
                WHERE finished = 1 {season_filter}
                UNION ALL
                SELECT 
                    team_a as tid, 
                    team_a_score as gf, 
                    team_h_score as ga,
                    CASE 
                        WHEN team_a_score > team_h_score THEN 3 
                        WHEN team_a_score = team_h_score THEN 1 
                        ELSE 0 
                    END as pts
                FROM {schema.table_fixtures} 
                WHERE finished = 1 {season_filter}
            )
            SELECT 
                t.{schema.col_team_name} as team_name,
                COUNT(*) as played,
                SUM(CASE WHEN gf > ga THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN gf = ga THEN 1 ELSE 0 END) as draws,
                SUM(CASE WHEN gf < ga THEN 1 ELSE 0 END) as losses,
                SUM(gf) as goals_for,
                SUM(ga) as goals_against,
                SUM(gf) - SUM(ga) as goal_diff,
                SUM(pts) as points,
                0 as clean_sheets
            FROM MatchPts m
            JOIN {schema.table_teams} t 
                ON m.tid = t.{schema.col_team_id} {season_filter}
            GROUP BY t.{schema.col_team_name}
            ORDER BY points DESC, goal_diff DESC
        """
        
        results = execute_query(query)
        standings = [
            StandingEntry(
                rank=i + 1,
                **row,
                xG_for=0,
                xG_against=0,
                xPts=0
            ) for i, row in enumerate(results)
        ]
        
        return StandingsResponse(season=season, standings=standings)
        
    except Exception as e:
        logger.error(f"Standings error ({season}): {e}")
        return StandingsResponse(season=season, standings=[])


def get_player_trends(player_id: int, season: str = CURRENT_SEASON) -> PlayerTrendsResponse:
    """
    Gap-free player trends using recursive CTE.
    No unsafe dict access.
    """
    schema = get_season_schema(season)
    max_gw = _get_max_gameweek(schema)
    
    try:
        # Get player name
        season_filter = build_season_filter(schema, "")
        p_query = f"""
            SELECT CONCAT(first_name, ' ', second_name) as name 
            FROM {schema.table_players} 
            WHERE {schema.col_player_table_id} = %s {season_filter}
        """
        p_result = execute_query(p_query, (player_id,))
        player_name = p_result[0]["name"] if p_result else "Unknown"
        
        # Build season filter for fact table
        fact_season_filter = build_season_filter(schema, "")
        
        query = f"""
            WITH RECURSIVE gw_axis AS (
                SELECT 1 as gw
                UNION ALL
                SELECT gw + 1 FROM gw_axis WHERE gw < {max_gw}
            ),
            Perf AS (
                SELECT 
                    {schema.col_gameweek} as gw,
                    total_points as p,
                    minutes as m,
                    goals_scored as g,
                    assists as a,
                    value / 10 as v
                FROM {schema.table_fact}
                WHERE {schema.col_player_id} = %s {fact_season_filter}
            )
            SELECT 
                ax.gw as gameweek,
                COALESCE(perf.p, 0) as points,
                COALESCE(perf.m, 0) as minutes,
                COALESCE(perf.g, 0) as goals,
                COALESCE(perf.a, 0) as assists,
                COALESCE(perf.v, 0) as value,
                '-' as opponent,
                0 as was_home,
                0 as xG,
                0 as xA
            FROM gw_axis ax
            LEFT JOIN Perf ON ax.gw = Perf.gw
            ORDER BY ax.gw
        """
        
        results = execute_query(query, (player_id,))
        trend = [PlayerTrendPoint(**row) for row in results]
        
        return PlayerTrendsResponse(
            player_id=player_id,
            player_name=player_name,
            team_name="",
            trend=trend,
            overall_form=0
        )
        
    except Exception as e:
        logger.error(f"Player trends error ({season}): {e}")
        return PlayerTrendsResponse(
            player_id=player_id,
            player_name="Error",
            team_name="",
            trend=[],
            overall_form=0
        )


def get_available_filters() -> DashboardFilters:
    """Get available seasons, teams, and gameweeks for filtering."""
    try:
        # Get all seasons from historical players table
        seasons = [
            r["season"] 
            for r in execute_query("SELECT DISTINCT season FROM fpl_season_players ORDER BY season DESC")
        ]
        if CURRENT_SEASON not in seasons:
            seasons.insert(0, CURRENT_SEASON)
        
        # Get teams from current season (canonical baseline)
        teams = [
            {"id": t["id"], "name": t["name"]} 
            for t in execute_query("SELECT id, name FROM teams ORDER BY name")
        ]
        
        return DashboardFilters(
            seasons=seasons,
            teams=teams,
            gameweeks=list(range(1, 39))
        )
    except Exception as e:
        logger.error(f"get_available_filters error: {e}")
        return DashboardFilters()


def get_distributions(season: str = CURRENT_SEASON) -> DistributionsResponse:
    """Era-aware distribution data."""
    schema = get_season_schema(season)
    
    try:
        # Position distribution
        season_filter = build_season_filter(schema, "")
        pos_query = f"""
            SELECT 
                element_type as position_id,
                CASE element_type 
                    WHEN 1 THEN 'GKP' 
                    WHEN 2 THEN 'DEF' 
                    WHEN 3 THEN 'MID' 
                    WHEN 4 THEN 'FWD' 
                END as position,
                COUNT(*) as player_count,
                SUM(total_points) as total_goals,
                AVG(total_points) as avg_points
            FROM {schema.table_players}
            WHERE 1=1 {season_filter}
            GROUP BY element_type
        """
        pos_results = execute_query(pos_query)
        by_position = [PositionDistribution(**row) for row in pos_results]
        
        # Team distribution (only if supported)
        by_team = []
        if schema.supports_teams:
            team_col = get_player_team_column(schema)
            team_join, team_select = build_team_join(schema, "p", "t")
            
            team_query = f"""
                SELECT 
                    p.{team_col} as team_id,
                    {team_select} as team_name,
                    SUM(p.total_points) as total_points,
                    COUNT(*) as player_count,
                    0 as total_goals
                FROM {schema.table_players} p
                {team_join}
                WHERE 1=1 {build_season_filter(schema, "p")}
                GROUP BY p.{team_col}, {team_select}
            """
            team_results = execute_query(team_query)
            by_team = [TeamDistribution(**row) for row in team_results]
        
        return DistributionsResponse(by_team=by_team, by_position=by_position)
        
    except Exception as e:
        logger.error(f"Distributions error ({season}): {e}")
        return DistributionsResponse(by_team=[], by_position=[])


def get_global_players(filters: GlobalSearchFilters) -> List[TopPlayer]:
    """
    Global player search with OPTIONAL team joins.
    Works for all seasons including those without teams table.
    """
    schema = get_season_schema(filters.season)
    
    # Build conditions
    conditions = []
    if schema.is_historical:
        conditions.append(f"p.season = '{schema.name}'")
    if filters.name:
        # Escape single quotes to prevent SQL injection
        safe_name = filters.name.replace("'", "''")
        conditions.append(
            f"(p.first_name LIKE '%{safe_name}%' OR p.second_name LIKE '%{safe_name}%')"
        )
    if filters.team_id:
        team_col = get_player_team_column(schema)
        conditions.append(f"p.{team_col} = {filters.team_id}")
    if filters.position:
        pos_map = {"GKP": 1, "DEF": 2, "MID": 3, "FWD": 4}
        if filters.position in pos_map:
            conditions.append(f"p.element_type = {pos_map[filters.position]}")
    if filters.min_points:
        conditions.append(f"p.total_points >= {filters.min_points}")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    # Conditionally add team join
    team_join_clause, team_select = build_team_join(schema, "p", "t")
    team_name_select = team_select if team_select else "'Unknown'"
    
    # Sort column mapping
    sort_col = {
        "total_points": "p.total_points",
        "name": "p.second_name",
        "form": "p.total_points"
    }.get(filters.sort_by, "p.total_points")
    
    sort_order = "DESC" if filters.order.lower() == "desc" else "ASC"
    
    query = f"""
        SELECT 
            p.{schema.col_player_table_id} as player_id,
            CONCAT(p.first_name, ' ', p.second_name) as player_name,
            {team_name_select} as team_name,
            p.total_points,
            0 as total_goals,
            0 as total_assists
        FROM {schema.table_players} p
        {team_join_clause}
        {where_clause}
        ORDER BY {sort_col} {sort_order}
        LIMIT 50
    """
    
    try:
        results = execute_query(query)
        return [TopPlayer(**row) for row in results]
    except Exception as e:
        logger.error(f"Global search error ({filters.season}): {e}")
        return []
