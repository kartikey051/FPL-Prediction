"""
Dashboard Service - Refactored for Correctness (No SQL Joins between FPL/Understat)

This service implements a Fetcher-Merger pattern:
1. Fetch FPL data (Source of Truth)
2. Fetch Understat data (Enrichment)
3. Merge in Python using normalized names + season context

NEVER joins FPL and Understat tables in SQL.
"""

import unicodedata
from typing import List, Optional, Dict, Any
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
    build_team_join, build_player_join, build_standings_xg_query, 
    get_understat_season_year, get_player_team_column, CURRENT_SEASON
)
from Utils.logging_config import get_logger

logger = get_logger("dashboard_service")


def normalize_name(name: str) -> str:
    """Normalize player name for fuzzy matching (remove accents, lowercase)."""
    if not name:
        return ""
    n = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
    return n.lower().strip()


def normalize_team_name(name: str) -> str:
    """
    Normalize FPL team names to match Understat team names.
    Handles specific aliases like 'Man City' -> 'Manchester City'.
    """
    clean_name = normalize_name(name)
    
    # Mapping FPL (normalized) -> Understat (normalized)
    mapping = {
        "man city": "manchester city",
        "man utd": "manchester united",
        "spurs": "tottenham",
        "nott'm forest": "nottingham forest",
        "nottingham forest": "nottingham forest", 
        "newcastle": "newcastle united",
        "sheffield utd": "sheffield united",
        "wolves": "wolverhampton wanderers",
        "wolverhampton": "wolverhampton wanderers",
        "brighton": "brighton",
        "leicester": "leicester",
        "leeds": "leeds",
        "west ham": "west ham",
        "stoke": "stoke",
        "west brom": "west bromwich albion",
        "swansea": "swansea",
        "hull": "hull",
        "bournemouth": "bournemouth",
        "watford": "watford",
        "middlesbrough": "middlesbrough",
        "cardiff": "cardiff",
        "fulham": "fulham",
        "huddersfield": "huddersfield",
        "norwich": "norwich",
        "qpr": "queens park rangers",
        "reading": "reading",
        "wigan": "wigan",
        # Add more as needed based on observation
    }
    
    return mapping.get(clean_name, clean_name)


def _resolve_team_player_ids(schema: SeasonSchema, team_id: int) -> List[int]:
    """Resolve player IDs for a given team using FPL schema."""
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


def _fetch_understat_squad_stats(season_year: int) -> Dict[str, Dict[str, float]]:
    """
    Fetch Understat squad stats filtered by season.
    
    The understat_roster_metrics table has a 'season' column added via
    the ingestion script (derived from match_link -> match_id -> season).
    Returns empty dict if no data exists for the requested season.
    """
    try:
        # Check if season column exists in the table
        check_query = """
            SELECT COUNT(*) as cnt 
            FROM information_schema.columns 
            WHERE table_name = 'understat_roster_metrics' 
            AND column_name = 'season'
        """
        check_result = execute_query(check_query)
        has_season_col = check_result[0]["cnt"] > 0 if check_result else False
        
        if has_season_col:
            # Check if requested season has data
            available_query = f"""
                SELECT COUNT(*) as cnt
                FROM understat_roster_metrics 
                WHERE season = {season_year}
            """
            available_result = execute_query(available_query)
            
            if available_result and available_result[0]["cnt"] == 0:
                # No data for this season - return empty (show "—" in UI)
                logger.info(f"No Understat data available for season {season_year}")
                return {}
            
            # Season-specific aggregation
            query = f"""
                SELECT 
                    player, 
                    SUM(xG) as xg, 
                    SUM(xA) as xa,
                    SUM(shots) as shots,
                    SUM(time) as minutes
                FROM understat_roster_metrics
                WHERE season = {season_year}
                GROUP BY player
            """
        else:
            # No season column - return empty to avoid mismatched data
            logger.warning("No season column in understat_roster_metrics, skipping xG data")
            return {}
        
        results = execute_query(query)
        
        mapping = {}
        for r in results:
            norm_name = normalize_name(r["player"])
            mapping[norm_name] = {
                "xG": float(r["xg"] or 0),
                "xA": float(r["xa"] or 0),
                "shots": int(r["shots"] or 0),
                "minutes": int(r["minutes"] or 0)
            }
        return mapping
    except Exception as e:
        logger.warning(f"Understat fetch failed: {e}")
        return {}


def get_summary_stats(team_id: Optional[int] = None, season: str = CURRENT_SEASON) -> SummaryStats:
    """Fact-first summary stats (FPL Only)."""
    schema = get_season_schema(season)
    
    try:
        player_ids = _resolve_team_player_ids(schema, team_id) if team_id else None
        
        # Player Count & Value
        team_col = get_player_team_column(schema)
        p_conditions = []
        if schema.is_historical:
            p_conditions.append(f"season = '{schema.name}'")
        if team_id:
            p_conditions.append(f"{team_col} = {team_id}")
        
        p_where = "WHERE " + " AND ".join(p_conditions) if p_conditions else ""
        p_query = f"SELECT COUNT(*) as count, AVG(now_cost) as avg_val FROM {schema.table_players} {p_where}"
        p_result = execute_query(p_query)
        
        # Performance Facts
        f_conditions = []
        if schema.is_historical:
            f_conditions.append(f"season = '{schema.name}'")
        if player_ids is not None:
            if not player_ids:
                return SummaryStats(0, 20, 380, 38, 0, 0, 0, 0)
            f_conditions.append(f"{schema.col_player_id} IN ({','.join(map(str, player_ids))})")
        
        f_where = "WHERE " + " AND ".join(f_conditions) if f_conditions else ""
        f_query = f"SELECT SUM(total_points) as pts, SUM(goals_scored) as g, SUM(assists) as a FROM {schema.table_fact} {f_where}"
        f_result = execute_query(f_query)
        
        total_players = p_result[0]["count"] if p_result else 0
        total_points = f_result[0]["pts"] or 0 if f_result else 0
        
        return SummaryStats(
            total_players=total_players,
            total_teams=20,
            total_fixtures=380,
            total_gameweeks=_get_max_gameweek(schema),
            avg_points_per_player=round(total_points / max(total_players, 1), 2),
            total_goals=int(f_result[0]["g"] or 0) if f_result else 0,
            total_assists=int(f_result[0]["a"] or 0) if f_result else 0,
            avg_player_value=round((p_result[0]["avg_val"] or 0) / 10, 1) if p_result else 0
        )
    except Exception as e:
        logger.error(f"Summary error: {e}")
        return SummaryStats(0, 0, 0, 0, 0, 0, 0, 0)


def get_gameweek_trends(team_id: Optional[int] = None, season: str = CURRENT_SEASON) -> TrendsResponse:
    """Gap-free trends (FPL Only)."""
    schema = get_season_schema(season)
    max_gw = _get_max_gameweek(schema)
    
    player_ids = _resolve_team_player_ids(schema, team_id) if team_id else None
    
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
        return TrendsResponse(data=[TrendDataPoint(**row) for row in results])
    except Exception as e:
        logger.error(f"Trends error: {e}")
        return TrendsResponse(data=[])


def get_top_players(limit: int = 10, season: str = CURRENT_SEASON) -> List[TopPlayer]:
    """Top players (FPL Only)."""
    schema = get_season_schema(season)
    
    join_clause, select_frag = build_team_join(schema, "p", "t")
    team_select = select_frag if select_frag else "'Unknown'"
    season_filter = build_season_filter(schema, "f")
    
    # Correct player join
    if schema.is_historical:
        p_join = f"JOIN {schema.table_players} p ON f.element_id = p.element_id AND p.season = '{schema.name}'"
    else:
        p_join = f"JOIN {schema.table_players} p ON f.player_id = p.id"

    query = f"""
        SELECT 
            f.{schema.col_player_id} as player_id,
            CONCAT(p.first_name, ' ', p.second_name) as player_name,
            {team_select} as team_name,
            SUM(f.total_points) as total_points,
            SUM(f.goals_scored) as total_goals,
            SUM(f.assists) as total_assists
        FROM {schema.table_fact} f
        {p_join}
        {join_clause}
        WHERE 1=1 {season_filter}
        GROUP BY f.{schema.col_player_id}, p.first_name, p.second_name
        ORDER BY total_points DESC
        LIMIT {limit}
    """
    
    try:
        results = execute_query(query)
        return [TopPlayer(**row) for row in results]
    except Exception as e:
        logger.error(f"Top players error: {e}")
        return []


def get_team_squad(team_id: int, season: str = CURRENT_SEASON) -> TeamSquadResponse:
    """
    Decoupled Squad Analytics:
    1. FPL Stats from DB
    2. Understat Stats from DB (separate query)
    3. Merge in Python
    """
    schema = get_season_schema(season)
    
    try:
        # 1. Fetch Team Name
        team_name = "Unknown"
        if schema.supports_teams:
            q = f"SELECT {schema.col_team_name} as name FROM {schema.table_teams} WHERE {schema.col_team_id} = %s {build_season_filter(schema)}"
            res = execute_query(q, (team_id,))
            if res:
                team_name = res[0]["name"]

        # 2. Fetch FPL Stats
        team_col = get_player_team_column(schema)
        season_filter = build_season_filter(schema, "p")
        
        # Position logic
        pos_code = """
            CASE p.element_type 
                WHEN 1 THEN 'GKP' WHEN 2 THEN 'DEF' WHEN 3 THEN 'MID' WHEN 4 THEN 'FWD' ELSE 'UNK' 
            END
        """ if schema.is_historical else "NULL" # Current season might need join with positions table, but simplified here
        
        # For current season, relying on p.element_type existence in 'players' table logic
        if not schema.is_historical:
             # 'players' table has element_type from bootstrap-static
             pos_code = """
                CASE p.element_type 
                    WHEN 1 THEN 'GKP' WHEN 2 THEN 'DEF' WHEN 3 THEN 'MID' WHEN 4 THEN 'FWD' ELSE 'UNK' 
                END
             """

        if schema.is_historical:
            # Revert to LEFT JOIN to match original behavior (Show all players in team, even if 0 pointers)
            # This fixes "used to work fine" feedback
            fpl_query = f"""
                SELECT 
                    p.{schema.col_player_table_id} as player_id,
                    CONCAT(p.first_name, ' ', p.second_name) as name,
                    {pos_code} as position,
                    COALESCE(SUM(f.total_points), 0) as total_points,
                    COALESCE(SUM(f.goals_scored), 0) as goals,
                    COALESCE(SUM(f.assists), 0) as assists,
                    COALESCE(SUM(f.minutes), 0) as minutes,
                    COALESCE(MAX(p.now_cost), 0) / 10.0 as now_cost
                FROM {schema.table_players} p
                LEFT JOIN {schema.table_fact} f 
                    ON p.{schema.col_player_table_id} = f.{schema.col_player_id} 
                    {build_season_filter(schema, "f")}
                WHERE p.{team_col} = %s
                GROUP BY p.{schema.col_player_table_id}, p.first_name, p.second_name, p.element_type
                ORDER BY total_points DESC
            """
        else:
            # Current season (no season column on players table)
            fpl_query = f"""
                SELECT 
                    p.{schema.col_player_table_id} as player_id,
                    CONCAT(p.first_name, ' ', p.second_name) as name,
                    {pos_code} as position,
                    COALESCE(SUM(f.total_points), 0) as total_points,
                    COALESCE(SUM(f.goals_scored), 0) as goals,
                    COALESCE(SUM(f.assists), 0) as assists,
                    COALESCE(SUM(f.minutes), 0) as minutes,
                    COALESCE(MAX(p.now_cost), 0) / 10.0 as now_cost
                FROM {schema.table_players} p
                LEFT JOIN {schema.table_fact} f 
                    ON p.{schema.col_player_table_id} = f.{schema.col_player_id} 
                    {build_season_filter(schema, "f")}
                WHERE p.{team_col} = %s
                GROUP BY p.{schema.col_player_table_id}, p.first_name, p.second_name, p.element_type
                ORDER BY total_points DESC
            """
        
        fpl_stats = execute_query(fpl_query, (team_id,))
        
        # 3. Fetch Understat (Enrichment - Safe Lifetime)
        # We use a safe aggregation by player name since season filtering is not possible
        us_metrics = {}
        if schema.supports_understat:
            # We pass 0 or any year, as the function now ignores year and gets lifetime stats
            # But to be clean we pass year if available
            year = get_understat_season_year(season) or 0
            us_metrics = _fetch_understat_squad_stats(year)
                
        # 4. Merge & Scale
        squad = []
        for p in fpl_stats:
            norm = normalize_name(p["name"])
            us = us_metrics.get(norm)
            
            xG = round(float(us["xG"]), 2) if us else None
            xA = round(float(us["xA"]), 2) if us else None
            
            pts90 = (p["total_points"] / (p["minutes"] / 90)) if p["minutes"] > 90 else 0.0
            
            squad.append(SquadMember(
                player_id=p["player_id"],
                name=p["name"],
                position=p["position"],
                total_points=int(p["total_points"]),
                goals=int(p["goals"]),
                assists=int(p["assists"]),
                minutes=int(p["minutes"]),
                now_cost=float(p["now_cost"]),
                form=0,
                consistency=0,
                pts_per_90=round(pts90, 2),
                xG=xG,
                xA=xA,
                xG_per_90=round(xG / (p["minutes"] / 90), 2) if xG and p["minutes"] > 90 else None,
                xA_per_90=round(xA / (p["minutes"] / 90), 2) if xA and p["minutes"] > 90 else None
            ))
            
        return TeamSquadResponse(team_name=team_name, season=season, players=squad)
        
    except Exception as e:
        logger.error(f"Squad error: {e}")
        return TeamSquadResponse(team_name="Error", season=season, players=[])


def get_league_standings(season: str = CURRENT_SEASON) -> StandingsResponse:
    """Decoupled Standings: FPL Fixtures + Understat Metrics."""
    schema = get_season_schema(season)
    if not schema.supports_standings:
        return StandingsResponse(season=season, standings=[])
    
    try:
        # 1. FPL Standings
        season_filter = build_season_filter(schema, "")
        fpl_query = f"""
            WITH MatchPts AS (
                SELECT team_h as tid, team_h_score as gf, team_a_score as ga,
                    CASE WHEN team_h_score > team_a_score THEN 3 WHEN team_h_score = team_a_score THEN 1 ELSE 0 END as pts
                FROM {schema.table_fixtures} WHERE finished = 1 {season_filter}
                UNION ALL
                SELECT team_a as tid, team_a_score as gf, team_h_score as ga,
                    CASE WHEN team_a_score > team_h_score THEN 3 WHEN team_a_score = team_h_score THEN 1 ELSE 0 END as pts
                FROM {schema.table_fixtures} WHERE finished = 1 {season_filter}
            )
            SELECT 
                t.{schema.col_team_name} as team_name,
                COUNT(m.tid) as played,
                SUM(CASE WHEN gf > ga THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN gf = ga THEN 1 ELSE 0 END) as draws,
                SUM(CASE WHEN gf < ga THEN 1 ELSE 0 END) as losses,
                SUM(gf) as goals_for,
                SUM(ga) as goals_against,
                SUM(gf) - SUM(ga) as goal_diff,
                SUM(pts) as points
            FROM {schema.table_teams} t
            LEFT JOIN MatchPts m ON t.{schema.col_team_id} = m.tid
            WHERE 1=1 {build_season_filter(schema, "t")}
            GROUP BY t.{schema.col_team_name}
            ORDER BY points DESC, goal_diff DESC
        """
        fpl_table = execute_query(fpl_query)
        
        # 2. Understat xG Table
        us_table = {}
        if schema.supports_understat:
            year = get_understat_season_year(season)
            if year:
                xg_query = build_standings_xg_query(year)
                try:
                    us_rows = execute_query(xg_query)
                    for r in us_rows:
                        us_table[normalize_name(r["team_name"])] = r
                except Exception as e:
                    # Retry with smarter dynamic column detection
                    if "Unknown column" in str(e):
                        logger.warning(f"Standings fetch failed, attempting dynamic column detection...")
                        try:
                            # inspect columns
                            col_check = execute_query("SELECT * FROM understat_team_metrics LIMIT 1")
                            if col_check:
                                keys = list(col_check[0].keys())
                                logger.info(f"Available Understat columns: {keys}")
                                
                                c_home, c_away = "team_h", "team_a" # defaults
                                
                                if "home_team" in keys: c_home = "home_team"
                                elif "h_team" in keys: c_home = "h_team"
                                elif "home" in keys: c_home = "home"
                                
                                if "away_team" in keys: c_away = "away_team"
                                elif "a_team" in keys: c_away = "a_team"
                                elif "away" in keys: c_away = "away"
                                
                                logger.info(f"Detected Understat columns: {c_home}/{c_away}")
                                retry_query = build_standings_xg_query(year, c_home, c_away)
                                us_rows = execute_query(retry_query)
                                for r in us_rows:
                                    us_table[normalize_name(r["team_name"])] = r
                        except Exception as e2:
                            logger.error(f"Standings retry failed: {e2}")
                    else:
                        logger.warning(f"Understat standings fetch failed: {e}")

        # 3. Merge
        standings = []
        for i, row in enumerate(fpl_table):
            # Use improved normalization for fuzzy matching
            tn = normalize_team_name(row["team_name"])
            us = us_table.get(tn)
            
            # If still no match, try raw normalized name just in case
            if not us:
                us = us_table.get(normalize_name(row["team_name"]))
            
            standings.append(StandingEntry(
                rank=i + 1,
                team_name=row["team_name"],
                played=int(row["played"]),
                wins=int(row["wins"]),
                draws=int(row["draws"]),
                losses=int(row["losses"]),
                goals_for=int(row["goals_for"]),
                goals_against=int(row["goals_against"]),
                goal_diff=int(row["goal_diff"]),
                points=int(row["points"]),
                clean_sheets=0,
                xG_for=round(float(us["xG_for"]), 2) if us else None,
                xG_against=round(float(us["xG_against"]), 2) if us else None,
                xPts=None
            ))
            
        return StandingsResponse(season=season, standings=standings)
        
    except Exception as e:
        logger.error(f"Standings error: {e}")
        return StandingsResponse(season=season, standings=[])


def get_player_trends(player_id: int, season: str = CURRENT_SEASON) -> PlayerTrendsResponse:
    """Gap-free player trends."""
    schema = get_season_schema(season)
    max_gw = _get_max_gameweek(schema)
    
    try:
        # Get Name
        season_filter = build_season_filter(schema, "")
        p_query = f"SELECT CONCAT(first_name, ' ', second_name) as name FROM {schema.table_players} WHERE {schema.col_player_table_id} = %s {season_filter}"
        p_res = execute_query(p_query, (player_id,))
        p_name = p_res[0]["name"] if p_res else "Unknown"
        
        query = f"""
            WITH RECURSIVE gw_axis AS (
                SELECT 1 as gw UNION ALL SELECT gw + 1 FROM gw_axis WHERE gw < {max_gw}
            ),
            Perf AS (
                SELECT {schema.col_gameweek} as gw, total_points as p, minutes as m, goals_scored as g, assists as a, value / 10 as v
                FROM {schema.table_fact}
                WHERE {schema.col_player_id} = %s {build_season_filter(schema, "")}
            )
            SELECT ax.gw, COALESCE(p.p,0) as p, COALESCE(p.m,0) as m, COALESCE(p.g,0) as g, COALESCE(p.a,0) as a, COALESCE(p.v,0) as v
            FROM gw_axis ax LEFT JOIN Perf p ON ax.gw = p.gw ORDER BY ax.gw
        """
        results = execute_query(query, (player_id,))
        trends = [PlayerTrendPoint(
            gameweek=r["gw"], points=int(r["p"]), minutes=int(r["m"]), goals=int(r["g"]), assists=int(r["a"]), value=float(r["v"]),
            opponent="-", was_home=False, xG=None, xA=None
        ) for r in results]
        
        return PlayerTrendsResponse(player_id=player_id, player_name=p_name, team_name="", trend=trends, overall_form=0)
    except Exception as e:
        logger.error(f"Player trend error: {e}")
        return PlayerTrendsResponse(player_id=player_id, player_name="Error", team_name="", trend=[], overall_form=0)


def get_distributions(season: str = CURRENT_SEASON) -> DistributionsResponse:
    """Distributions (FPL Only)."""
    schema = get_season_schema(season)
    try:
        season_filter = build_season_filter(schema, "")
        pos_query = f"""
            SELECT element_type, COUNT(*) as c, SUM(total_points) as pts 
            FROM {schema.table_players} WHERE 1=1 {season_filter} GROUP BY element_type
        """
        rows = execute_query(pos_query)
        
        pos_map = {1: 'GKP', 2: 'DEF', 3: 'MID', 4: 'FWD'}
        dist = [PositionDistribution(
            position=pos_map.get(r["element_type"], 'UNK'),
            position_id=r["element_type"],
            player_count=r["c"],
            total_goals=0,
            avg_points=round(float(r["pts"]) / max(r["c"], 1), 2)
        ) for r in rows]
        
        return DistributionsResponse(by_team=[], by_position=dist)
    except Exception as e:
        logger.error(f"Distribution error: {e}")
        return DistributionsResponse(by_team=[], by_position=[])


def get_global_players(filters: GlobalSearchFilters) -> List[TopPlayer]:
    """Safe Global Search with Python-side Understat enrichment & sorting."""
    schema = get_season_schema(filters.season)
    
    # 1. Build Conditions
    conditions = []
    if schema.is_historical:
        conditions.append(f"p.season = '{schema.name}'")
    if filters.name:
        safe_name = filters.name.replace("'", "''")
        conditions.append(f"(p.first_name LIKE '%{safe_name}%' OR p.second_name LIKE '%{safe_name}%')")
    if filters.team_id:
        conditions.append(f"p.{get_player_team_column(schema)} = {filters.team_id}")
    if filters.position:
        pm = {'GKP': 1, 'DEF': 2, 'MID': 3, 'FWD': 4}
        if filters.position in pm: conditions.append(f"p.element_type = {pm[filters.position]}")
    
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    # 2. Join Teams (if supported)
    join, t_col = "", "'Unknown'"
    if schema.supports_teams:
        j, s = build_team_join(schema, "p", "t")
        join, t_col = j, s
        
    # 3. Determine Sorting Strategy
    # If sorting by xG/xA, we must fetch ALL candidates, enrich, then sort in Python.
    manual_sort_key = None
    if filters.sort_by in ["xG", "xg", "xA", "xa"]:
        manual_sort_key = "xG" if filters.sort_by.lower() == "xg" else "xA"
        sql_order = ""
        sql_limit = "" # Fetch all to allow correct sorting
    else:
        # Standard SQL sort
        map_sort = {
            "form": "CAST(p.form as DECIMAL(10,1))",
            "value": "p.now_cost",
            "position": "p.element_type",
            "total_points": "p.total_points"
        }
        sort_col = map_sort.get(filters.sort_by, "p.total_points")
        sql_order = f"ORDER BY {sort_col} {filters.order.upper()}"
        sql_limit = "LIMIT 50"

    query = f"""
        SELECT 
            p.{schema.col_player_table_id} as pid, 
            CONCAT(p.first_name, ' ', p.second_name) as name, 
            {t_col} as tname, 
            p.total_points,
            p.goals_scored,
            p.assists
        FROM {schema.table_players} p {join} {where}
        {sql_order} {sql_limit}
    """
    
    try:
        res = execute_query(query)
        
        # 4. Enrich with Understat (Lifetime)
        us_metrics = {}
        if schema.supports_understat:
             us_metrics = _fetch_understat_squad_stats(0)

        players = []
        for r in res:
            norm = normalize_name(r["name"])
            us = us_metrics.get(norm)
            
            xG = round(float(us["xG"]), 2) if us else None
            xA = round(float(us["xA"]), 2) if us else None
            
            players.append(TopPlayer(
                player_id=r["pid"], 
                player_name=r["name"], 
                team_name=r["tname"], 
                total_points=r["total_points"], 
                total_goals=int(r.get("goals_scored") or 0),
                total_assists=int(r.get("assists") or 0),
                xG=xG,
                xA=xA
            ))
            
        # 5. Apply Manual Sort if needed
        if manual_sort_key:
            reverse = (filters.order.lower() == "desc")
            # Sort None values to bottom usually, or top if desc? 
            # Let's treat None as -1 so they go last in DESC sort
            players.sort(key=lambda x: (getattr(x, manual_sort_key) or -1.0), reverse=reverse)
            players = players[:50]
            
        return players
    except Exception as e:
        logger.error(f"Global search error: {e}")
        return []


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
        return DashboardFilters(seasons=[CURRENT_SEASON], teams=[], gameweeks=[])
