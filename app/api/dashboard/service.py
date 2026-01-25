"""
Dashboard service - queries real data from FPL database tables.
"""

from typing import List, Optional, Dict, Any

from app.db.session import execute_query
from app.api.dashboard.schemas import (
    SummaryStats,
    TrendDataPoint,
    TrendsResponse,
    TeamDistribution,
    PositionDistribution,
    DistributionsResponse,
    TopPlayer,
    DashboardFilters,
)
from Utils.logging_config import get_logger

logger = get_logger("dashboard_service")


def get_summary_stats(team_id: Optional[int] = None) -> SummaryStats:
    """
    Get key performance indicators from the database.
    
    Queries:
    - players table: count, avg value
    - teams table: count
    - fixtures table: count
    - fact_player_gameweeks: points, goals, assists
    """
    try:
        # Player count and avg value
        player_query = "SELECT COUNT(*) as count, AVG(now_cost) as avg_value FROM players"
        if team_id:
            player_query += f" WHERE team = {team_id}"
        player_result = execute_query(player_query)
        total_players = player_result[0]["count"] if player_result else 0
        avg_value = (player_result[0]["avg_value"] or 0) / 10 if player_result else 0  # FPL stores in tenths
        
        # Team count
        team_result = execute_query("SELECT COUNT(*) as count FROM teams")
        total_teams = team_result[0]["count"] if team_result else 0
        
        # Fixture count
        fixture_result = execute_query("SELECT COUNT(*) as count FROM fixtures")
        total_fixtures = fixture_result[0]["count"] if fixture_result else 0
        
        # Gameweek stats from fact table
        fact_query = """
            SELECT 
                COUNT(DISTINCT event) as gw_count,
                AVG(total_points) as avg_points,
                SUM(goals_scored) as total_goals,
                SUM(assists) as total_assists
            FROM fact_player_gameweeks
        """
        if team_id:
            fact_query += f" WHERE team_id = {team_id}"
        
        fact_result = execute_query(fact_query)
        
        if fact_result and fact_result[0]:
            gw_count = fact_result[0]["gw_count"] or 0
            avg_points = round(fact_result[0]["avg_points"] or 0, 2)
            total_goals = fact_result[0]["total_goals"] or 0
            total_assists = fact_result[0]["total_assists"] or 0
        else:
            gw_count, avg_points, total_goals, total_assists = 0, 0, 0, 0
        
        return SummaryStats(
            total_players=total_players,
            total_teams=total_teams,
            total_fixtures=total_fixtures,
            total_gameweeks=gw_count,
            avg_points_per_player=avg_points,
            total_goals=total_goals,
            total_assists=total_assists,
            avg_player_value=round(avg_value, 1),
        )
    
    except Exception as e:
        logger.error(f"Error fetching summary stats: {e}")
        # Return zeros if tables don't exist yet
        return SummaryStats(
            total_players=0,
            total_teams=0,
            total_fixtures=0,
            total_gameweeks=0,
            avg_points_per_player=0,
            total_goals=0,
            total_assists=0,
            avg_player_value=0,
        )


def get_gameweek_trends(team_id: Optional[int] = None) -> TrendsResponse:
    """
    Get trends data aggregated by gameweek.
    
    Returns points, goals, assists per gameweek for charts.
    """
    try:
        query = """
            SELECT 
                event as gameweek,
                SUM(total_points) as total_points,
                SUM(goals_scored) as total_goals,
                SUM(assists) as total_assists,
                AVG(minutes) as avg_minutes
            FROM fact_player_gameweeks
        """
        
        if team_id:
            query += f" WHERE team_id = {team_id}"
        
        query += " GROUP BY event ORDER BY event"
        
        results = execute_query(query)
        
        data = []
        for row in results:
            data.append(TrendDataPoint(
                gameweek=row["gameweek"],
                total_points=float(row["total_points"] or 0),
                total_goals=row["total_goals"] or 0,
                total_assists=row["total_assists"] or 0,
                avg_minutes=round(float(row["avg_minutes"] or 0), 1),
            ))
        
        return TrendsResponse(data=data)
    
    except Exception as e:
        logger.error(f"Error fetching trends: {e}")
        return TrendsResponse(data=[])


def get_distributions(team_id: Optional[int] = None) -> DistributionsResponse:
    """
    Get distribution data for charts.
    
    - Goals/points by team
    - Player count by position
    """
    try:
        # Distribution by team
        team_query = """
            SELECT 
                t.name as team_name,
                t.id as team_id,
                COALESCE(SUM(f.goals_scored), 0) as total_goals,
                COALESCE(SUM(f.total_points), 0) as total_points,
                COUNT(DISTINCT f.player_id) as player_count
            FROM teams t
            LEFT JOIN fact_player_gameweeks f ON t.id = f.team_id
            GROUP BY t.id, t.name
            ORDER BY total_points DESC
        """
        
        team_results = execute_query(team_query)
        
        by_team = []
        for row in team_results:
            by_team.append(TeamDistribution(
                team_name=row["team_name"],
                team_id=row["team_id"],
                total_goals=row["total_goals"] or 0,
                total_points=row["total_points"] or 0,
                player_count=row["player_count"] or 0,
            ))
        
        # Distribution by position
        position_query = """
            SELECT 
                pos.singular_name as position,
                pos.id as position_id,
                COUNT(DISTINCT p.id) as player_count,
                COALESCE(SUM(f.goals_scored), 0) as total_goals,
                COALESCE(AVG(f.total_points), 0) as avg_points
            FROM positions pos
            LEFT JOIN players p ON pos.id = p.element_type
            LEFT JOIN fact_player_gameweeks f ON p.id = f.player_id
            GROUP BY pos.id, pos.singular_name
            ORDER BY pos.id
        """
        
        position_results = execute_query(position_query)
        
        by_position = []
        for row in position_results:
            by_position.append(PositionDistribution(
                position=row["position"] or "Unknown",
                position_id=row["position_id"],
                player_count=row["player_count"] or 0,
                total_goals=row["total_goals"] or 0,
                avg_points=round(float(row["avg_points"] or 0), 2),
            ))
        
        return DistributionsResponse(by_team=by_team, by_position=by_position)
    
    except Exception as e:
        logger.error(f"Error fetching distributions: {e}")
        return DistributionsResponse(by_team=[], by_position=[])


def get_top_players(limit: int = 10) -> List[TopPlayer]:
    """
    Get top performing players by total points.
    """
    try:
        query = """
            SELECT 
                f.player_id,
                f.player_name,
                t.name as team_name,
                SUM(f.total_points) as total_points,
                SUM(f.goals_scored) as total_goals,
                SUM(f.assists) as total_assists
            FROM fact_player_gameweeks f
            LEFT JOIN teams t ON f.team_id = t.id
            GROUP BY f.player_id, f.player_name, t.name
            ORDER BY total_points DESC
            LIMIT %s
        """
        
        results = execute_query(query, (limit,))
        
        players = []
        for row in results:
            players.append(TopPlayer(
                player_id=row["player_id"],
                player_name=row["player_name"] or "Unknown",
                team_name=row["team_name"] or "Unknown",
                total_points=row["total_points"] or 0,
                total_goals=row["total_goals"] or 0,
                total_assists=row["total_assists"] or 0,
            ))
        
        return players
    
    except Exception as e:
        logger.error(f"Error fetching top players: {e}")
        return []


def get_available_filters() -> DashboardFilters:
    """
    Get available filter options for the dashboard.
    """
    try:
        # Get teams
        teams_result = execute_query("SELECT id, name, short_name FROM teams ORDER BY name")
        teams = [{"id": t["id"], "name": t["name"], "short_name": t.get("short_name", "")} for t in teams_result]
        
        # Get gameweeks
        gw_result = execute_query("SELECT DISTINCT event FROM fact_player_gameweeks ORDER BY event")
        gameweeks = [r["event"] for r in gw_result]
        
        return DashboardFilters(teams=teams, gameweeks=gameweeks)
    
    except Exception as e:
        logger.error(f"Error fetching filters: {e}")
        return DashboardFilters()
