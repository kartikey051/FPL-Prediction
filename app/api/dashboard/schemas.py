"""
Pydantic schemas for dashboard endpoints.
"""

from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime


class SummaryStats(BaseModel):
    """Key performance indicators for the dashboard."""
    total_players: int
    total_teams: int
    total_fixtures: int
    total_gameweeks: int
    avg_points_per_player: float
    total_goals: int
    total_assists: int
    avg_player_value: float


class TrendDataPoint(BaseModel):
    """Single data point for trend charts."""
    gameweek: int
    total_points: float
    total_goals: int
    total_assists: int
    avg_minutes: float
    
    # Understat additions
    total_xG: Optional[float] = None
    total_xA: Optional[float] = None


class TrendsResponse(BaseModel):
    """Response for gameweek trends."""
    data: List[TrendDataPoint]
    seasons: List[str] = []


class TeamDistribution(BaseModel):
    """Goals/points distribution by team."""
    team_name: str
    team_id: int
    total_goals: int
    total_points: int
    player_count: int


class PositionDistribution(BaseModel):
    """Player count and stats by position."""
    position: str
    position_id: int
    player_count: int
    total_goals: int
    avg_points: float


class DistributionsResponse(BaseModel):
    """Response for distribution charts."""
    by_team: List[TeamDistribution]
    by_position: List[PositionDistribution]


class TopPlayer(BaseModel):
    """Top performing player data."""
    player_id: int
    player_name: str
    team_name: str
    total_points: int
    total_goals: int
    total_assists: int
    xG: Optional[float] = None
    xA: Optional[float] = None


class DashboardFilters(BaseModel):
    """Available filters for dashboard queries."""
    seasons: List[str] = []
    teams: List[Dict[str, Any]] = []
    gameweeks: List[int] = []


# --- Advanced Analytics Schemas ---

class SquadMember(BaseModel):
    """Individual player summary for team view."""
    player_id: int
    name: str
    position: str
    total_points: int
    goals: int
    assists: int
    minutes: int
    now_cost: float
    form: float  # Avg pts last 5
    pts_per_90: float
    consistency: float  # std dev or % reliability
    
    # Understat analytics
    xG: Optional[float] = None
    xA: Optional[float] = None
    xG_per_90: Optional[float] = None
    xA_per_90: Optional[float] = None


class TeamSquadResponse(BaseModel):
    """Full squad details for a team."""
    team_name: str
    season: str
    players: List[SquadMember]


class StandingEntry(BaseModel):
    """League table entry."""
    rank: int
    team_name: str
    played: int
    wins: int
    draws: int
    losses: int
    goals_for: int
    goals_against: int
    goal_diff: int
    points: int
    clean_sheets: int
    
    # Understat team metrics
    xG_for: Optional[float] = None
    xG_against: Optional[float] = None
    xPts: Optional[float] = None


class StandingsResponse(BaseModel):
    """League table response."""
    season: str
    standings: List[StandingEntry]


class PlayerTrendPoint(BaseModel):
    """GW breakdown for a single player."""
    gameweek: int
    points: int
    minutes: int
    goals: int
    assists: int
    value: float
    opponent: str
    was_home: bool
    
    # Understat
    xG: Optional[float] = None
    xA: Optional[float] = None


class PlayerTrendsResponse(BaseModel):
    """Historical performance for a player."""
    player_id: int
    player_name: str
    team_name: str
    trend: List[PlayerTrendPoint]
    overall_form: float


class GlobalSearchFilters(BaseModel):
    """Filters for global player lookup."""
    name: Optional[str] = None
    team_id: Optional[int] = None
    position: Optional[str] = None
    season: str = "2024-25"
    min_points: Optional[int] = None
    min_minutes: Optional[int] = None
    sort_by: str = "total_points"  # total_points, form, xG, etc.
    order: str = "desc"
