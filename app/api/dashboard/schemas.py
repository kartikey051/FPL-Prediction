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


class DashboardFilters(BaseModel):
    """Available filters for dashboard queries."""
    seasons: List[str] = []
    teams: List[Dict[str, Any]] = []
    gameweeks: List[int] = []
