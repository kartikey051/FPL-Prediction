"""
Dashboard router - API endpoints for dashboard data.
"""

from typing import Optional, List

from fastapi import APIRouter, Depends, Query

from app.api.deps import get_current_user
from app.api.dashboard.schemas import (
    SummaryStats,
    TrendsResponse,
    DistributionsResponse,
    TopPlayer,
    DashboardFilters,
)
from app.api.dashboard.service import (
    get_summary_stats,
    get_gameweek_trends,
    get_distributions,
    get_top_players,
    get_available_filters,
)

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


@router.get("/summary", response_model=SummaryStats)
async def dashboard_summary(
    team_id: Optional[int] = Query(None, description="Filter by team ID"),
    current_user: dict = Depends(get_current_user),
):
    """
    Get dashboard summary statistics.
    
    Returns key metrics:
    - Total players, teams, fixtures
    - Average points per player
    - Total goals and assists
    
    Optionally filter by team.
    """
    return get_summary_stats(team_id=team_id)


@router.get("/trends", response_model=TrendsResponse)
async def dashboard_trends(
    team_id: Optional[int] = Query(None, description="Filter by team ID"),
    current_user: dict = Depends(get_current_user),
):
    """
    Get gameweek trends data.
    
    Returns aggregated stats per gameweek:
    - Total points
    - Goals scored
    - Assists
    - Average minutes
    
    Use for line/area charts.
    """
    return get_gameweek_trends(team_id=team_id)


@router.get("/distributions", response_model=DistributionsResponse)
async def dashboard_distributions(
    team_id: Optional[int] = Query(None, description="Filter by team ID"),
    current_user: dict = Depends(get_current_user),
):
    """
    Get distribution data for charts.
    
    Returns:
    - Goals/points by team (for bar charts)
    - Player count by position (for pie charts)
    """
    return get_distributions(team_id=team_id)


@router.get("/top-players", response_model=List[TopPlayer])
async def dashboard_top_players(
    limit: int = Query(10, ge=1, le=50, description="Number of players to return"),
    current_user: dict = Depends(get_current_user),
):
    """
    Get top performing players by total points.
    
    Returns player rankings with goals and assists.
    """
    return get_top_players(limit=limit)


@router.get("/filters", response_model=DashboardFilters)
async def dashboard_filters(
    current_user: dict = Depends(get_current_user),
):
    """
    Get available filter options.
    
    Returns lists of:
    - Teams (id, name)
    - Available gameweeks
    """
    return get_available_filters()
