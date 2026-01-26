"""
Dashboard router - Anti-Gravity API layer.
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, Query, Body

from app.api.deps import get_current_user
from app.api.dashboard.schemas import (
    SummaryStats, TrendsResponse, DistributionsResponse,
    TopPlayer, DashboardFilters, TeamSquadResponse,
    StandingsResponse, PlayerTrendsResponse, GlobalSearchFilters
)
from app.api.dashboard.service import (
    get_summary_stats, get_gameweek_trends, get_distributions,
    get_top_players, get_available_filters, get_team_squad,
    get_league_standings, get_player_trends, get_global_players
)

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])

@router.get("/summary", response_model=SummaryStats)
async def dashboard_summary(
    team_id: Optional[int] = Query(None),
    season: str = Query("2024-25"),
    current_user: dict = Depends(get_current_user),
):
    return get_summary_stats(team_id=team_id, season=season)

@router.get("/trends", response_model=TrendsResponse)
async def dashboard_trends(
    team_id: Optional[int] = Query(None),
    season: str = Query("2024-25"),
    current_user: dict = Depends(get_current_user),
):
    return get_gameweek_trends(team_id=team_id, season=season)

@router.get("/distributions", response_model=DistributionsResponse)
async def dashboard_distributions(
    season: str = Query("2024-25"),
    current_user: dict = Depends(get_current_user),
):
    return get_distributions(season=season)

@router.get("/top-players", response_model=List[TopPlayer])
async def dashboard_top_players(
    limit: int = Query(10),
    season: str = Query("2024-25"),
    current_user: dict = Depends(get_current_user),
):
    return get_top_players(limit=limit, season=season)

@router.post("/search/players", response_model=List[TopPlayer])
async def search_players(
    filters: GlobalSearchFilters = Body(...),
    current_user: dict = Depends(get_current_user),
):
    """Global player discovery decoupled from team selection."""
    return get_global_players(filters)

@router.get("/filters", response_model=DashboardFilters)
async def dashboard_filters(
    current_user: dict = Depends(get_current_user),
):
    return get_available_filters()

@router.get("/teams/{team_id}/squad", response_model=TeamSquadResponse)
async def dashboard_team_squad(
    team_id: int,
    season: str = Query("2024-25"),
    current_user: dict = Depends(get_current_user),
):
    return get_team_squad(team_id=team_id, season=season)

@router.get("/standings", response_model=StandingsResponse)
async def dashboard_standings(
    season: str = Query("2024-25"),
    current_user: dict = Depends(get_current_user),
):
    return get_league_standings(season=season)

@router.get("/players/{player_id}/trends", response_model=PlayerTrendsResponse)
async def dashboard_player_trends(
    player_id: int,
    season: str = Query("2024-25"),
    current_user: dict = Depends(get_current_user),
):
    return get_player_trends(player_id=player_id, season=season)
