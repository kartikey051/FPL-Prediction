"""
Prediction Router - API endpoints for player predictions and recommendations.
"""

from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from datetime import date

from app.api.deps import get_current_user
from app.api.prediction.schemas import (
    PredictionFilters, PredictionResponse, PredictedPlayer,
    RefreshResponse, PlayerPredictionDetail
)
from app.api.prediction.service import (
    get_best_players, generate_predictions, get_player_prediction,
    get_budget_optimized_squad
)

router = APIRouter(prefix="/prediction", tags=["Predictions"])


@router.get("/best-players", response_model=PredictionResponse)
async def get_best_player_predictions(
    max_budget: Optional[float] = Query(None, description="Maximum budget in millions"),
    max_price: Optional[float] = Query(None, description="Max price per player"),
    min_budget: Optional[float] = Query(None, description="Minimum player price"),
    position: Optional[str] = Query(None, description="Position: GKP, DEF, MID, FWD"),
    team_id: Optional[int] = Query(None, description="Filter by team ID"),
    min_minutes: int = Query(300, description="Minimum minutes played"),
    limit: int = Query(15, ge=1, le=50, description="Number of players"),
    season: str = Query("2024-25", description="Season"),
    current_user: dict = Depends(get_current_user),
):
    """
    Get best predicted players based on constraints.
    
    **Constraints:**
    - `max_budget`: Total budget constraint (optional)
    - `max_price`: Maximum price for individual player
    - `position`: Filter by position (GKP, DEF, MID, FWD)
    - `team_id`: Filter by specific team
    - `min_minutes`: Minimum minutes played (default 300)
    
    **Returns:**
    List of top predicted players sorted by predicted points.
    """
    filters = PredictionFilters(
        max_budget=max_budget,
        max_price=max_price,
        min_budget=min_budget,
        position=position,
        team_id=team_id,
        min_minutes=min_minutes,
        limit=limit,
        season=season
    )
    
    players = get_best_players(filters)
    
    total_budget = sum(p.now_cost for p in players)
    
    return PredictionResponse(
        players=players,
        total_budget_used=round(total_budget, 1),
        filters_applied={
            "max_budget": max_budget,
            "max_price": max_price,
            "position": position,
            "team_id": team_id,
            "min_minutes": min_minutes,
            "limit": limit
        },
        prediction_date=date.today().isoformat(),
        model_info="Weighted prediction model (form: 40%, efficiency: 30%, avg: 20%, goals: 10%)"
    )


@router.post("/refresh", response_model=RefreshResponse)
async def refresh_predictions(
    min_minutes: int = Query(300, description="Minimum minutes for inclusion"),
    season: str = Query("2024-25", description="Season to refresh"),
    current_user: dict = Depends(get_current_user),
):
    """
    Refresh predictions for all players.
    
    This regenerates predictions based on current player stats.
    Run this after new gameweek data is available.
    """
    try:
        count = generate_predictions(season=season, min_minutes=min_minutes)
        return RefreshResponse(
            success=True,
            players_updated=count,
            message=f"Successfully updated predictions for {count} players"
        )
    except Exception as e:
        return RefreshResponse(
            success=False,
            players_updated=0,
            message=f"Failed to refresh predictions: {str(e)}"
        )


@router.get("/player/{player_id}", response_model=PlayerPredictionDetail)
async def get_single_player_prediction(
    player_id: int,
    season: str = Query("2024-25"),
    current_user: dict = Depends(get_current_user),
):
    """
    Get detailed prediction for a specific player.
    """
    prediction = get_player_prediction(player_id, season)
    
    if not prediction:
        raise HTTPException(
            status_code=404,
            detail=f"No prediction found for player {player_id}"
        )
    
    return prediction


@router.get("/optimized-squad")
async def get_optimized_squad(
    max_budget: float = Query(100.0, description="Total budget in millions"),
    formation: str = Query("3-4-3", description="Formation (e.g., 3-4-3, 4-3-3, 4-4-2)"),
    current_user: dict = Depends(get_current_user),
):
    """
    Get an optimized starting XI within budget constraints.
    
    Uses a value-based greedy algorithm to select the best
    players per position that fit within the budget.
    
    **Parameters:**
    - `max_budget`: Total budget (default 100.0m)
    - `formation`: Desired formation (default 3-4-3)
    
    **Returns:**
    Selected squad with total cost and predicted points.
    """
    result = get_budget_optimized_squad(max_budget, formation)
    return result


@router.get("/positions/{position}", response_model=list[PredictedPlayer])
async def get_predictions_by_position(
    position: str,
    limit: int = Query(10, ge=1, le=30),
    max_price: Optional[float] = Query(None),
    season: str = Query("2024-25"),
    current_user: dict = Depends(get_current_user),
):
    """
    Get top predicted players for a specific position.
    
    **Positions:** GKP, DEF, MID, FWD
    """
    position = position.upper()
    if position not in ['GKP', 'DEF', 'MID', 'FWD']:
        raise HTTPException(
            status_code=400,
            detail="Invalid position. Use: GKP, DEF, MID, FWD"
        )
    
    filters = PredictionFilters(
        position=position,
        max_price=max_price,
        limit=limit,
        season=season
    )
    
    return get_best_players(filters)
