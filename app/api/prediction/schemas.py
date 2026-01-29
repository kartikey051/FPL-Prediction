"""
Pydantic schemas for prediction endpoints.
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date


class PredictionFilters(BaseModel):
    """Filters for player prediction requests."""
    max_budget: Optional[float] = Field(None, description="Maximum total budget in millions (e.g., 100.0)")
    min_budget: Optional[float] = Field(None, description="Minimum player price")
    max_price: Optional[float] = Field(None, description="Maximum individual player price")
    position: Optional[str] = Field(None, description="Position filter: GKP, DEF, MID, FWD")
    team_id: Optional[int] = Field(None, description="Filter by team ID")
    min_minutes: Optional[int] = Field(300, description="Minimum minutes played for consideration")
    limit: int = Field(15, description="Number of players to return", ge=1, le=50)
    season: str = Field("2024-25", description="Season to predict for")


class PredictedPlayer(BaseModel):
    """Individual player prediction response."""
    player_id: int
    player_name: str
    team_name: str
    position: str
    now_cost: float  # Price in millions
    predicted_points: float
    total_points: int  # Actual points so far
    form: float  # Recent form (pts/game last 5)
    minutes: int
    goals: int
    assists: int
    points_per_million: float  # Value metric
    confidence: Optional[str] = None  # HIGH, MEDIUM, LOW


class PredictionResponse(BaseModel):
    """Response for best players prediction."""
    players: List[PredictedPlayer]
    total_budget_used: float
    filters_applied: dict
    prediction_date: str
    model_info: str


class RefreshResponse(BaseModel):
    """Response for prediction refresh."""
    success: bool
    players_updated: int
    message: str


class PlayerPredictionDetail(BaseModel):
    """Detailed prediction for a single player."""
    player_id: int
    player_name: str
    team_name: str
    position: str
    now_cost: float
    predicted_points: float
    total_points: int
    form: float
    points_per_90: float
    goals: int
    assists: int
    minutes: int
    # Historical form data
    last_5_points: List[int] = []
    prediction_breakdown: Optional[dict] = None
