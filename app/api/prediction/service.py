"""
Prediction Service - Player Points Prediction Using FPL Data

This service provides player predictions using:
1. Rolling averages of recent performance (form)
2. Points per 90 minutes efficiency
3. Value metrics (points per million)

The predictions are calculated directly from database data,
making it robust and not dependent on external pickle files.
"""

from typing import List, Optional, Dict, Any
from datetime import date, datetime
from app.db.session import execute_query, execute_write
from app.api.prediction.schemas import (
    PredictedPlayer, PredictionFilters, PlayerPredictionDetail
)
from app.api.dashboard.season_config import (
    get_season_schema, CURRENT_SEASON, get_player_team_column
)
from Utils.logging_config import get_logger

logger = get_logger("prediction_service")

# Position mapping
POSITION_MAP = {1: 'GKP', 2: 'DEF', 3: 'MID', 4: 'FWD'}
POSITION_REVERSE = {'GKP': 1, 'DEF': 2, 'MID': 3, 'FWD': 4}


def ensure_predictions_table():
    """Create predictions table if it doesn't exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS player_predicted_points (
        id INT AUTO_INCREMENT PRIMARY KEY,
        player_id INT NOT NULL,
        player_name VARCHAR(100),
        team_id INT,
        team_name VARCHAR(100),
        position VARCHAR(10),
        now_cost DECIMAL(5,1),
        predicted_points DECIMAL(6,2),
        total_points INT,
        form DECIMAL(5,2),
        minutes INT,
        goals INT,
        assists INT,
        points_per_million DECIMAL(6,2),
        prediction_date DATE,
        season VARCHAR(10),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_player_date (player_id, prediction_date)
    )
    """
    try:
        execute_query(create_sql, fetch=False)
        logger.info("Predictions table ensured")
    except Exception as e:
        logger.error(f"Failed to create predictions table: {e}")


def calculate_predicted_points(
    total_points: int,
    minutes: int,
    form: float,
    goals: int,
    assists: int,
    position: str,
    games_played: int
) -> float:
    """
    Calculate predicted points for next gameweek using a weighted formula.
    
    Factors:
    - Form (recent performance) - 40%
    - Points per 90 efficiency - 30%
    - Season average - 20%
    - Position bonus adjustments - 10%
    
    This is a simplified prediction model that works without ML dependencies.
    """
    if games_played == 0 or minutes < 90:
        # Not enough data - use form or minimal prediction
        return max(form * 1.0, 2.0)
    
    # Points per 90 minutes
    pts_per_90 = (total_points / (minutes / 90)) if minutes > 0 else 0
    
    # Season average per game
    avg_per_game = total_points / games_played if games_played > 0 else 0
    
    # Position adjustments (attackers typically score more points)
    position_multiplier = {
        'GKP': 0.9,
        'DEF': 0.95,
        'MID': 1.0,
        'FWD': 1.05
    }.get(position, 1.0)
    
    # Weighted prediction
    predicted = (
        (form * 0.40) +                    # Recent form heavily weighted
        (pts_per_90 * 0.30) +              # Efficiency
        (avg_per_game * 0.20) +            # Season consistency
        (((goals * 0.5) + (assists * 0.3)) * 0.10)  # Goal threat bonus
    ) * position_multiplier
    
    # Ensure reasonable bounds (2-15 points typical range)
    return round(max(2.0, min(predicted, 15.0)), 2)


def get_player_stats_for_prediction(
    season: str = CURRENT_SEASON,
    min_minutes: int = 300
) -> List[Dict[str, Any]]:
    """
    Fetch player statistics needed for prediction.
    Uses current season data with form calculations.
    """
    schema = get_season_schema(season)
    
    # For current season, use live tables
    if not schema.is_historical:
        query = """
            SELECT 
                p.id as player_id,
                CONCAT(p.first_name, ' ', p.second_name) as player_name,
                t.name as team_name,
                p.team as team_id,
                p.element_type as position_id,
                CASE p.element_type 
                    WHEN 1 THEN 'GKP' WHEN 2 THEN 'DEF' 
                    WHEN 3 THEN 'MID' WHEN 4 THEN 'FWD' 
                    ELSE 'UNK' 
                END as position,
                p.now_cost / 10.0 as now_cost,
                p.total_points,
                CAST(p.form as DECIMAL(5,2)) as form,
                COALESCE(SUM(f.minutes), 0) as minutes,
                COALESCE(SUM(f.goals_scored), 0) as goals,
                COALESCE(SUM(f.assists), 0) as assists,
                COUNT(DISTINCT f.event) as games_played
            FROM players p
            LEFT JOIN teams t ON p.team = t.id
            LEFT JOIN fact_player_gameweeks f ON p.id = f.player_id
            GROUP BY p.id, p.first_name, p.second_name, t.name, p.team, 
                     p.element_type, p.now_cost, p.total_points, p.form
            HAVING minutes >= %s
            ORDER BY p.total_points DESC
        """
    else:
        # Historical season query
        query = f"""
            SELECT 
                p.element_id as player_id,
                CONCAT(p.first_name, ' ', p.second_name) as player_name,
                t.team_name as team_name,
                p.team_id,
                p.element_type as position_id,
                CASE p.element_type 
                    WHEN 1 THEN 'GKP' WHEN 2 THEN 'DEF' 
                    WHEN 3 THEN 'MID' WHEN 4 THEN 'FWD' 
                    ELSE 'UNK' 
                END as position,
                p.now_cost / 10.0 as now_cost,
                p.total_points,
                CAST(COALESCE(p.form, 0) as DECIMAL(5,2)) as form,
                COALESCE(SUM(f.minutes), 0) as minutes,
                COALESCE(SUM(f.goals_scored), 0) as goals,
                COALESCE(SUM(f.assists), 0) as assists,
                COUNT(DISTINCT f.gameweek) as games_played
            FROM fpl_season_players p
            LEFT JOIN fpl_season_teams t ON p.team_id = t.team_id AND t.season = '{season}'
            LEFT JOIN fpl_player_gameweeks f ON p.element_id = f.element_id AND f.season = '{season}'
            WHERE p.season = '{season}'
            GROUP BY p.element_id, p.first_name, p.second_name, t.team_name, p.team_id, 
                     p.element_type, p.now_cost, p.total_points, p.form
            HAVING minutes >= %s
            ORDER BY p.total_points DESC
        """
    
    try:
        results = execute_query(query, (min_minutes,))
        return results
    except Exception as e:
        logger.error(f"Failed to fetch player stats: {e}")
        return []


def generate_predictions(season: str = CURRENT_SEASON, min_minutes: int = 300) -> int:
    """
    Generate and store predictions for all qualifying players.
    Returns number of players processed.
    """
    ensure_predictions_table()
    
    players = get_player_stats_for_prediction(season, min_minutes)
    if not players:
        logger.warning("No players found for prediction")
        return 0
    
    today = date.today().isoformat()
    count = 0
    
    for p in players:
        try:
            predicted_pts = calculate_predicted_points(
                total_points=int(p['total_points'] or 0),
                minutes=int(p['minutes'] or 0),
                form=float(p['form'] or 0),
                goals=int(p['goals'] or 0),
                assists=int(p['assists'] or 0),
                position=p['position'],
                games_played=int(p['games_played'] or 0)
            )
            
            now_cost = float(p['now_cost'] or 0)
            pts_per_million = round(predicted_pts / now_cost, 2) if now_cost > 0 else 0
            
            # Upsert prediction
            upsert_sql = """
                INSERT INTO player_predicted_points 
                (player_id, player_name, team_id, team_name, position, now_cost,
                 predicted_points, total_points, form, minutes, goals, assists,
                 points_per_million, prediction_date, season)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    player_name = VALUES(player_name),
                    team_id = VALUES(team_id),
                    team_name = VALUES(team_name),
                    position = VALUES(position),
                    now_cost = VALUES(now_cost),
                    predicted_points = VALUES(predicted_points),
                    total_points = VALUES(total_points),
                    form = VALUES(form),
                    minutes = VALUES(minutes),
                    goals = VALUES(goals),
                    assists = VALUES(assists),
                    points_per_million = VALUES(points_per_million),
                    season = VALUES(season)
            """
            
            execute_write(upsert_sql, (
                p['player_id'],
                p['player_name'],
                p.get('team_id'),
                p['team_name'],
                p['position'],
                now_cost,
                predicted_pts,
                int(p['total_points'] or 0),
                float(p['form'] or 0),
                int(p['minutes'] or 0),
                int(p['goals'] or 0),
                int(p['assists'] or 0),
                pts_per_million,
                today,
                season
            ))
            count += 1
            
        except Exception as e:
            logger.error(f"Failed to process player {p.get('player_id')}: {e}")
            continue
    
    logger.info(f"Generated predictions for {count} players")
    return count


def get_best_players(filters: PredictionFilters) -> List[PredictedPlayer]:
    """
    Get best predicted players based on filters.
    Fetches from stored predictions or generates on-the-fly.
    """
    ensure_predictions_table()
    
    # Check if we have recent predictions
    today = date.today().isoformat()
    check_query = """
        SELECT COUNT(*) as cnt FROM player_predicted_points 
        WHERE prediction_date = %s AND season = %s
    """
    result = execute_query(check_query, (today, filters.season))
    
    if not result or result[0]['cnt'] == 0:
        # Generate predictions if none exist for today
        logger.info("No predictions for today, generating...")
        generate_predictions(filters.season, filters.min_minutes or 300)
    
    # Build query with filters
    conditions = ["prediction_date = %s", "season = %s"]
    params = [today, filters.season]
    
    if filters.position:
        conditions.append("position = %s")
        params.append(filters.position.upper())
    
    if filters.team_id:
        conditions.append("team_id = %s")
        params.append(filters.team_id)
    
    if filters.max_price:
        conditions.append("now_cost <= %s")
        params.append(filters.max_price)
    
    if filters.min_budget:
        conditions.append("now_cost >= %s")
        params.append(filters.min_budget)
    
    if filters.min_minutes:
        conditions.append("minutes >= %s")
        params.append(filters.min_minutes)
    
    where_clause = " AND ".join(conditions)
    
    query = f"""
        SELECT 
            player_id, player_name, team_name, position, now_cost,
            predicted_points, total_points, form, minutes, goals, assists,
            points_per_million
        FROM player_predicted_points
        WHERE {where_clause}
        ORDER BY predicted_points DESC
        LIMIT %s
    """
    params.append(filters.limit)
    
    try:
        results = execute_query(query, tuple(params))
        
        players = []
        for r in results:
            # Determine confidence based on minutes and form consistency
            minutes = int(r['minutes'] or 0)
            form = float(r['form'] or 0)
            
            if minutes >= 1500 and form >= 5.0:
                confidence = "HIGH"
            elif minutes >= 900 and form >= 3.0:
                confidence = "MEDIUM"
            else:
                confidence = "LOW"
            
            players.append(PredictedPlayer(
                player_id=r['player_id'],
                player_name=r['player_name'],
                team_name=r['team_name'] or 'Unknown',
                position=r['position'],
                now_cost=float(r['now_cost']),
                predicted_points=float(r['predicted_points']),
                total_points=int(r['total_points'] or 0),
                form=float(r['form'] or 0),
                minutes=minutes,
                goals=int(r['goals'] or 0),
                assists=int(r['assists'] or 0),
                points_per_million=float(r['points_per_million'] or 0),
                confidence=confidence
            ))
        
        return players
        
    except Exception as e:
        logger.error(f"Failed to get best players: {e}")
        return []


def get_player_prediction(player_id: int, season: str = CURRENT_SEASON) -> Optional[PlayerPredictionDetail]:
    """Get detailed prediction for a specific player."""
    today = date.today().isoformat()
    
    query = """
        SELECT 
            player_id, player_name, team_name, position, now_cost,
            predicted_points, total_points, form, minutes, goals, assists
        FROM player_predicted_points
        WHERE player_id = %s AND prediction_date = %s AND season = %s
    """
    
    try:
        results = execute_query(query, (player_id, today, season))
        
        if not results:
            return None
        
        r = results[0]
        minutes = int(r['minutes'] or 0)
        pts_per_90 = (int(r['total_points'] or 0) / (minutes / 90)) if minutes > 0 else 0
        
        return PlayerPredictionDetail(
            player_id=r['player_id'],
            player_name=r['player_name'],
            team_name=r['team_name'] or 'Unknown',
            position=r['position'],
            now_cost=float(r['now_cost']),
            predicted_points=float(r['predicted_points']),
            total_points=int(r['total_points'] or 0),
            form=float(r['form'] or 0),
            points_per_90=round(pts_per_90, 2),
            goals=int(r['goals'] or 0),
            assists=int(r['assists'] or 0),
            minutes=minutes
        )
        
    except Exception as e:
        logger.error(f"Failed to get player prediction: {e}")
        return None


def get_budget_optimized_squad(
    max_budget: float = 100.0,
    formation: str = "3-4-3"
) -> Dict[str, Any]:
    """
    Get an optimized squad within budget constraints.
    
    Args:
        max_budget: Total budget in millions (default 100.0)
        formation: Formation like "3-4-3", "4-3-3", "4-4-2"
    
    Returns:
        Dictionary with selected players by position and total metrics.
    """
    # Parse formation
    parts = formation.split("-")
    if len(parts) != 3:
        parts = [3, 4, 3]  # Default
    
    required = {
        'GKP': 1,
        'DEF': int(parts[0]),
        'MID': int(parts[1]),
        'FWD': int(parts[2])
    }
    
    today = date.today().isoformat()
    squad = {'GKP': [], 'DEF': [], 'MID': [], 'FWD': []}
    total_cost = 0.0
    total_predicted = 0.0
    
    # Greedy selection by position (prioritize by points_per_million for value)
    for position, count in required.items():
        remaining_budget = max_budget - total_cost
        max_per_player = remaining_budget / max(count, 1)
        
        query = """
            SELECT 
                player_id, player_name, team_name, position, now_cost,
                predicted_points, points_per_million
            FROM player_predicted_points
            WHERE position = %s 
              AND prediction_date = %s 
              AND now_cost <= %s
            ORDER BY points_per_million DESC, predicted_points DESC
            LIMIT %s
        """
        
        try:
            results = execute_query(query, (position, today, max_per_player * 1.5, count * 3))
            
            selected = 0
            for r in results:
                if selected >= count:
                    break
                cost = float(r['now_cost'])
                if total_cost + cost <= max_budget:
                    squad[position].append({
                        'player_id': r['player_id'],
                        'player_name': r['player_name'],
                        'team_name': r['team_name'],
                        'now_cost': cost,
                        'predicted_points': float(r['predicted_points'])
                    })
                    total_cost += cost
                    total_predicted += float(r['predicted_points'])
                    selected += 1
                    
        except Exception as e:
            logger.error(f"Failed to select {position}: {e}")
    
    return {
        'squad': squad,
        'formation': formation,
        'total_cost': round(total_cost, 1),
        'budget_remaining': round(max_budget - total_cost, 1),
        'total_predicted_points': round(total_predicted, 2),
        'player_count': sum(len(v) for v in squad.values())
    }
