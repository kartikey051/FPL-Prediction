import sys
import os
sys.path.append(os.getcwd())
from app.db.session import execute_query

def inspect():
    print("--- FPL Season Players ---")
    try:
        rows = execute_query("SELECT * FROM fpl_season_players LIMIT 3")
        if rows:
            print(f"Columns: {list(rows[0].keys())}")
            print(f"Sample: {rows[0]}")
            # Check distinct seasons
            seasons = execute_query("SELECT DISTINCT season FROM fpl_season_players")
            print(f"Seasons available: {[r['season'] for r in seasons]}")
        else:
            print("Table empty")
    except Exception as e:
        print(f"Error: {e}")

    print("\n--- Understat Roster ---")
    try:
        rows = execute_query("SELECT player, xG, xA, time FROM understat_roster_metrics LIMIT 3")
        if rows:
            print(f"Sample: {rows}")
            # Check sum
            total = execute_query("SELECT SUM(xG) as tot FROM understat_roster_metrics")
            print(f"Total xG in DB: {total[0]['tot']}")
    except Exception as e:
        print(f"Error: {e}")

    print("\n--- Understat Teams ---")
    try:
        rows = execute_query("SELECT season, team_h, h_xg FROM understat_team_metrics LIMIT 3")
        if rows:
            print(f"Sample: {rows}")
            seasons = execute_query("SELECT DISTINCT season FROM understat_team_metrics")
            print(f"Seasons available: {[r['season'] for r in seasons]}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect()
