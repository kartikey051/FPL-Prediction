from app.db.session import execute_query
import sys

def verify_data():
    try:
        # Check team metrics seasons
        q1 = "SELECT season, COUNT(*) as count FROM understat_team_metrics GROUP BY season ORDER BY season"
        rows1 = execute_query(q1)
        print("Understat Team Metrics by Season:")
        for r in rows1:
            print(f"Season {r['season']}: {r['count']} rows")
            
        # Check roster metrics (no season column, but limit check)
        q2 = "SELECT COUNT(*) as count FROM understat_roster_metrics"
        rows2 = execute_query(q2)
        print(f"\nUnderstat Roster Metrics Total Rows: {rows2[0]['count']}")
        
        # Check Columns
        q3 = "SHOW COLUMNS FROM understat_team_metrics"
        cols = execute_query(q3)
        print("\nUnderstat Team Metrics Columns:")
        print([c['Field'] for c in cols])

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_data()
