
from Utils.db import execute_query
from Utils.logging_config import get_logger

logger = get_logger("inspect")

def inspect():
    try:
        # Check columns of understat_roster_metrics
        cols = execute_query("SHOW COLUMNS FROM understat_roster_metrics")
        print("Columns in understat_roster_metrics:")
        for c in cols:
            print(f"- {c['Field']} ({c['Type']})")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect()
