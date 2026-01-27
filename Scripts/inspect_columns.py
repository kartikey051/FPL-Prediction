
from Utils.db import execute_query
import sys

try:
    cols = execute_query("SHOW COLUMNS FROM understat_team_metrics")
    with open("schema_info.txt", "w") as f:
        for c in cols:
            f.write(f"{c['Field']}\n")
    print("Schema written to schema_info.txt")
except Exception as e:
    print(f"Error: {e}")
