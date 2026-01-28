"""Debug script to verify Squad Analytics for 2023-24."""
import sys
from app.db.session import execute_query
from app.api.dashboard.service import get_team_squad
from app.api.dashboard.season_config import get_season_schema

SEASON = "2023-24"
TEAM_NAME = "Liverpool"

print(f"--- Debugging Squad Analytics for {SEASON} ({TEAM_NAME}) ---")

# 1. Get correct Team ID for the season
schema = get_season_schema(SEASON)
print(f"Using table: {schema.table_teams}")

query = f"""
    SELECT {schema.col_team_id} as id, {schema.col_team_name} as name 
    FROM {schema.table_teams} 
    WHERE {schema.col_team_name} = %s AND season = %s
"""
# Note: table might use 'team_name' or 'name' depending on schema, verify from schema object
# But schema.col_team_name handles that abstraction

rows = execute_query(query, (TEAM_NAME, SEASON))
if not rows:
    print(f"ERROR: Could not find team '{TEAM_NAME}' in season '{SEASON}'")
    # Try listing all teams to see what's there
    all_teams = execute_query(f"SELECT {schema.col_team_name} as name FROM {schema.table_teams} WHERE season = '{SEASON}' LIMIT 5")
    print(f"Sample teams found: {[r['name'] for r in all_teams]}")
    sys.exit(1)

team_id = rows[0]["id"]
print(f"Found Team ID: {team_id}")

# 2. Fetch Squad using the service
print(f"Fetching squad for Team ID {team_id}...")
squad_response = get_team_squad(team_id, SEASON)

print(f"Squad Name: {squad_response.team_name}")
print(f"Player Count: {len(squad_response.players)}")

if squad_response.players:
    print("\nTop 5 Players:")
    for p in squad_response.players[:5]:
        print(f"  - {p.name} ({p.position}): Pts={p.total_points}, xG={p.xG}, xA={p.xA}")
else:
    print("ERROR: Squad is empty!")
