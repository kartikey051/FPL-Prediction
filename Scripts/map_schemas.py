from Utils.db import execute_query
import json

def get_schema(table_name):
    try:
        cols = execute_query(f"DESCRIBE {table_name}")
        return [{c['Field']: c['Type']} for c in cols]
    except Exception as e:
        return str(e)

def main():
    tables = [
        "players", "teams", "fact_player_gameweeks", "fixtures",
        "fpl_season_players", "fpl_season_teams", "fpl_player_gameweeks", "fpl_fixtures",
        "understat_teams", "understat_roster", "events_raw"
    ]
    
    # Also check for any other likely understat tables
    all_tables_query = execute_query("SHOW TABLES")
    all_tables = [list(t.values())[0] for t in all_tables_query]
    
    schemas = {}
    for table in all_tables:
        if table in tables or "understat" in table or "fpl" in table:
            schemas[table] = get_schema(table)
            
    with open("full_schema_map.json", "w") as f:
        json.dump(schemas, f, indent=4)
    print(f"Schema for {len(schemas)} tables dumped to full_schema_map.json")

if __name__ == "__main__":
    main()
