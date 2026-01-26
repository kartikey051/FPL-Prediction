from Utils.db import execute_query
import json

def inspect_schemas():
    tables = [
        "players", "teams", "fact_player_gameweeks", "fixtures",
        "fpl_season_players", "fpl_season_teams", "fpl_player_gameweeks", "fpl_fixtures"
    ]
    schemas = {}
    for table in tables:
        try:
            res = execute_query(f"DESCRIBE {table}")
            schemas[table] = [row['Field'] for row in res]
        except Exception as e:
            schemas[table] = f"Error: {str(e)}"
    
    with open("schema_dump.json", "w") as f:
        json.dump(schemas, f, indent=4)
        
    print("Schema dump saved to schema_dump.json")

if __name__ == "__main__":
    inspect_schemas()
