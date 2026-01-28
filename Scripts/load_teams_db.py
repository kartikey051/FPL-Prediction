import pandas as pd
from sqlalchemy import create_engine

# 1. DATABASE CONFIGURATION
# UPDATE THESE WITH YOUR LOCAL CREDENTIALS
DB_CONFIG = {
    "user": "root",           # Your MySQL Username
    "password": "Tarun123!",   # Your MySQL Password
    "host": "localhost",      # Usually 'localhost' or '127.0.0.1'
    "port": "3306",           # Default MySQL port
    "database": "FPL"         # The name of your FPL database
}

# 2. LOADING FUNCTION
def load_teams_from_mysql():
    # Create Connection Engine
    # Format: mysql+pymysql://user:password@host:port/database
    conn_str = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    try:
        engine = create_engine(conn_str)
        print("Connected to MySQL successfully.\n")
    except Exception as e:
        print(f"Connection Failed: {e}")
        return

    tables_to_load = [
        "clean_team_season_metrics",
        "clean_team_season_stats",
        "clean_team_xg_season"
    ]

    for table in tables_to_load:
        print(f"Querying table: {table} ...", end=" ")
        try:
            # Read directly into Pandas
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, engine)
            
            # Save to CSV for your other pipeline
            csv_name = f"{table}.csv"
            df.to_csv(csv_name, index=False)
            
            print(f"Success!")
            print(f"   Shape: {df.shape}")
            print(f"   Saved as: {csv_name}")
            
        except Exception as e:
            print(f"Failed: {e}")

    print("\nAll tables loaded.")

# 3. EXECUTION
if __name__ == "__main__":
    load_teams_from_mysql()