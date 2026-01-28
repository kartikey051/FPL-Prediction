 
# This file consists of python code to load data from the mysql database to 
# pandas dataframes and perform operations to clean the data before passing it on to the feature extraction.

 
# Installing Necessary packages

 
# Importing Libraries and Establishing connection with the database


import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import pickle
from pathlib import Path
from dotenv import load_dotenv

current_dir = Path(__file__).resolve().parent
dotenv_path = current_dir.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

 
# Establishing connection with the database


# Database connection parameters
user = os.getenv('FPL_DB_USER') 
password = os.getenv('FPL_DB_PASSWORD') 
database = os.getenv('FPL_DB_NAME') 
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')

# Creating connection
#   Format: mysql+mysqlconnector://user:password@host:port/database
connection_string = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'

# Create engine
engine = create_engine(connection_string)


 
# Importing MySQL Data to Python Dataframes


dfs={}

try:
    inspector = inspect(engine)
    # Get all table names from the database
    table_names = inspector.get_table_names()
    print(f"Found {len(table_names)} tables in the database.")
    print("==Tables in the database:==\n",
          table_names,
          "\n\nStarting to import...")

    for table in table_names:
        dfs[table] = pd.read_sql_table(table, engine)
        print(f"Table '{table}' imported with {len(dfs[table])} records.")
    
    print("All tables imported with data successfully.")

except Exception as e:
    print(f"Error occurred during import: {e}")
finally:
    engine.dispose()
    print("Database connection closed.")


# Show all tables in dfs
print("Tables loaded into dataframes:", list(dfs.keys()))

 
# Moving data to individual variables


fact_player_gameweeks_df = dfs['fact_player_gameweeks']
fact_player_gameweeks_df.head()




fpl_fixtures_df = dfs['fpl_fixtures']
fpl_fixtures_df.head()




fpl_season_players_df = dfs['fpl_season_players']
fpl_season_players_df.head()




fpl_season_teams_df = dfs['fpl_season_teams']
fpl_season_teams_df.head()



player_history_df = dfs.get('player_history')
player_history_df.head()


players_df = dfs.get('players')
players_df.head()


positions_df = dfs.get('positions')
positions_df.head()


teams_df = dfs.get('teams')
teams_df.head()



understat_roster_metrics_df = dfs['understat_roster_metrics']
understat_roster_metrics_df.head()




understat_team_metrics_df = dfs['understat_team_metrics']
understat_team_metrics_df.head()


# print("========\nfact_player_gameweeks_df\n",fact_player_gameweeks_df.info(),"\n\n")
# print("========\nfpl_fixtures_df\n",fpl_fixtures_df.info(),"\n\n")
# print("========\nfpl_season_players_df\n",fpl_season_players_df.info(),"\n\n")
# print("========\nfpl_season_teams_df\n",fpl_season_teams_df.info(),"\n\n")
# print("========\nplayer_history_df\n",player_history_df.info(),"\n\n")
# print("========\nplayers_df\n",players_df.info(),"\n\n")
# print("========\npositions_df\n",positions_df.info(),"\n\n")
# print("========\nteams_df\n",teams_df.info(),"\n\n")
# print("========\nunderstat_roster_metrics_df\n",understat_roster_metrics_df.info(),"\n\n")
# print("========\nunderstat_team_metrics_df\n",understat_team_metrics_df.info())

 
# # Cleaning Positions Data


# Renaming columns for better clarity
positions_df = positions_df.rename(columns={
    'id':'position_id',
    'singular_name':'position_name',
    'singular_name_short':'position_short_name',
    'sqaud_select':'squad_capacity', # Total allowed in 15-man squad
    'squad_min_play':'min_starting_size',
    'sqaud_max_play':'max_starting_size',
    'ui_shirt_specific':'is_gk_shirt'  # To be converted to boolean
})

# Type conversions
positions_df['is_gk_shirt'] = positions_df['is_gk_shirt'].astype(bool)


#sorted(positions_df.columns)
# Reordering
# positions_df = positions_df[[
#     'position_id', 'position_name', 'position_short_name', 
#     'squad_capacity', 'min_starting_size', 'max_starting_size', 
#     'element_count', 'is_gk_shirt'
# ]]

 
# # Cleaning fact_player_gameweeks_df
# A fact table.
# Each row in this table represents a unique event.


# Normalize values
fact_player_gameweeks_df['value'] = fact_player_gameweeks_df['value'] / 10.0

# Standardize column names across dataframes 
count_cols = [
    'event',
    'minutes',
    'total_points',
    'goals_scored', 
    'assists', 
    'clean_sheets', 
    'goals_conceded', 
    'yellow_cards', 
    'red_cards', 
    'own_goals', 
    'opponent_id'
]
fact_player_gameweeks_df[count_cols] = fact_player_gameweeks_df[count_cols].fillna(0).astype(int)

# Sorting by time
fact_player_gameweeks_df = fact_player_gameweeks_df.sort_values(by = ['player_id','event'])


# Encoding location
fact_player_gameweeks_df['is_home'] = fact_player_gameweeks_df['home_away'].apply(lambda x : 1 if x == 'H' else 0)

# Resetting index
#  After sorting the index becomes scrambled. It is reset to keep the dataframe clean
fact_player_gameweeks_df = fact_player_gameweeks_df.reset_index(drop=True)


 
# # Cleaning fpl_fixtures_df


# Converring kickoff_time to datetime
fpl_fixtures_df['kickoff_time'] = pd.to_datetime(fpl_fixtures_df['kickoff_time'])

# Handling missing values
#  For modeling we often fill missing scores with -1 to differentiate from a 0-0 draw
fpl_fixtures_df['team_h_score'] = fpl_fixtures_df['team_h_score'].fillna(-1).astype(int)
fpl_fixtures_df['team_a_score'] = fpl_fixtures_df['team_a_score'].fillna(-1).astype(int)

# Extracting time features
fpl_fixtures_df['kickoff_hour'] = fpl_fixtures_df['kickoff_time'].dt.hour
fpl_fixtures_df['kickoff_dayofweek'] = fpl_fixtures_df['kickoff_time'].dt.day_name()

# Creating a Match Result column
# We get the match result to the model
# H  -> Home Win, A -> Away Win, D -> Draw, U -> Unplayed
def get_match_result(row):
    if not row['finished']:
        return 'U' 
    if row['team_h_score'] > row['team_a_score']:
        return 'H' 
    elif row['team_h_score'] < row['team_a_score']:
        return 'A' 
    else: return 'D' 

fpl_fixtures_df['result'] = fpl_fixtures_df.apply(get_match_result, axis=1)

# Consistency check (sorting)
fpl_fixtures_df = fpl_fixtures_df.sort_values(by=['event','kickoff_time']).reset_index(drop=True)

 
# # Cleaning fpl_season_players_df


# Concatenating name
fpl_season_players_df['full_name'] = fpl_season_players_df['first_name'] + " " + fpl_season_players_df['second_name']

# Nomralizing price
fpl_season_players_df['now_cost'] = fpl_season_players_df['now_cost'] / 10.0

# Mapping positions
position_map = positions_df.set_index('position_id')['position_name'].to_dict()
fpl_season_players_df['position'] = fpl_season_players_df['element_type'].map(position_map)

# Extracting year
fpl_season_players_df['season_start'] = fpl_season_players_df['season'].str.split('-').str[0].astype(int)

# 
cols_to_keep = [
    'season', 
    'season_start', 
    'element_id', 
    'full_name', 
    'team_id', 
    'position', 
    'total_points', 
    'now_cost'
]

fpl_season_players_df = fpl_season_players_df[cols_to_keep]

# fpl_season_players_df.head()


 
# # Cleaning fpl_season_teams_df


# Renaming for clarity
fpl_season_teams_df = fpl_season_teams_df.rename(columns={
    'strength': 'fpl_difficulty_rating'
})

# Aligning Season format
# To get numeric year
fpl_season_teams_df['season_start'] = fpl_season_teams_df['season'].str.split('-').str[0].astype(int)

# Calculating Home/Away advantage
# Some teams are stroger when they are home than away
# Useful for a 'home_advantage_bias'. Some people perform better at their home stadium
fpl_season_teams_df['home_advantage_bias'] = fpl_season_teams_df['strength_overall_home'] - fpl_season_teams_df['strength_overall_away']

# Data type conversion
strength_cols = [col for col in fpl_season_teams_df.columns if 'strength' in col]
fpl_season_teams_df[strength_cols] = fpl_season_teams_df[strength_cols].astype(int)

fpl_season_teams_df.head()

 
# # Cleaning player_history_df
# This table contans information about individual match performance of every player.
# 
# ## Changes made
# - Renaming columns
# 
# 
# 


# Checking data types
# player_history_df.info()

# Checking columns and values.

# Find the columns with only one unique value. 
# These columns can be dropped as they do not provide any useful information.
constant_columns = [col for col in player_history_df.columns if player_history_df[col].nunique() <= 1]

# Checking ranges of data
# To confirm if we have full
# print(f"Data Range: {player_history_df['kickoff_time'].min()} to {player_history_df['kickoff_time'].max()}")


# Dropping Irrelevant columns
player_history_df = player_history_df.drop(columns=constant_columns)


# Renaming columns for better clarity
player_history_df = player_history_df.rename(columns={
    'team_h_score':'team_home_score',
    'team_a_score':'team_away_score',
    'bps':'bonus_points_system_score',
    'ict_index':'influence_creativity_threat_index'
})


# Type Conversion

# Converting 'kickoff_time' to datetime
player_history_df['kickoff_time'] = pd.to_datetime(player_history_df['kickoff_time'])

# Boolean conversion: Columns with only two values (0 and 1) can be converted to boolean type
# Converting `was_home`
player_history_df['was_home'] = player_history_df['was_home'].astype(bool)
# Converting `started`
player_history_df['starts'] = player_history_df['starts'].astype(bool)


# list all columns in ascending order
sorted(player_history_df.columns)


# Getting which team won


def get_player_team_score(row):
    return row['team_home_score'] if row['was_home'] else row['team_away_score']

def get_opponent_team_score(row):
    return row['team_away_score'] if row['was_home'] else row['team_home_score']

# Create result column (W/D/L) based on team and opponent scores
def get_match_result(row):
    player_score = get_player_team_score(row)
    opponent_score = get_opponent_team_score(row)

    if player_score > opponent_score:
        return 'W'  # Win
    elif player_score < opponent_score:
        return 'L'  # Loss
    else:
        return 'D'  # Draw

player_history_df['match_result'] = player_history_df.apply(get_match_result, axis=1)

# player_history_df.info()
player_history_df.head()

 
# # Cleaning player_df
# 
# Contains the bio, current statues, prices and performance of each player.
# Many columns present.


sorted(players_df.columns)


# Renaming columns for better clarity
players_df = players_df.rename(columns={
    'now_cost': 'price',
    'element_type': 'position_id', # This is to be mapped
    'team': 'team_id',
    'web_name': 'player_name'
})

# Data type conversion and normalization
#  `price`
players_df['price'] = players_df['price'] / 10.0  # Convert to actual price in millions
# `selected_by_percent`
players_df['selected_by_percent'] = players_df['selected_by_percent'].astype(float)

# Mapping IDs to Descriptive Names
#  Use mapping tables to replace numeric IDs with actual team and position names
position_map = positions_df.set_index('position_id')['position_name'].to_dict()
# print(position_map)
players_df['position'] = players_df['position_id'].map(position_map)

team_map = teams_df.set_index('id')['name'].to_dict()
# print(team_map)
players_df['team_name'] = players_df['team_id'].map(team_map)


# Feature Selection
#  Select only relavent columns from the player_df
columns_to_keep = [
    'player_name', 
    'team_name',
    'position',
    'price', 
    'total_points', 
    'points_per_game',
    'form', 
    'status', 
    'chance_of_playing_next_round', 
    'selected_by_percent',
    'minutes', 
    'goals_scored', 
    'assists', 
    'clean_sheets', 
    'bonus', 
    'bps',
    'ict_index', 'expected_goals', 
    'expected_assists', 
    'expected_goal_involvements', 
    'expected_goals_conceded']

players_df = players_df[columns_to_keep].copy()

 
# # Cleaning teams_df


# Renaming columns
teams_df = teams_df.rename(columns={
    'id':'team_id',
    'name':'team_name',
    'short_name':'team_short_name',
    'strength':'overall_strength_rating'
})

# Add a full_name column by mapping the team id with a string
team_full_name_map = {
    1: 'Arsenal',
    2: 'Aston Villa',
    3: 'Bournemouth',
    4: 'Brentford',
    5: 'Brighton & Hove Albion',
    6: 'Burnley',
    7: 'Chelsea',
    8: 'Crystal Palace',
    9: 'Everton',
    10: 'Fulham',
    11: 'Leeds United',
    12: 'Leicester City',
    13: 'Liverpool',
    14: 'Manchester City',
    15: 'Manchester United',
    16: 'Newcastle United',
    17: 'Nottingham Forest',
    18: 'Southampton',
    19: 'Tottenham Hotspur',
    20: 'West Ham United',
    21: 'Wolverhampton Wanderers'
}

teams_df['team_full_name'] = teams_df['team_id'].map(team_full_name_map)

# Selecting features
teams_df = teams_df[[
    'team_id', 
    'team_name', 
    'team_short_name', 
    'team_full_name',
    'overall_strength_rating',
    'strength_overall_home', 
    'strength_overall_away',
    'strength_attack_home', 
    'strength_attack_away',
    'strength_defence_home', 
    'strength_defence_away'
]]


 
# # Cleaning understat_roster_metrics_df


# Extracting match Id
understat_roster_metrics_df['understat_match'] = understat_roster_metrics_df['match_link'].str.extract(r'(\d+)$').astype(int) 

# Renaming
understat_roster_metrics_df = understat_roster_metrics_df.rename(columns={
    'time':'minutes_played',
    'xg':'expected_goals',
    'xa':'expected_assists',
    'h_a':'location'
})

# Location encoding
understat_roster_metrics_df['is_home'] = understat_roster_metrics_df['location'].apply(lambda x: 1 if x == 'h' else 0)

# Metrics rounding
# 2 Decimal places is standard
adv_cols = [
    'expected_goals', 
    'expected_assists', 
    'xgchain', 
    'xgbuildup'
    ]
understat_roster_metrics_df[adv_cols] = understat_roster_metrics_df[adv_cols].round(2)

# Handling 'Sub' Positions
# Understat marks players as 'Sub' if they came on. However, we often
# want to know their ACTUAL position. We'll keep it for now but note 
# it's a "Role" rather than a "Position" in some cases.
understat_roster_metrics_df['is_starter'] = understat_roster_metrics_df['position'].apply(lambda x: 0 if x == 'Sub' else 1)

# Dropping data that is no longer needed
understat_roster_metrics_df = understat_roster_metrics_df.drop(columns=['match_link','id','location'])

understat_roster_metrics_df.head(10)

 
# # Cleaning understat_team_metrics_df


fpl_teams_df = dfs['fpl_season_teams']
fpl_team_list = sorted(fpl_teams_df['team_name'].unique())

fpl_team_list



# Load files
fpl_teams = dfs['fpl_season_teams']
cs_teams = dfs['teams']

fpl_teams = fpl_teams[['team_id', 'team_name', 'short_name']].drop_duplicates()

cs_teams = cs_teams[['id','name','short_name']].drop_duplicates()
cs_teams = cs_teams.rename(columns={
    'id':'team_id',
    'name':'team_name'
})


primary_ids = cs_teams['team_id'].unique()
additional_teams = fpl_teams[~fpl_teams['team_id'].isin(primary_ids)]
name_map = pd.concat([cs_teams, additional_teams], ignore_index=True)


print(name_map)


# Standardize your existing name_map
fpl_teams = dfs['fpl_season_teams'][['team_id', 'team_name', 'short_name']].drop_duplicates()
cs_teams = dfs['teams'][['id','name','short_name']].drop_duplicates().rename(columns={'id':'team_id','name':'team_name'})

primary_ids = cs_teams['team_id'].unique()
additional_teams = fpl_teams[~fpl_teams['team_id'].isin(primary_ids)]
name_map = pd.concat([cs_teams, additional_teams], ignore_index=True)

# Understat Team Metrics Dataframe
understat_team_metrics_df = dfs['understat_team_metrics']

# Create a dictionary to fix Understat name discrepancies
# This ensures "Manchester City" matches "Man City" in your name_map
name_corrections = {
    'Manchester City': 'Man City',
    'Manchester United': 'Man Utd',
    'Tottenham': 'Spurs',
    'Wolverhampton Wanderers': 'Wolves',
    'Nottingham Forest': "Nott'm Forest",
    'Newcastle United': 'Newcastle',
    'Sheffield United': 'Sheffield Utd',
    'West Bromwich Albion': 'West Brom'
}

understat_team_metrics_df['team_h_clean'] = understat_team_metrics_df['team_h'].replace(name_corrections)
understat_team_metrics_df['team_a_clean'] = understat_team_metrics_df['team_a'].replace(name_corrections)

# Map the Unique Team IDs to the metrics table
# Join for Home Team
understat_team_metrics_df = understat_team_metrics_df.merge(name_map[['team_id', 'team_name']], left_on='team_h_clean', right_on='team_name', how='left')
understat_team_metrics_df = understat_team_metrics_df.rename(columns={'team_id': 'team_h_id'}).drop(columns=['team_name'])

# Join for Away Team
understat_team_metrics_df = understat_team_metrics_df.merge(name_map[['team_id', 'team_name']], left_on='team_a_clean', right_on='team_name', how='left')
understat_team_metrics_df = understat_team_metrics_df.rename(columns={'team_id': 'team_a_id'}).drop(columns=['team_name'])

# Final Cleaning: Drop helper columns and Understat's original inconsistent IDs
understat_team_metrics_df['date'] = pd.to_datetime(understat_team_metrics_df['date'])
understat_team_metrics_df = understat_team_metrics_df.drop(columns=['h', 'a', 'team_h_clean', 'team_a_clean'])

# Sort by date for chronological analysis
understat_team_metrics_df = understat_team_metrics_df.sort_values('date').reset_index(drop=True)

understat_team_metrics_df[['date', 'team_h_id', 'team_h', 'team_a_id', 'team_a', 'h_xg', 'a_xg']].head()
understat_team_metrics_df.head(20)


# 
cleaned_dfs = {
    "fact_player_gameweeks": fact_player_gameweeks_df,
    "fpl_fixtures": fpl_fixtures_df,
    "fpl_season_players": fpl_season_players_df,
    "fpl_season_teams": fpl_season_teams_df,
    "player_history": player_history_df,
    "players": players_df,
    "positions": positions_df,
    "teams": teams_df,
    "understat_roster_metrics": understat_roster_metrics_df,
    "understat_team_metrics": understat_team_metrics_df
}

# %store cleaned_dfs


# Assuming cleaned_dfs is your dictionary of dataframes
with open('cleaned_data.pkl', 'wb') as f:
    pickle.dump(cleaned_dfs, f)




# # Loop through and save each one
# for name, df in cleaned_dfs.items():
#     filename = f"cleaned_{name}.csv"
#     df.to_csv(filename, index=False)
#     print(f"Saved: {filename}")


