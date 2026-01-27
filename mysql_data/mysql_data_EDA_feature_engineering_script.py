


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import unicodedata
import pickle

# %store -r cleaned_dfs

with open('cleaned_data.pkl', 'rb') as f:
    cleaned_dfs = pickle.load(f)




# Loading dataframes from cleaned
cleaned_dfs

fact_player_gameweeks_df = cleaned_dfs['fact_player_gameweeks']
fpl_fixtures_df = cleaned_dfs['fpl_fixtures'] 
fpl_season_players_df = cleaned_dfs['fpl_season_players']
fpl_season_teams_df = cleaned_dfs['fpl_season_teams']
player_history_df = cleaned_dfs['player_history']
players_df = cleaned_dfs['players']
positions_df = cleaned_dfs['positions']
teams_df = cleaned_dfs['teams']
understat_roster_metrics_df = cleaned_dfs['understat_roster_metrics']
understat_team_metrics_df = cleaned_dfs['understat_team_metrics']


 
# # EDA


# EDA: Correlation within understat_roster_matrics_df
corr_matrix = understat_roster_metrics_df[[
    'goals','shots','expected_goals','key_passes','assists','expected_assists','xgchain'
    ]].corr()

plt.figure(figsize = (10,8))
sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt=".2f")
plt.title('Correlation Heatmap: Understat Performance Matrics')
plt.savefig('correlation_heatmap_roster_df.png')

'''
The heatmap is used to identify collinearity.
- Correlation between shots and expected_goals
- Here, for expected_goals, shots and xg_chain are observed to have higher collinearity
''' 


# EDA: Home vs Away Performance Bias
''' 
Checking the performance of teams performance when they are at home and when they are away.
'''
# Compare home_expected_goals vs away_expected_goals
plt.figure(figsize=(8,6))
sns.kdeplot(understat_team_metrics_df['h_xg'], label='Home xG', fill=True)
sns.kdeplot(understat_team_metrics_df['a_xg'], label='Away xg', fill=True)
plt.title('Distribution of Expected Goals (xG): Home vs Array')
plt.xlabel('xG per Match')
plt.legend()
# plt.savefig('home_away_xg_dist.png')
plt.show()
''' 

'''


# EDA: Price vs Value Analysis
'''
For business, it is profitable if players score high points despite low price.
'''

plt.figure(figsize=(10,6))
sns.scatterplot(data=players_df, x='price', y='total_points', hue='position', alpha=0.6)
plt.title('Player Price vs. Total Points (Value Mapping)')
plt.grid(True, linestyle='--', alpha=0.7)
# plt.savefig('price_vs_points.png')
plt.show()
''' 
Scatter plot helps identify outliers.
- Top-left players are high-value/low-cost(must-haves)
- Bottom-left players are over priced(avoid)
'''

 
# # Feature Engineering


# Calculating Rolling Averages
# Recent performance is usually more predictive than season-long totals.
# A 3-match rolling average for player's points and exptected_goals

# Sort by player and event to ensure chronological order
fact_player_gameweeks_df = fact_player_gameweeks_df.sort_values(['player_id','event'])
fact_player_gameweeks_df['points_form_3'] = fact_player_gameweeks_df.groupby('player_id')['total_points'].transform(
    lambda x: x.rolling(window=3, min_periods=1).mean()
)

# Shift the results by 1 so we are not useing the current game to predict itself
# Also prevents data leakage
fact_player_gameweeks_df['previous_point_form_3'] = fact_player_gameweeks_df.groupby('player_id')['points_form_3'].shift(1)

fact_player_gameweeks_df[['player_name','event','total_points','previous_point_form_3']].sample(10)

# Model sees "What was this player's average points over the last 3 games before this match started?"


# Difficulty-Adjusted Metrics
# A scoring against some teams can be more harder than scoring against another team.
# A 'Weighted_Threat" feature

# Merge gameweek data with season team data to get strength rating
merged = fact_player_gameweeks_df.merge(
    fpl_season_teams_df[['team_id', 'strength_overall_away']].drop_duplicates(),
    left_on='opponent_id',
    right_on='team_id',
    how='left'
)
merged['difficulty_adjusted_points'] = merged['total_points'] / (merged['strength_overall_away'] / 1000)

merged.head()

# Helps standardize performance.
# - Few points against a top-level team is better performance, than getting the same points against a struggling team


# Lagged "Last Game" Features
# Players often have streaks, 
# Capture if a player was subbed early or missed the last game

# Capture the minutes played in previous match
fact_player_gameweeks_df['previous_match_mins'] = fact_player_gameweeks_df.groupby('player_id')['minutes'].shift(1)

# Binary feature: Did they start the last match
fact_player_gameweeks_df['started_last_match'] = (fact_player_gameweeks_df['previous_match_mins'] >= 60).astype(int)




def normalize_name(name):
    """
    Standardizes names by removing accents, 
    converting to lowercase, and stripping whitespace.
    """
    if pd.isna(name): return ""
    name = unicodedata.normalize('NFD', str(name)).encode('ascii', 'ignore').decode('utf-8')
    return name.lower().strip()

# Add Season Info to FPL Gameweeks
# FPL GW data usually identifies matches by 'event' (GW); we need 'season' to match Understat
fact_player_gameweeks_season = pd.merge(
    fact_player_gameweeks_df,
    fpl_season_players_df[['element_id', 'full_name', 'season_start']].drop_duplicates(),
    left_on=['player_id', 'player_name'],
    right_on=['element_id', 'full_name'],
    how='left'
).rename(columns={'season_start': 'season'}).drop(columns=['full_name', 'element_id'])

# Enrich Understat Roster with Season and Match Meta
# Links individual player stats to the correct season via the Understat Match ID
print("Enriching Understat roster metrics...")
understat_roster_enriched = pd.merge(
    understat_roster_metrics_df,
    understat_team_metrics_df[['id', 'season']],
    left_on='understat_match',
    right_on='id',
    how='left'
).drop(columns=['id'])

# Normalize Names for Both Dataframes
# This ensures 'Gabriel Martinelli' (FPL) matches 'Martinelli' or 'Gabriel Martinelli' (Understat)
fact_player_gameweeks_season['join_name'] = fact_player_gameweeks_season['player_name'].apply(normalize_name)
understat_roster_enriched['join_name'] = understat_roster_enriched['player'].apply(normalize_name)

# Perform the Master Join
# We join on Normalized Name, Season, and Home/Away status
# This uniquely identifies a player's performance in a specific fixture
master_df = pd.merge(
    fact_player_gameweeks_season,
    understat_roster_enriched[[
        'join_name', 'season', 'is_home', 
        'expected_goals', 'expected_assists', 'key_passes', 
        'xgchain', 'xgbuildup', 'shots', 'understat_match'
    ]],
    on=['join_name', 'season', 'is_home'],
    how='inner'
)

# Final Cleaning
# Remove duplicates that may arise from multi-season players or name collisions
master_df = master_df.drop_duplicates().reset_index(drop=True)

# Save the master table
master_df.to_csv('master_table.csv', index=False)

print(f"Success! 'master_table.csv' created with {len(master_df)} rows.")
print("\nPreview of Unified Data (Points + xG):")
print(master_df[['player_name', 'event', 'total_points', 'expected_goals', 'expected_assists']].head())

#%store master_df

with open('master_df.pkl', 'wb') as f:
    pickle.dump(master_df, f)

