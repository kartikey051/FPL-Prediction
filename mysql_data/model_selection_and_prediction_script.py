
# Importing Libraries
import pandas as pd
import numpy as np
import pickle

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.impute import SimpleImputer



with open('master_df.pkl', 'rb') as f:
    master_df = pickle.load(f)

master_df


master_df.head(10)
unique_players_count = master_df['player_name'].nunique()
print(f"Number of unique players in master_df: {unique_players_count}")


# Feature Engineering
# We sort the data chornologically to calculate rolling averages
master_df = master_df.sort_values(by=['player_id','season','event'])

# Fill NaN values in relevant columns before calculating rolling averages
# This is crucial because missing stats data will propagate as NaNs in rolling averages
# Filling with 0 makes sense for these stats where NaN implies no activity.
cols_to_fill_na_before_rolling = ['expected_goals','expected_assists','shots','key_passes']
for col in cols_to_fill_na_before_rolling:
    master_df[col] = master_df[col].fillna(0)

# Create 3-game rolling average (This represents form)
cols_to_roll = ['total_points','expected_goals','expected_assists','shots','key_passes']
for col in cols_to_roll:
    # min_periods=1 ensures that if fewer than 3 observations are available, it still calculates the mean over available data.
    # If the original columns already had NaNs, they were filled with 0 above.
    master_df[f'{col}_roll_3'] = master_df.groupby('player_id')[col].transform(lambda x: x.rolling(3, min_periods = 1).mean())

# Create the Target: What we want to predict
master_df['target_pts'] = master_df.groupby('player_id')['total_points'].shift(-1)

# Drop rows where we do not have a future row to compare against
ml_df = master_df.dropna(subset=['target_pts']).copy()
ml_df.columns = [str(col) for col in ml_df.columns] # Ensure all ml_df columns are strings

# Select features and split data
features = [
    'value',
    'is_home',
    'total_points_roll_3',
    'expected_goals_roll_3',
    'shots_roll_3',
    'key_passes_roll_3'
]
X = ml_df[features].copy()
X.columns = [str(col) for col in X.columns] # Ensure X columns are strings
y = ml_df['target_pts']

# Split the data into 80% training and 20% testing
X_train, X_test, y_train, y_test = train_test_split(X,y, test_size=0.2, random_state=42)

# Ensure column names are strings for consistency with scikit-learn
X_train.columns = [str(col) for col in X_train.columns]
X_test.columns = [str(col) for col in X_test.columns]


# Initialize Imputer (e.g., with mean strategy)
imputer = SimpleImputer(strategy='mean')

# Fit on X_train and transform both X_train and X_test
X_train = pd.DataFrame(imputer.fit_transform(X_train), columns=X_train.columns, index=X_train.index)
X_test = pd.DataFrame(imputer.transform(X_test), columns=X_test.columns, index=X_test.index)

# Initialize Models
models = {
    "Linear Regression" : LinearRegression(),
    "Ridge Regression" : Ridge(alpha=1.0),
    "Randome Forest" : RandomForestRegressor(n_estimators=100, random_state=42),
    "Gradient Boosting" : GradientBoostingRegressor(n_estimators=100, random_state=42)
}

# Train and Evaluate
results = []

for name, model in models.items():
    model.fit(X_train, y_train)           # Training
    preds = model.predict(X_test)         # Predicting

    # Calcuate Metrics
    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_absolute_error(y_test, preds))
    r2 = r2_score(y_test, preds)

    results.append({
        "Model": name,
        "MAE": round(mae,4),
        "RMSE": round(rmse,4),
        "R2 Score": round(r2,4)
    })

# Output results (moved outside the loop)
comparison_df = pd.DataFrame(results).sort_values(by="MAE")
comparison_df


# On testing the data with multiple models, Gradient Boosting showed the least errors with high R2 Score.


# 1. Get the best performing model (Gradient Boosting)
gbr_model = models["Gradient Boosting"]

# 2. Prepare data for prediction
# Identify the latest entries for each player where 'target_pts' is not yet known.
# These are the rows in `master_df` that were dropped when creating `ml_df`.
players_to_predict = master_df[master_df['target_pts'].isnull()].copy()

# Select the features used for training
X_predict_df = players_to_predict[features].copy()
X_predict_df.columns = [str(col) for col in X_predict_df.columns] # Ensure column names are strings

# Impute missing values using the imputer fitted during training
# The 'imputer' object should be available from the previous cell's execution.
X_predict_imputed = pd.DataFrame(imputer.transform(X_predict_df),
                                 columns=X_predict_df.columns,
                                 index=X_predict_df.index)

# 3. Make predictions using the Gradient Boosting model
predictions = gbr_model.predict(X_predict_imputed)

# 4. Add predictions back to the players_to_predict DataFrame
players_to_predict['predicted_points'] = predictions

# 5. Get the top 11 players based on predicted points
top_11_players = players_to_predict.sort_values(by='predicted_points', ascending=False).head(11)

# Display the relevant information for the top 11 players
print("Top 11 Players based on Predicted Points for the Next Event:")
print(top_11_players[['player_name', 'team_name', 'value', 'predicted_points', 'total_points_roll_3']])


