import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error,r2_score
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense   
#import lstm and cnn
from tensorflow.keras.layers import LSTM, Conv2D, MaxPooling2D, Flatten

# 1. Load Data
df = pd.read_pickle(r'C:\Users\aneesh pal\FPL-Prediction\master_df.pkl')

# 2. THE ABSOLUTE FIX: Force column names to standard Python strings
# This removes the 'quoted_name' type that scikit-learn is complaining about
df.columns = [str(c) for c in df.columns]

# 3. Handle missing values (Crucial for Random Forest)
# Players not in the Understat system will have NaNs in xG/xA columns
df = df.fillna(0)

# 4. Create the Time-Series Split (Fixes the NameError)
# We train on everything except the last 3 Gameweeks
last_gw = df['event'].max()
train_df = df[df['event'] < (last_gw - 3)]
test_df = df[df['event'] >= (last_gw - 3)]


# 5. Define features and target
# Ensure these strings match your df columns exactly
features = ['minutes', 'value', 'goals_scored', 'assists', 'clean_sheets', 
            'is_home', 'points_form_3', 'expected_goals', 'expected_assists', 'xgchain']
target = 'total_points'

X_train, y_train = train_df[features], train_df[target]
X_test, y_test = test_df[features], test_df[target]

# 6. Define Models
models = {
    "LSTM": Sequential([
        LSTM(50, activation='relu', input_shape=(X_train.shape[1], 1)),
        Dense(1)    
    ]),
    "CNN": Sequential([
        Conv2D(32, (2, 2), activation='relu', input_shape
=(X_train.shape[1], 1, 1)),
        MaxPooling2D((2, 2)),   
        Flatten(),
        Dense(1)
    ]),
    
    "Baseline (Linear-ish)": RandomForestRegressor(n_estimators=1000, max_depth=10, random_state=42),
    "Random Forest": RandomForestRegressor(n_estimators=1000, random_state=42),
    "XGBoost": XGBRegressor(n_estimators=1000, learning_rate=0.05, max_depth=10, random_state=25)
}

# 7. Model Selection Loop
print(f"{'Model':<20} | {'MAE':<10} | {'RMSE':<10} | {'R2':<10} ")
print("-" * 45)

for name, model in models.items():
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    
    mae = mean_absolute_error(y_test, predictions)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    
    print(f"{name:<20} | {mae:<10.4f} | {rmse:<10.4f} | {r2_score(y_test, predictions):<10.4f}")