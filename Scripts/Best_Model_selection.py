import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import xgboost as xgb
import warnings

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, mean_absolute_percentage_error

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, Conv1D, MaxPooling1D, Flatten, SimpleRNN, LSTM, Input
from tensorflow.keras.callbacks import EarlyStopping

warnings.filterwarnings('ignore')

# Set random seeds for reproducibility
np.random.seed(42)
tf.random.set_seed(42)

# 1. DATA LOADING & PROCESSING
print("="*80)
print("1. LOADING AND PROCESSING DATA")
print("="*80)

# Load datasets
xg = pd.read_csv('clean_team_xg_season.csv')
stats = pd.read_csv('clean_team_season_stats.csv')
metrics = pd.read_csv('clean_team_season_metrics.csv')

print(f"XG Shape:      {xg.shape}")
print(f"Stats Shape:   {stats.shape}")
print(f"Metrics Shape: {metrics.shape}")

# --- Joining Strategy ---
def convert_season_format(season_int):
    return f"{season_int}-{str(season_int+1)[-2:]}"

xg['season_converted'] = xg['season'].apply(convert_season_format)

# Create Merge Keys
metrics['merge_key'] = metrics['season'] + '_' + metrics['team_name']
xg['merge_key'] = xg['season_converted'] + '_' + xg['team_name']

# Merge
xg_supplement = xg[['merge_key', 'xg_for', 'xg_against']].rename(
    columns={'xg_for': 'xg_for_supp', 'xg_against': 'xg_against_supp'}
)
merged_data = metrics.merge(xg_supplement, on='merge_key', how='left')

# Fill NaNs
merged_data['xg_for'] = merged_data['xg_for'].fillna(merged_data['xg_for_supp'])
merged_data['xg_against'] = merged_data['xg_against'].fillna(merged_data['xg_against_supp'])
merged_data = merged_data.drop(['merge_key', 'xg_for_supp', 'xg_against_supp'], axis=1)
merged_data = merged_data.dropna()

print(f"Merged Dataset Shape: {merged_data.shape}")


# 2. FEATURE ENGINEERING
print("\n" + "="*80)
print("2. FEATURE ENGINEERING")
print("="*80)

merged_data['xg_diff'] = merged_data['xg_for'] - merged_data['xg_against']
merged_data['goal_efficiency'] = merged_data['gf'] / (merged_data['xg_for'] + 0.1)
merged_data['defensive_efficiency'] = merged_data['ga'] / (merged_data['xg_against'] + 0.1)
merged_data['win_rate'] = merged_data['wins'] / merged_data['played']
merged_data['goals_per_game'] = merged_data['gf'] / merged_data['played']
merged_data['conceded_per_game'] = merged_data['ga'] / merged_data['played']
merged_data['xg_for_per_game'] = merged_data['xg_for'] / merged_data['played']
merged_data['xg_against_per_game'] = merged_data['xg_against'] / merged_data['played']

# Save for reference
merged_data.to_csv('merged_team_data.csv', index=False)
print("Saved 'merged_team_data.csv'")

# 3. SCALING & SPLITTING
print("\n" + "="*80)
print("3. DATA SPLITTING & SCALING (MinMax)")
print("="*80)

target = 'pts'
exclude_cols = ['season', 'team_id', 'team_name', 'pts', 'played']
feature_cols = [col for col in merged_data.columns if col not in exclude_cols]

X = merged_data[feature_cols].values
y = merged_data[target].values.reshape(-1, 1)

print(f"Features: {len(feature_cols)}")

# Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Scale
scaler_X = MinMaxScaler()
X_train_scaled = scaler_X.fit_transform(X_train)
X_test_scaled = scaler_X.transform(X_test)

scaler_y = MinMaxScaler()
y_train_scaled = scaler_y.fit_transform(y_train)
y_test_scaled = scaler_y.transform(y_test)

# Reshape for Deep Learning Models (Samples, Features, 1)
X_train_reshaped = X_train_scaled.reshape((X_train_scaled.shape[0], X_train_scaled.shape[1], 1))
X_test_reshaped = X_test_scaled.reshape((X_test_scaled.shape[0], X_test_scaled.shape[1], 1))

print("Data Scaled and Reshaped for DL.")

# 4. MODEL TRAINING
results = {}

def calculate_metrics(y_true_scaled, y_pred_scaled, scaler_y):
    # Inverse transform
    y_true = scaler_y.inverse_transform(y_true_scaled.reshape(-1, 1)).ravel()
    y_pred = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1)).ravel()
    
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)
    mape = mean_absolute_percentage_error(y_true, y_pred) * 100
    
    return {'MAE': mae, 'RMSE': rmse, 'R2': r2, 'MAPE': mape, 'predictions': y_pred, 'actuals': y_true}

def train_sklearn_model(name, model):
    print(f"Training {name}...")
    model.fit(X_train_scaled, y_train_scaled.ravel())
    y_pred = model.predict(X_test_scaled)
    results[name] = calculate_metrics(y_test_scaled, y_pred, scaler_y)

def train_keras_model(name, model, epochs=100):
    print(f"Training {name}...")
    es = EarlyStopping(monitor='val_loss', patience=15, restore_best_weights=True)
    # Use reshaped data
    model.fit(X_train_reshaped, y_train_scaled, validation_split=0.2, epochs=epochs, batch_size=16, callbacks=[es], verbose=0)
    y_pred = model.predict(X_test_reshaped, verbose=0)
    results[name] = calculate_metrics(y_test_scaled, y_pred, scaler_y)

# --- A. Standard ML Models ---
train_sklearn_model('Random Forest', RandomForestRegressor(n_estimators=200, max_depth=20, random_state=42))
train_sklearn_model('Gradient Boosting', GradientBoostingRegressor(n_estimators=200, max_depth=10, learning_rate=0.1, random_state=42))
train_sklearn_model('AdaBoost', AdaBoostRegressor(n_estimators=200, learning_rate=0.1, random_state=42))
train_sklearn_model('XGBoost', xgb.XGBRegressor(objective='reg:squarederror', n_estimators=200, max_depth=6, learning_rate=0.1, random_state=42, n_jobs=-1))
train_sklearn_model('MLP (Sklearn)', MLPRegressor(hidden_layer_sizes=(64, 32), max_iter=1000, random_state=42))

# --- B. Deep Learning Models ---

# 1. CNN (1D Convolution)
# Reshaping tabular data as a "sequence" of features
cnn_model = Sequential([
    Input(shape=(X_train_reshaped.shape[1], 1)),
    Conv1D(filters=32, kernel_size=3, activation='relu', padding='same'),
    MaxPooling1D(pool_size=2),
    Flatten(),
    Dense(64, activation='relu'),
    Dropout(0.2),
    Dense(1)
])
cnn_model.compile(optimizer='adam', loss='mse')
train_keras_model('CNN', cnn_model)

# 2. RNN (Simple Recurrent Unit)
rnn_model = Sequential([
    Input(shape=(X_train_reshaped.shape[1], 1)),
    SimpleRNN(32, activation='relu', return_sequences=False),
    Dense(16, activation='relu'),
    Dense(1)
])
rnn_model.compile(optimizer='adam', loss='mse')
train_keras_model('RNN', rnn_model)

# 3. LSTM (Long Short-Term Memory)
lstm_model = Sequential([
    Input(shape=(X_train_reshaped.shape[1], 1)),
    LSTM(32, activation='relu', return_sequences=False),
    Dropout(0.2),
    Dense(16, activation='relu'),
    Dense(1)
])
lstm_model.compile(optimizer='adam', loss='mse')
train_keras_model('LSTM', lstm_model)

# 5. RESULTS & VISUALIZATION
print("\n" + "="*80)
print("5. FINAL COMPARISON")
print("="*80)

results_df = pd.DataFrame({
    'Model': list(results.keys()),
    'MAE': [results[m]['MAE'] for m in results.keys()],
    'RMSE': [results[m]['RMSE'] for m in results.keys()],
    'R2': [results[m]['R2'] for m in results.keys()],
    'MAPE (%)': [results[m]['MAPE'] for m in results.keys()]
})

results_df = results_df.sort_values('R2', ascending=False)
print(results_df.to_string(index=False))

best_model = results_df.iloc[0]['Model']
print(f"\n🏆 Best Model: {best_model} (R2: {results_df.iloc[0]['R2']:.4f})")

# Save
results_df.to_csv('final_model_results.csv', index=False)

# Plotting
fig, axes = plt.subplots(3, 3, figsize=(20, 15))
fig.suptitle(f'Actual vs Predicted Points (Target Inverse Scaled)\nBest Model: {best_model}', fontsize=16)

models_list = list(results.keys())
for idx, model_name in enumerate(models_list):
    row, col = idx // 3, idx % 3
    if row < 3:
        ax = axes[row, col]
        y_true = results[model_name]['actuals']
        y_pred = results[model_name]['predictions']
        r2 = results[model_name]['R2']
        
        ax.scatter(y_true, y_pred, alpha=0.6, color='blue')
        ax.plot([y_true.min(), y_true.max()], [y_true.min(), y_true.max()], 'r--', lw=2)
        ax.set_title(f"{model_name}\nR² = {r2:.3f}")
        ax.set_xlabel("Actual")
        ax.set_ylabel("Predicted")
        ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('all_models_comparison.png')
print("✓ Saved plot: all_models_comparison.png")
