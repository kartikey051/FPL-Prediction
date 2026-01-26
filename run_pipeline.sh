#!/bin/bash

# Exit immediately if any command fails
set -e

echo "=================================================="
echo "Starting FPL Data Pipeline"
echo "=================================================="

# Get the absolute path of the directory containing this script
PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"

# Define the path to the Mac virtual environment (bin instead of Scripts)
VENV_ACTIVATE="$PROJECT_ROOT/.venv/bin/activate"

# CRITICAL: Set PYTHONPATH to project root so 'python -m' can find your modules
export PYTHONPATH="$PROJECT_ROOT"

if [ ! -f "$VENV_ACTIVATE" ]; then
    echo "ERROR: Virtual environment not found at $VENV_ACTIVATE"
    echo "Check if your .venv folder exists in $PROJECT_ROOT"
    exit 1
fi

# Activate the virtual environment
source "$VENV_ACTIVATE"
echo "Virtual environment activated."

# Helper function to run modules using python -m
run_module() {
    local module_path="$1"
    local step_name="$2"
    echo ""
    echo "[$step_name] Running: python -m $module_path"
    
    # Run the module. If it fails, the 'set -e' will stop the script.
    python3 -m "$module_path"
}

# --------------------------------------------------
# Execution Steps (using dot notation for modules)
# --------------------------------------------------

# Step 1: Cold Start
# Assuming your script is at Scripts/events_cold_start.py
run_module "Scripts.events_cold_start" "1/8 Ingestion (Cold Start)"

# Step 2: Incremental Update
run_module "Scripts.incremental_event_update" "2/8 Incremental update"

# Step 3: Get Fixtures
run_module "Scripts.ingest_fixture" "3/8 Fetching fixtures"

# Step 4: Player Snapshots
run_module "Scripts.player_snapshot" "4/8 Fetching player snapshots"

# Step 5: Player History Dump
run_module "Scripts.player_history_dump" "5/8 Fetching player history"

# Step 6: Build Fact Table
run_module "Scripts.build_fact_table" "6/8 Building fact table"

# Step 7: Understat Teams
run_module "Scripts.ingest_understat_teams" "7/8 Ingesting Understat Team Metrics"

# Step 8: Understat Roster
run_module "Scripts.ingest_understat_roster" "8/8 Ingesting Understat Roster Metrics"

echo ""
echo "=================================================="
echo "Pipeline completed successfully"
echo "=================================================="

deactivate
