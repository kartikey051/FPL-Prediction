@echo off
setlocal enabledelayedexpansion

echo ==================================================
echo Starting FPL Data Pipeline
echo ==================================================

REM --------------------------------------------------
REM Activate virtual environment
REM --------------------------------------------------
if not exist "C:\Users\karti\Desktop\final project\FPL-Prediction\.venv\Scripts\activate.bat" (
    echo ERROR: Virtual environment not found at .venv
    exit /b 1
)

call .venv\Scripts\activate.bat

REM --------------------------------------------------
REM 1. Cold Start
REM --------------------------------------------------
echo.
echo [1/6] Cold start ingestion
python -m Scripts.events_cold_start

if errorlevel 1 (
    echo ERROR: Cold start failed
    exit /b 1
)

REM --------------------------------------------------
REM 2. Incremental Update
REM --------------------------------------------------
echo.
echo [2/6] Incremental update
python -m Scripts.incremental_event_update

if errorlevel 1 (
    echo ERROR: Incremental update failed
    exit /b 1
)

REM --------------------------------------------------
REM 3. Get Fixtures
REM --------------------------------------------------
echo.
echo [3/6] Fetching fixtures
python -m Scripts.ingest_fixture

if errorlevel 1 (
    echo ERROR: Fixture ingestion failed
    exit /b 1
)

REM --------------------------------------------------
REM 4. Player Snapshots
REM --------------------------------------------------
echo.
echo [4/6] Fetching player snapshots
python -m Scripts.player_snapshot

if errorlevel 1 (
    echo ERROR: Player snapshot failed
    exit /b 1
)

REM --------------------------------------------------
REM 5. Player History Dump
REM --------------------------------------------------
echo.
echo [5/6] Fetching player history
python -m Scripts.player_history_dump

if errorlevel 1 (
    echo ERROR: Player history dump failed
    exit /b 1
)

REM --------------------------------------------------
REM 6. Build Fact Table
REM --------------------------------------------------
echo.
echo [6/6] Building fact table
python -m Scripts.build_fact_table

if errorlevel 1 (
    echo ERROR: Fact table build failed
    exit /b 1
)

REM --------------------------------------------------
REM Done
REM --------------------------------------------------
echo.
echo ==================================================
echo Pipeline completed successfully
echo ==================================================

endlocal
