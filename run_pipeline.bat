@echo off
setlocal enabledelayedexpansion

echo ==================================================
echo Starting FPL Data Pipeline
echo ==================================================

REM --------------------------------------------------
REM Activate virtual environment and Setup Paths
REM Uses %~dp0 to locate project root
REM --------------------------------------------------
SET "PROJECT_ROOT=%~dp0"
SET "VENV_PATH=%PROJECT_ROOT%.venv\Scripts\activate.bat"

REM CRITICAL: Set PYTHONPATH to project root so python scripts can find 'Utils' and 'Api_calls' modules
SET "PYTHONPATH=%PROJECT_ROOT%"

if not exist "%VENV_PATH%" (
    echo ERROR: Virtual environment not found at "%VENV_PATH%"
    echo Please ensure .venv exists in the project root.
    pause
    exit /b 1
)

call "%VENV_PATH%"

REM --------------------------------------------------
REM 1. Cold Start
REM --------------------------------------------------
echo.
echo [1/9] Cold start ingestion
python "%PROJECT_ROOT%Scripts\events_cold_start.py"

if errorlevel 1 (
    echo WARNING: Cold start script returned error usually meaning data already exists.
    echo Continuing to next step...
)

REM --------------------------------------------------
REM 2. Incremental Update
REM --------------------------------------------------
echo.
echo [2/9] Incremental update
python "%PROJECT_ROOT%Scripts\incremental_event_update.py"

if errorlevel 1 (
    echo ERROR: Incremental update failed
    pause
    exit /b 1
)

REM --------------------------------------------------
REM 3. Get Fixtures
REM --------------------------------------------------
echo.
echo [3/9] Fetching fixtures
python "%PROJECT_ROOT%Scripts\ingest_fixture.py"

if errorlevel 1 (
    echo ERROR: Fixture ingestion failed
    pause
    exit /b 1
)

REM --------------------------------------------------
REM 4. Player Snapshots
REM --------------------------------------------------
echo.
echo [4/9] Fetching player snapshots
python "%PROJECT_ROOT%Scripts\player_snapshot.py"

if errorlevel 1 (
    echo ERROR: Player snapshot failed
    pause
    exit /b 1
)

REM --------------------------------------------------
REM 5. Player History Dump
REM --------------------------------------------------
echo.
echo [5/9] Fetching player history
python "%PROJECT_ROOT%Scripts\player_history_dump.py"

if errorlevel 1 (
    echo ERROR: Player history dump failed
    pause
    exit /b 1
)

REM --------------------------------------------------
REM 6. Build Fact Table
REM --------------------------------------------------
echo.
echo [6/9] Building fact table
python "%PROJECT_ROOT%Scripts\build_fact_table.py"

if errorlevel 1 (
    echo ERROR: Fact table build failed
    pause
    exit /b 1
)

REM --------------------------------------------------
REM 7. Ingest Understat Teams (Kaggle)
REM --------------------------------------------------
echo.
echo [7/9] Ingesting Understat Team Metrics
python "%PROJECT_ROOT%Scripts\ingest_understat_teams.py"

if errorlevel 1 (
    echo ERROR: Understat Team Metrics ingestion failed
    pause
    exit /b 1
)

REM --------------------------------------------------
REM 8. Ingest Understat Roster (Kaggle)
REM --------------------------------------------------
echo.
echo [8/9] Ingesting Understat Roster Metrics
python "%PROJECT_ROOT%Scripts\ingest_understat_roster.py"

if errorlevel 1 (
    echo ERROR: Understat Roster Metrics ingestion failed
    pause
    exit /b 1
)

REM --------------------------------------------------
REM 9. Ingest FPL GitHub History
REM --------------------------------------------------
echo.
echo [9/9] Ingesting Historical FPL Data from GitHub
python "%PROJECT_ROOT%Scripts\ingest_fpl_github.py"

if errorlevel 1 (
    echo ERROR: GitHub history ingestion failed
    pause
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
pause
