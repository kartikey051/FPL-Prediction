# FPL Prediction - Complete Project Documentation

> **Comprehensive technical documentation covering all components of the Fantasy Premier League (FPL) Prediction system, from data ingestion pipelines to web API endpoints and machine learning models.**

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Environment Setup](#environment-setup)
4. [Data Ingestion Pipeline](#data-ingestion-pipeline)
5. [Utility Modules](#utility-modules)
6. [Exception Handling](#exception-handling)
7. [Database Layer](#database-layer)
8. [Web Application (FastAPI)](#web-application-fastapi)
9. [API Endpoints Reference](#api-endpoints-reference)
10. [Dashboard Service](#dashboard-service)
11. [Prediction System](#prediction-system)
12. [Machine Learning Models](#machine-learning-models)
13. [Frontend Architecture](#frontend-architecture)
14. [Logging System](#logging-system)
15. [Conventions & Best Practices](#conventions--best-practices)
16. [Pipeline Execution](#pipeline-execution)

---

## Project Overview

The FPL Prediction system is a comprehensive Fantasy Premier League analytics platform that:

- **Ingests** live and historical FPL data from multiple sources
- **Processes** and stores data in a MySQL database
- **Analyzes** player and team performance using statistical methods
- **Predicts** player points using weighted algorithms
- **Visualizes** data through an interactive web dashboard
- **Recommends** optimal squad selections within budget constraints

### Technology Stack

| Layer | Technology |
|-------|------------|
| Backend Framework | FastAPI (Python 3.11+) |
| Database | MySQL 8.0 |
| Authentication | JWT (python-jose, passlib) |
| HTTP Client | Requests |
| Data Processing | Pandas, NumPy |
| ML/DL | Scikit-learn, XGBoost, TensorFlow/Keras |
| Frontend | HTML5, JavaScript (vanilla), CSS, Bootstrap 5 |
| Charts | Chart.js |
| API Documentation | Swagger UI (auto-generated) |

---

## Architecture

```
FPL-Prediction/
├── app/                          # FastAPI Web Application
│   ├── main.py                   # Application entry point
│   ├── api/                      # API routers and services
│   │   ├── auth/                 # Authentication module
│   │   ├── dashboard/            # Dashboard analytics APIs
│   │   └── prediction/           # Prediction APIs
│   ├── core/                     # Core utilities (config, security)
│   ├── db/                       # Database layer
│   │   ├── session.py           # Connection management
│   │   └── models/              # ORM-like models
│   ├── static/                   # CSS, JavaScript
│   └── templates/               # Jinja2 HTML templates
├── Api_calls/                    # FPL API fetch modules
├── Scripts/                      # Data ingestion scripts
├── Utils/                        # Shared utilities
├── Exceptions/                   # Custom exceptions
├── Model/                        # ML model training
├── Data/                         # Data output directory
└── logs/                         # Application logs
```

### Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  FPL Official API          │  GitHub Historical Data  │  Kaggle/Understat   │
│  (bootstrap-static,        │  (vaastav/Fantasy-       │  (Team xG metrics)  │
│   event/live, fixtures)    │   Premier-League)        │                     │
└───────────┬─────────────────┴────────────┬─────────────┴──────────┬─────────┘
            │                               │                        │
            ▼                               ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER (Scripts/)                            │
│  player_snapshot.py │ build_fact_table.py │ ingest_fpl_github.py │          │
│  ingest_fixture.py  │ ingest_understat_*.py │ clean_and_store.py │          │
└───────────┬─────────────────┴────────────┬─────────────┴──────────┬─────────┘
            │                               │                        │
            ▼                               ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MySQL DATABASE                                     │
│  players │ teams │ fixtures │ fact_player_gameweeks │ fpl_season_* │        │
│  understat_team_metrics │ clean_team_season_metrics │ users │ predictions   │
└───────────────────────────────────────┬─────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FastAPI APPLICATION (app/)                            │
│  Authentication │ Dashboard APIs │ Prediction APIs │ Static Files           │
└───────────────────────────────────────┬─────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          WEB DASHBOARD                                       │
│  Overview │ Trends │ Discovery │ Teams │ Standings │ AI Predictions         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Environment Setup

### Required Environment Variables

Create a `.env` file in the project root:

```env
# Database Configuration
FPL_DB_HOST=localhost
FPL_DB_PORT=3306
FPL_DB_USER=your_username
FPL_DB_PASSWORD=your_password
FPL_DB_NAME=fpladmin

# JWT Configuration (change in production!)
JWT_SECRET_KEY=your-super-secret-key-change-in-production
JWT_ALGORITHM=HS256
JWT_ACCESS_TOKEN_EXPIRE_MINUTES=1440

# Application
APP_NAME=FPL Dashboard
DEBUG=false

# Kaggle API (for Understat data)
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

### Configuration Loading

Configuration is managed by Pydantic:

**File: `app/core/config.py`**

```python
class Settings(BaseSettings):
    """Application settings loaded from environment."""
    
    APP_NAME: str = "FPL Dashboard"
    DEBUG: bool = False
    
    # Database - reuse existing FPL_DB_* pattern
    FPL_DB_HOST: str = "localhost"
    FPL_DB_PORT: int = 3306
    FPL_DB_USER: str = ""
    FPL_DB_PASSWORD: str = ""
    FPL_DB_NAME: str = ""
    
    # JWT Configuration
    JWT_SECRET_KEY: str = "your-super-secret-key-change-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours
    
    class Config:
        env_file = ".env"
        extra = "ignore"
```

---

## Data Ingestion Pipeline

### Pipeline Overview

The data pipeline orchestrates 10 sequential steps to collect, transform, and store FPL data.

**Orchestrator: `run_pipeline.bat`**

```batch
@echo off
echo Starting FPL Data Pipeline

REM 1. Cold Start - Initial event ingestion
python Scripts\events_cold_start.py

REM 2. Incremental Update - Latest gameweek data
python Scripts\incremental_event_update.py

REM 3. Fixtures - Match schedule data
python Scripts\ingest_fixture.py

REM 4. Player Snapshots - Current player data
python Scripts\player_snapshot.py

REM 5. Player History - Historical performance
python Scripts\player_history_dump.py

REM 6. Fact Table - Aggregated player-gameweek data
python Scripts\build_fact_table.py

REM 7. Understat Teams - Team xG metrics from Kaggle
python Scripts\ingest_understat_teams.py

REM 8. Understat Roster - Player xG metrics from Kaggle
python Scripts\ingest_understat_roster.py

REM 9. GitHub History - Historical seasons (2016-2024)
python Scripts\ingest_fpl_github.py

REM 10. Clean & Store - Data normalization and merging
python Scripts\clean_and_store.py

echo Pipeline completed successfully
```

---

### Script Details

#### 1. `player_snapshot.py` - Current Season Player Data

**Purpose:** Fetches current player, team, and position data from FPL API.

**Source:** `https://fantasy.premierleague.com/api/bootstrap-static/`

**Output Tables:**
- `players` - All current season players
- `teams` - Premier League teams
- `positions` - Position definitions (GKP, DEF, MID, FWD)

```python
from Api_calls.players import fetch_player_snapshot
from Utils.db import upsert_dataframe

players, teams, positions = fetch_player_snapshot()

# Persist to database
upsert_dataframe(players, "players", primary_keys=["id"])
upsert_dataframe(teams, "teams", primary_keys=["id"])
upsert_dataframe(positions, "positions", primary_keys=["id"])
```

#### 2. `build_fact_table.py` - Player Performance Facts

**Purpose:** Builds the denormalized fact table for analytics queries.

**Process:**
1. Fetch all players from bootstrap-static
2. For each player, fetch gameweek history
3. Attach match context (opponent, home/away, FDR)
4. Enrich with team names
5. Store as `fact_player_gameweeks`

**Primary Key:** `(player_id, event)`

**Key Columns:**
| Column | Description |
|--------|-------------|
| `player_id` | Unique player identifier |
| `event` | Gameweek number |
| `total_points` | Points scored in GW |
| `minutes` | Minutes played |
| `goals_scored` | Goals scored in GW |
| `assists` | Assists in GW |
| `value` | Player price at GW |
| `opponent_id` | Opposing team ID |
| `home_away` | H (home) or A (away) |
| `fdr` | Fixture Difficulty Rating |

#### 3. `ingest_fpl_github.py` - Historical Data Ingestion

**Purpose:** Loads historical seasons (2016-17 to 2024-25) from the public GitHub repository.

**Source:** `https://github.com/vaastav/Fantasy-Premier-League`

**Generated Tables:**
- `fpl_season_teams` - Historical team data per season
- `fpl_season_players` - Historical player snapshots
- `fpl_fixtures` - Match results
- `fpl_player_gameweeks` - Player performance by gameweek

```python
SEASONS = ["2016-17", "2017-18", ..., "2024-25"]

for season in SEASONS:
    ingest_season_teams(season)
    ingest_season_players(season)
    ingest_fixtures(season)
    ingest_gameweeks(season)
```

#### 4. `ingest_understat_teams.py` - Understat xG Data

**Purpose:** Downloads team-level xG (expected goals) metrics from Kaggle.

**Dataset:** `yarknyorulmaz/understat-match-team-metrics-dataset-epl-v16-v24`

**Output Table:** `understat_team_metrics`

**Key Columns:**
| Column | Description |
|--------|-------------|
| `season` | Year (e.g., 2023) |
| `team_h` | Home team name |
| `team_a` | Away team name |
| `h_xg` | Home team xG |
| `a_xg` | Away team xG |
| `h_goals` | Home team goals |
| `a_goals` | Away team goals |

#### 5. `clean_and_store.py` - Data Normalization

**Purpose:** Merges FPL and Understat data into clean analytics tables.

**Process:**
1. Drop and recreate clean tables
2. Aggregate FPL fixtures into team season stats (W/D/L, GF, GA, Pts)
3. Aggregate Understat into team xG totals
4. Merge using team name + season key
5. Store final merged data

**Output Tables:**
- `clean_team_season_stats` - FPL-only team stats
- `clean_team_xg_season` - Understat xG by team/season
- `clean_team_season_metrics` - **Final merged table** (FPL + xG)

**Schema: `clean_team_season_metrics`**
```sql
CREATE TABLE clean_team_season_metrics (
    season VARCHAR(10),
    team_id INT,
    team_name VARCHAR(255),
    played INT,
    wins INT,
    draws INT,
    losses INT,
    gf INT,             -- Goals For
    ga INT,             -- Goals Against
    gd INT,             -- Goal Difference
    pts INT,            -- Points
    xg_for FLOAT,       -- Expected Goals For
    xg_against FLOAT,   -- Expected Goals Against
    PRIMARY KEY (season, team_id)
);
```

---

## Utility Modules

### `Utils/db.py` - Database Operations

This module provides all database connectivity and operations.

#### Connection Management

```python
@contextmanager
def get_connection():
    """Context manager that yields a MySQL connection."""
    cfg = _get_db_config()
    try:
        conn = mysql.connector.connect(**cfg)
        yield conn
    except MySQLError as e:
        logger.error(f"Failed to connect to MySQL: {e}")
        raise DBConnectionError(...)
    finally:
        if conn and conn.is_connected():
            conn.close()
```

#### Key Functions

| Function | Purpose |
|----------|---------|
| `upsert_dataframe(df, table_name, primary_keys)` | Insert/update DataFrame to table |
| `upsert_events(records)` | Upsert raw event JSON data |
| `execute_query(query, params, fetch=True)` | Execute SELECT query |
| `execute_write(query, params)` | Execute INSERT/UPDATE/DELETE |
| `create_table_from_df(conn, table, df, pks)` | Auto-create table from DataFrame |

#### Upsert Logic

```python
def upsert_dataframe(df, table_name, primary_keys=None, batch_size=1000):
    """Insert or update rows from DataFrame."""
    
    # Build INSERT ... ON DUPLICATE KEY UPDATE
    update_clause = ""
    if primary_keys:
        updates = [f"`{col}` = VALUES(`{col}`)" 
                   for col in df.columns if col not in primary_keys]
        update_clause = "ON DUPLICATE KEY UPDATE " + ", ".join(updates)
    
    sql = f"""
        INSERT INTO `{table_name}` ({columns})
        VALUES ({placeholders})
        {update_clause}
    """
    
    # Batch execution for large datasets
    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        cur.executemany(sql, batch)
        conn.commit()
```

### `Utils/http.py` - HTTP Client

Centralized HTTP client with retry logic and error classification.

```python
def safe_get(url: str, retries: int = 3, backoff: float = 2.0):
    """
    HTTP GET with automatic retries.
    
    - Exponential backoff (2, 4, 8 seconds...)
    - Jitter to prevent thundering herd
    - Retries on 429, 500, 502, 503, 504
    - Raises FPLClientError on 4xx (non-retryable)
    """
    
def _raw_get(url: str):
    """
    Single HTTP GET with response classification.
    """
    resp = requests.get(url, timeout=DEFAULT_TIMEOUT)
    
    # Retryable server errors
    if resp.status_code in (429, 500, 502, 503, 504):
        raise FPLServerError(f"Temporary server error {resp.status_code}")
    
    # Non-retryable client errors
    if 400 <= resp.status_code < 500:
        raise FPLClientError(f"Client error {resp.status_code}")
    
    return resp
```

### `Utils/retry.py` - Retry Decorator

Generic retry wrapper with exponential backoff:

```python
def retry_request(func, retries=3, backoff=2.0, jitter=True):
    """
    Retry strategy:
    - Exponential backoff: 2^attempt seconds
    - Optional jitter: +random(0, 1) seconds
    - Logs each attempt
    """
    for attempt in range(1, retries + 1):
        try:
            return func()
        except RETRYABLE_EXCEPTIONS as e:
            if attempt == retries:
                raise
            
            sleep_time = backoff ** attempt
            if jitter:
                sleep_time += random.uniform(0, 1.0)
            
            time.sleep(sleep_time)
```

### `Utils/logging_config.py` - Unified Logging

Consistent logging across all modules:

```python
def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with:
    - File output: logs/pipeline.log
    - Console output
    - Format: [timestamp] LEVEL | module | message
    """
    logger = logging.getLogger(name)
    
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s | %(name)s | %(message)s"
    )
    
    file_handler = logging.FileHandler("logs/pipeline.log")
    console_handler = logging.StreamHandler()
    
    # Prevent duplicate handlers
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger
```

**Log Format Example:**
```
[2026-01-29 15:30:22,456] INFO | player_snapshot | Player snapshot written to CSV
[2026-01-29 15:30:23,789] ERROR | db | Failed to connect to MySQL: Connection refused
```

---

## Exception Handling

### Exception Hierarchy

```
Exception
├── FPLBaseError (Base for all FPL errors)
│   ├── FPLNetworkError      # Connection/timeout errors
│   ├── FPLServerError       # 5xx API responses (retryable)
│   ├── FPLClientError       # 4xx API responses (non-retryable)
│   └── FPLSchemaError       # Unexpected JSON structure
├── APIRequestError          # Generic API failure
├── GameweekDiscoveryError   # Cannot determine latest GW
├── WriteFileError           # File I/O errors
├── DBConnectionError        # Database connection failed
├── DBWriteError             # Database write/schema failed
├── PlayerFetchError         # Player data fetch failed
└── HTTPException (FastAPI)
    ├── CredentialsException     # 401 Unauthorized
    ├── UserExistsException      # 400 Bad Request
    ├── UserNotFoundException    # 404 Not Found
    └── DatabaseException        # 500 Internal Error
```

### `Exceptions/api_errors.py`

```python
class APIRequestError(Exception):
    """Raised when an API request fails after retries."""
    pass

class GameweekDiscoveryError(Exception):
    """Raised when the latest gameweek cannot be determined."""
    pass

class DBConnectionError(Exception):
    """Raised when database connection cannot be established."""
    pass

class DBWriteError(Exception):
    """Raised when a database write or schema operation fails."""
    pass
```

### `Exceptions/fpl_exceptions.py`

```python
class FPLBaseError(Exception):
    """Base class for FPL pipeline errors."""
    pass

class FPLNetworkError(FPLBaseError):
    """Network/connection errors (retryable)."""
    pass

class FPLServerError(FPLBaseError):
    """FPL API 5xx errors (retryable)."""
    pass

class FPLClientError(FPLBaseError):
    """FPL API 4xx errors (non-retryable)."""
    pass

class FPLSchemaError(FPLBaseError):
    """Unexpected JSON structure from API."""
    pass
```

### `app/core/exceptions.py` - HTTP Exceptions

```python
class CredentialsException(HTTPException):
    """401 Unauthorized - invalid credentials."""
    def __init__(self, detail="Could not validate credentials"):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
        )

class UserExistsException(HTTPException):
    """400 Bad Request - duplicate user."""
    def __init__(self, detail="Username already registered"):
        super().__init__(status_code=400, detail=detail)

class UserNotFoundException(HTTPException):
    """404 Not Found - user doesn't exist."""
    def __init__(self, detail="User not found"):
        super().__init__(status_code=404, detail=detail)

class DatabaseException(HTTPException):
    """500 Internal Error - database operation failed."""
    def __init__(self, detail="Database operation failed"):
        super().__init__(status_code=500, detail=detail)
```

---

## Database Layer

### Schema Overview

#### Current Season Tables

| Table | Description | Primary Key |
|-------|-------------|-------------|
| `players` | Current player details | `id` |
| `teams` | Premier League teams | `id` |
| `positions` | Element type definitions | `id` |
| `fixtures` | Current season fixtures | `id` |
| `fact_player_gameweeks` | Player performance facts | `(player_id, event)` |
| `events_raw` | Raw gameweek JSON data | `event_id` |

#### Historical Tables

| Table | Description | Primary Key |
|-------|-------------|-------------|
| `fpl_season_players` | Historical player snapshots | `(season, element_id)` |
| `fpl_season_teams` | Historical team data | `(season, team_id)` |
| `fpl_fixtures` | Historical fixtures | `(season, fixture_id)` |
| `fpl_player_gameweeks` | Historical player performance | `(season, element_id, gameweek)` |

#### Analytics Tables

| Table | Description | Primary Key |
|-------|-------------|-------------|
| `clean_team_season_stats` | FPL team aggregates | `(season, team_id)` |
| `clean_team_xg_season` | Understat xG aggregates | `(season, team_name)` |
| `clean_team_season_metrics` | Merged FPL + xG | `(season, team_id)` |
| `understat_team_metrics` | Raw Understat match data | `id` or `(match_id, team_id)` |

#### User & Prediction Tables

| Table | Description | Primary Key |
|-------|-------------|-------------|
| `users` | Registered user accounts | `id` |
| `player_predicted_points` | Daily predictions | `(player_id, prediction_date)` |

### Session Management

**File: `app/db/session.py`**

```python
def get_db_config() -> dict:
    """Get database configuration from settings."""
    return {
        "host": settings.FPL_DB_HOST,
        "port": settings.FPL_DB_PORT,
        "user": settings.FPL_DB_USER,
        "password": settings.FPL_DB_PASSWORD,
        "database": settings.FPL_DB_NAME,
    }

@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    conn = mysql.connector.connect(**get_db_config())
    try:
        yield conn
    finally:
        if conn.is_connected():
            conn.close()

def get_db() -> Generator:
    """FastAPI dependency for database connections."""
    with get_db_connection() as conn:
        yield conn

def execute_query(query: str, params=None, fetch=True):
    """Execute SELECT query, returns list of dicts."""
    with get_db_connection() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params or ())
        if fetch:
            return cursor.fetchall()
        conn.commit()
        return cursor.rowcount

def execute_write(query: str, params=None) -> int:
    """Execute INSERT/UPDATE/DELETE, returns lastrowid or rowcount."""
```

### User Model

**File: `app/db/models/user.py`**

**Schema:**
```sql
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    hashed_password VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username),
    INDEX idx_email (email)
);
```

**Functions:**
```python
def ensure_users_table():
    """Create users table if not exists."""

def get_user_by_username(username: str) -> Optional[dict]:
    """Fetch user by username."""

def get_user_by_email(email: str) -> Optional[dict]:
    """Fetch user by email."""

def create_user(username, email, hashed_password) -> int:
    """Create new user, returns user ID."""

def user_exists(username=None, email=None) -> bool:
    """Check if user exists."""
```

---

## Web Application (FastAPI)

### Application Entry Point

**File: `app/main.py`**

```python
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title=settings.APP_NAME,
    description="FPL Analytics Dashboard with JWT Authentication",
    version="1.0.0",
    docs_url="/docs",      # Swagger UI
    redoc_url="/redoc",    # ReDoc
)

# CORS (allow all origins for development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files & templates
app.mount("/static", StaticFiles(directory="app/static"))
templates = Jinja2Templates(directory="app/templates")

# Include API routers
app.include_router(auth_router)      # /auth/*
app.include_router(dashboard_router) # /dashboard/*
app.include_router(prediction_router) # /prediction/*

@app.on_event("startup")
async def startup_event():
    """Initialize database tables on startup."""
    ensure_users_table()
    ensure_predictions_table()

# HTML page routes
@app.get("/")
async def login_page(request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/dashboard")
async def dashboard_page(request):
    return templates.TemplateResponse("dashboard.html", {"request": request})
```

### Security Layer

**File: `app/core/security.py`**

#### Password Hashing

```python
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify password against bcrypt hash."""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Hash password using bcrypt."""
    return pwd_context.hash(password)
```

#### JWT Token Management

```python
from jose import jwt, JWTError

def create_access_token(data: dict, expires_delta=None) -> str:
    """
    Create JWT access token.
    
    Args:
        data: Payload (e.g., {"sub": username})
        expires_delta: Custom expiration time
    
    Returns:
        Encoded JWT string
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(
        minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES
    ))
    to_encode.update({"exp": expire})
    
    return jwt.encode(
        to_encode,
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM
    )

def decode_access_token(token: str) -> Optional[dict]:
    """
    Decode and validate JWT token.
    
    Returns:
        Decoded payload or None if invalid/expired
    """
    try:
        return jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM]
        )
    except JWTError:
        return None
```

### Authentication Dependency

**File: `app/api/deps.py`**

```python
from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)

async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    """
    FastAPI dependency that extracts and validates current user from JWT.
    
    Usage:
        @router.get("/protected")
        async def endpoint(current_user: dict = Depends(get_current_user)):
            ...
    
    Raises:
        CredentialsException: 401 if token missing/invalid
    """
    if not token:
        raise CredentialsException("Not authenticated")
    
    payload = decode_access_token(token)
    if payload is None:
        raise CredentialsException("Invalid or expired token")
    
    username = payload.get("sub")
    user = get_user_by_username(username)
    
    if user is None:
        raise CredentialsException("User not found")
    
    return user
```

---

## API Endpoints Reference

### Authentication (`/auth`)

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| POST | `/auth/register` | Create new user | No |
| POST | `/auth/login` | Login, get JWT token | No |
| POST | `/auth/token` | OAuth2 token endpoint (Swagger) | No |
| GET | `/auth/me` | Get current user info | Yes |

#### Register User

```http
POST /auth/register
Content-Type: application/json

{
    "username": "john_doe",
    "email": "john@example.com",
    "password": "securepass123"
}
```

**Response (201 Created):**
```json
{
    "id": 1,
    "username": "john_doe",
    "email": "john@example.com",
    "is_active": true,
    "created_at": "2026-01-29T14:30:00Z"
}
```

#### Login

```http
POST /auth/login
Content-Type: application/json

{
    "username": "john_doe",
    "password": "securepass123"
}
```

**Response:**
```json
{
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "bearer"
}
```

### Dashboard (`/dashboard`)

All dashboard endpoints require JWT authentication.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/dashboard/summary` | KPIs (players, goals, assists, avg points) |
| GET | `/dashboard/trends` | Gameweek performance trends |
| GET | `/dashboard/distributions` | Position-based distributions |
| GET | `/dashboard/top-players` | Top scoring players |
| POST | `/dashboard/search/players` | Global player search |
| GET | `/dashboard/filters` | Available filter options |
| GET | `/dashboard/teams/{team_id}/squad` | Team squad details |
| GET | `/dashboard/standings` | League standings with xG |
| GET | `/dashboard/players/{player_id}/trends` | Individual player trends |

#### Summary Stats

```http
GET /dashboard/summary?season=2024-25&team_id=1
Authorization: Bearer <token>
```

**Response:**
```json
{
    "total_players": 450,
    "total_teams": 20,
    "total_fixtures": 380,
    "total_gameweeks": 24,
    "avg_points_per_player": 52.3,
    "total_goals": 842,
    "total_assists": 656,
    "avg_player_value": 5.2
}
```

#### League Standings

```http
GET /dashboard/standings?season=2024-25
Authorization: Bearer <token>
```

**Response:**
```json
{
    "season": "2024-25",
    "standings": [
        {
            "rank": 1,
            "team_name": "Arsenal",
            "played": 24,
            "wins": 18,
            "draws": 4,
            "losses": 2,
            "goals_for": 52,
            "goals_against": 18,
            "goal_diff": 34,
            "points": 58,
            "clean_sheets": 0,
            "xG_for": 48.5,
            "xG_against": 22.1,
            "xPts": null
        },
        ...
    ]
}
```

### Predictions (`/prediction`)

All prediction endpoints require JWT authentication.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/prediction/best-players` | Get top predicted players |
| POST | `/prediction/refresh` | Regenerate all predictions |
| GET | `/prediction/player/{id}` | Single player prediction detail |
| GET | `/prediction/optimized-squad` | Budget-optimized starting XI |
| GET | `/prediction/positions/{pos}` | Top players by position |

#### Best Players Prediction

```http
GET /prediction/best-players?limit=15&position=MID&max_price=10.0&min_minutes=500
Authorization: Bearer <token>
```

**Response:**
```json
{
    "players": [
        {
            "player_id": 351,
            "player_name": "Mohamed Salah",
            "team_name": "Liverpool",
            "position": "MID",
            "now_cost": 13.5,
            "predicted_points": 8.4,
            "total_points": 186,
            "form": 7.2,
            "minutes": 1890,
            "goals": 15,
            "assists": 10,
            "points_per_million": 0.62,
            "confidence": "HIGH"
        },
        ...
    ],
    "total_budget_used": 125.3,
    "filters_applied": {
        "position": "MID",
        "max_price": 10.0,
        "min_minutes": 500,
        "limit": 15
    },
    "prediction_date": "2026-01-29",
    "model_info": "Weighted prediction model (form: 40%, efficiency: 30%, avg: 20%, goals: 10%)"
}
```

#### Optimized Squad

```http
GET /prediction/optimized-squad?max_budget=100&formation=3-4-3
Authorization: Bearer <token>
```

**Response:**
```json
{
    "squad": {
        "GKP": [{"player_id": 1, "player_name": "Pickford", "now_cost": 5.0, ...}],
        "DEF": [...],
        "MID": [...],
        "FWD": [...]
    },
    "formation": "3-4-3",
    "total_cost": 94.5,
    "budget_remaining": 5.5,
    "total_predicted_points": 62.8,
    "player_count": 11
}
```

---

## Dashboard Service

**File: `app/api/dashboard/service.py`**

The dashboard service provides analytics queries with season-aware schema configuration.

### Season Schema Configuration

**File: `app/api/dashboard/season_config.py`**

The `SeasonSchema` dataclass provides typed, safe access to season-specific table/column names.

```python
@dataclass(frozen=True)
class SeasonSchema:
    name: str                       # "2024-25"
    is_historical: bool             # True for past seasons
    
    # Table names
    table_fact: str                 # Primary fact table
    table_players: str              # Player dimension
    table_teams: str                # Team dimension
    table_fixtures: str             # Fixtures table
    
    # Column mappings
    col_player_id: str              # "player_id" or "element_id"
    col_player_table_id: str        # Primary key in player table
    col_team_id: str                # "id" or "team_id"
    col_gameweek: str               # "event" or "gameweek"
    col_team_name: str              # "name" or "team_name"
    
    # Capability flags
    supports_teams: bool            # Some seasons lack team data
    supports_understat: bool        # xG data availability
```

#### Historical vs Current Season

```python
def get_season_schema(season: str) -> SeasonSchema:
    if season == CURRENT_SEASON:  # "2024-25"
        return SeasonSchema(
            name=season,
            is_historical=False,
            table_fact="fact_player_gameweeks",
            table_players="players",
            table_teams="teams",
            col_player_id="player_id",     # FK in fact table
            col_player_table_id="id",      # PK in players table
            col_gameweek="event",
            ...
        )
    else:
        return SeasonSchema(
            name=season,
            is_historical=True,
            table_fact="fpl_player_gameweeks",
            table_players="fpl_season_players",
            table_teams="fpl_season_teams",
            col_player_id="element_id",
            col_player_table_id="element_id",
            col_gameweek="gameweek",
            ...
        )
```

### Key Service Functions

#### `get_summary_stats(team_id, season)`

Returns dashboard KPIs:
- Total players, teams, fixtures, gameweeks
- Sum of goals, assists, average points
- Average player value

#### `get_gameweek_trends(team_id, season)`

Returns gap-free gameweek trends using recursive CTE:

```sql
WITH RECURSIVE gw_axis AS (
    SELECT 1 as gw
    UNION ALL
    SELECT gw + 1 FROM gw_axis WHERE gw < {max_gw}
),
TrendFacts AS (
    SELECT gameweek as gw, SUM(total_points), SUM(goals_scored), ...
    FROM fpl_player_gameweeks
    GROUP BY gameweek
)
SELECT ax.gw, COALESCE(tf.pts, 0), ...
FROM gw_axis ax
LEFT JOIN TrendFacts tf ON ax.gw = tf.gw
ORDER BY ax.gw
```

#### `get_league_standings(season)`

Fetches from `clean_team_season_metrics` (pre-merged xG data):

```python
query = """
    SELECT team_name, played, wins, draws, losses,
           gf AS goals_for, ga AS goals_against,
           gd AS goal_diff, pts AS points,
           xg_for, xg_against
    FROM clean_team_season_metrics
    WHERE season = %s
    ORDER BY pts DESC, gd DESC
"""
```

#### `get_team_squad(team_id, season)`

Returns all players for a team with calculated metrics:
- Total points, goals, assists
- Points per 90 minutes
- Now cost

---

## Prediction System

**File: `app/api/prediction/service.py`**

### Prediction Formula

```python
def calculate_predicted_points(
    total_points, minutes, form, goals, assists, position, games_played
) -> float:
    """
    Weighted prediction formula:
    - Form (recent performance): 40%
    - Points per 90 efficiency: 30%
    - Season average: 20%
    - Goal threat bonus: 10%
    
    Position multipliers:
    - GKP: 0.9x
    - DEF: 0.95x
    - MID: 1.0x
    - FWD: 1.05x
    """
    pts_per_90 = total_points / (minutes / 90)
    avg_per_game = total_points / games_played
    
    predicted = (
        (form * 0.40) +
        (pts_per_90 * 0.30) +
        (avg_per_game * 0.20) +
        (((goals * 0.5) + (assists * 0.3)) * 0.10)
    ) * position_multiplier
    
    # Bound to realistic range (2-15 points)
    return max(2.0, min(predicted, 15.0))
```

### Confidence Levels

```python
# Based on minutes played and recent form
if minutes >= 1500 and form >= 5.0:
    confidence = "HIGH"
elif minutes >= 900 and form >= 3.0:
    confidence = "MEDIUM"
else:
    confidence = "LOW"
```

### Prediction Storage

**Table: `player_predicted_points`**

```sql
CREATE TABLE player_predicted_points (
    id INT AUTO_INCREMENT PRIMARY KEY,
    player_id INT NOT NULL,
    player_name VARCHAR(100),
    team_id INT,
    team_name VARCHAR(100),
    position VARCHAR(10),
    now_cost DECIMAL(5,1),
    predicted_points DECIMAL(6,2),
    total_points INT,
    form DECIMAL(5,2),
    minutes INT,
    goals INT,
    assists INT,
    points_per_million DECIMAL(6,2),
    prediction_date DATE,
    season VARCHAR(10),
    UNIQUE KEY unique_player_date (player_id, prediction_date)
);
```

### Budget Optimization Algorithm

**Function: `get_budget_optimized_squad(max_budget, formation)`**

Greedy algorithm for squad selection:

```python
def get_budget_optimized_squad(max_budget=100.0, formation="3-4-3"):
    # Parse formation
    required = {
        'GKP': 1,
        'DEF': int(formation.split('-')[0]),  # 3
        'MID': int(formation.split('-')[1]),  # 4
        'FWD': int(formation.split('-')[2])   # 3
    }
    
    total_cost = 0.0
    squad = {'GKP': [], 'DEF': [], 'MID': [], 'FWD': []}
    
    # For each position, select best value players
    for position, count in required.items():
        max_per_player = (max_budget - total_cost) / count
        
        # Query: ORDER BY points_per_million DESC
        candidates = get_top_value_players(position, max_per_player, count * 3)
        
        for player in candidates:
            if len(squad[position]) >= count:
                break
            if total_cost + player.cost <= max_budget:
                squad[position].append(player)
                total_cost += player.cost
    
    return {
        'squad': squad,
        'formation': formation,
        'total_cost': total_cost,
        'budget_remaining': max_budget - total_cost,
        'total_predicted_points': sum(p.predicted_points for p in all_players)
    }
```

---

## Machine Learning Models

**File: `Model/Best_Model_selection.py`**

### Model Training Pipeline

The model selection script trains and compares 8 different models:

1. **Traditional ML:**
   - Random Forest Regressor
   - Gradient Boosting Regressor
   - AdaBoost Regressor
   - XGBoost Regressor
   - MLP (Multi-Layer Perceptron)

2. **Deep Learning:**
   - CNN (1D Convolutional)
   - RNN (Simple Recurrent)
   - LSTM (Long Short-Term Memory)

### Data Preparation

```python
# Load clean tables
xg = pd.read_csv('clean_team_xg_season.csv')
stats = pd.read_csv('clean_team_season_stats.csv')
metrics = pd.read_csv('clean_team_season_metrics.csv')

# Feature Engineering
merged_data['xg_diff'] = merged_data['xg_for'] - merged_data['xg_against']
merged_data['goal_efficiency'] = merged_data['gf'] / (merged_data['xg_for'] + 0.1)
merged_data['defensive_efficiency'] = merged_data['ga'] / (merged_data['xg_against'] + 0.1)
merged_data['win_rate'] = merged_data['wins'] / merged_data['played']
merged_data['goals_per_game'] = merged_data['gf'] / merged_data['played']
merged_data['xg_for_per_game'] = merged_data['xg_for'] / merged_data['played']

# Target variable
target = 'pts'  # Team points
```

### Model Metrics

```python
def calculate_metrics(y_true, y_pred, scaler_y):
    """Calculate regression metrics."""
    y_true = scaler_y.inverse_transform(y_true)
    y_pred = scaler_y.inverse_transform(y_pred)
    
    return {
        'MAE': mean_absolute_error(y_true, y_pred),
        'RMSE': sqrt(mean_squared_error(y_true, y_pred)),
        'R2': r2_score(y_true, y_pred),
        'MAPE': mean_absolute_percentage_error(y_true, y_pred) * 100
    }
```

### Model Persistence

```python
# Save best model with metadata
model_package = {
    'model': best_model_obj,
    'model_name': best_model_name,
    'scaler_X': scaler_X,
    'scaler_y': scaler_y,
    'feature_cols': feature_cols,
    'metrics': {
        'MAE': results[best_model]['MAE'],
        'RMSE': results[best_model]['RMSE'],
        'R2': results[best_model]['R2'],
        'MAPE': results[best_model]['MAPE']
    }
}

# Save to pickle
with open('best_model.pkl', 'wb') as f:
    pickle.dump(model_package, f)
```

### Loading the Model

```python
with open('Model/best_model.pkl', 'rb') as f:
    model_package = pickle.load(f)

model = model_package['model']
scaler_X = model_package['scaler_X']
scaler_y = model_package['scaler_y']

# Make predictions
X_new_scaled = scaler_X.transform(X_new)
y_pred_scaled = model.predict(X_new_scaled)
y_pred = scaler_y.inverse_transform(y_pred_scaled)
```

---

## Frontend Architecture

### JavaScript State Management

**File: `app/static/js/dashboard.js`**

```javascript
let state = {
    view: 'overview',      // Current active view
    season: '2024-25',     // Selected season
    team_id: '',           // Selected team filter
    token: localStorage.getItem('fpl_token'),
    charts: {},            // Chart.js instances (for destroy/recreate)
    search: {
        name: '',
        pos: '',
        sort: 'total_points'
    }
};
```

### API Gateway

```javascript
async function apiCall(endpoint, method = 'GET', body = null) {
    const options = {
        method,
        headers: {
            'Authorization': `Bearer ${state.token}`,
            'Content-Type': 'application/json',
        }
    };
    
    if (body) options.body = JSON.stringify(body);
    
    // Append season parameter to GET requests
    const url = new URL(`${window.location.origin}${endpoint}`);
    if (method === 'GET' && !url.searchParams.has('season')) {
        url.searchParams.set('season', state.season);
    }
    
    const response = await fetch(url.toString(), options);
    
    // Handle 401 - redirect to login
    if (response.status === 401) {
        localStorage.removeItem('fpl_token');
        window.location.href = '/';
        return null;
    }
    
    return response.json();
}
```

### View Orchestration

```javascript
function setView(viewId) {
    state.view = viewId;
    
    // Toggle nav active states
    document.querySelectorAll('.nav-item')
        .forEach(i => i.classList.toggle('active', i.dataset.view === viewId));
    
    // Toggle view sections
    document.querySelectorAll('.view-section')
        .forEach(s => s.classList.toggle('d-none', s.id !== `view-${viewId}`));
    
    // Update title
    const titles = {
        overview: ['Dashboard Overview', 'Key Performance Indicators'],
        trends: ['Performance Trends', 'Gap-free volatility tracking'],
        discovery: ['Player Discovery', 'Global lookup across all teams'],
        teams: ['Squad Analytics', 'Team productivity matrix'],
        standings: ['League Standings', 'Performance-based table'],
        predictions: ['AI Predictions', 'ML-powered recommendations']
    };
    
    document.getElementById('viewTitle').textContent = titles[viewId][0];
    document.getElementById('viewSubtitle').textContent = titles[viewId][1];
    
    refreshData();
}
```

### Data Loading

```javascript
async function refreshData() {
    const loader = document.getElementById('loadingState');
    const content = document.getElementById('dashboardContent');
    
    loader.classList.remove('d-none');
    content.classList.add('d-none');
    
    try {
        switch(state.view) {
            case 'overview':    await loadOverview(); break;
            case 'trends':      await loadTrends(); break;
            case 'discovery':   await loadDiscovery(); break;
            case 'teams':       await loadTeams(); break;
            case 'standings':   await loadStandings(); break;
            case 'predictions': await loadPredictions(); break;
        }
    } catch (e) {
        console.error("Data Link Failure:", e);
    } finally {
        loader.classList.add('d-none');
        content.classList.remove('d-none');
    }
}
```

### Views

| View | Function | API Endpoints | Features |
|------|----------|--------------|----------|
| Overview | `loadOverview()` | `/summary`, `/trends`, `/distributions` | KPI cards, trend chart, position distribution |
| Trends | `loadTrends()` | `/trends` | Volatility chart with gap-filling |
| Discovery | `loadDiscovery()` | `/search/players` (POST) | Global player search, sortable |
| Teams | `loadTeams()` | `/teams/{id}/squad` | Squad table, pts/90 metrics |
| Standings | `loadStandings()` | `/standings` | League table with xG columns |
| Predictions | `loadPredictions()` | `/prediction/best-players` | AI picks table with confidence badges |

---

## Logging System

### Log Configuration

All modules use the centralized logging configuration:

```python
from Utils.logging_config import get_logger

logger = get_logger("module_name")
```

### Log Format

```
[YYYY-MM-DD HH:MM:SS,mmm] LEVEL | module_name | message
```

**Examples:**
```
[2026-01-29 15:30:22,123] INFO | player_snapshot | Player snapshot job started
[2026-01-29 15:30:25,456] INFO | db | Successfully upserted 547 rows into players
[2026-01-29 15:30:26,789] ERROR | http | Temporary server error 503 while requesting /api/bootstrap-static/
[2026-01-29 15:30:28,012] WARNING | retry | Attempt 1 failed: Connection timed out
[2026-01-29 15:30:32,345] INFO | retry | Retrying in 4.27s...
```

### Log Levels

| Level | Usage |
|-------|-------|
| DEBUG | Detailed diagnostic info (not typically enabled) |
| INFO | Normal operation events |
| WARNING | Recoverable issues, degraded functionality |
| ERROR | Failures that prevent specific operations |
| CRITICAL | System-wide failures |

### Log File Location

```
logs/pipeline.log
```

---

## Conventions & Best Practices

### Code Organization

1. **Module Structure**: Each feature area has its own directory with:
   - `router.py` - FastAPI route definitions
   - `service.py` - Business logic
   - `schemas.py` - Pydantic models

2. **Import Order**:
   ```python
   # Standard library
   from datetime import datetime
   
   # Third-party
   from fastapi import APIRouter
   import pandas as pd
   
   # Local application
   from app.core.config import settings
   from Utils.logging_config import get_logger
   ```

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Files | snake_case | `player_snapshot.py` |
| Classes | PascalCase | `SeasonSchema` |
| Functions | snake_case | `get_best_players()` |
| Constants | UPPER_SNAKE | `CURRENT_SEASON` |
| Database Tables | snake_case | `clean_team_season_metrics` |
| API Endpoints | kebab-case | `/best-players` |

### Error Handling Pattern

```python
try:
    result = risky_operation()
    logger.info(f"Operation succeeded: {result}")
except SpecificError as e:
    logger.warning(f"Recoverable error: {e}")
    return default_value
except Exception as e:
    logger.exception("Unexpected error")
    raise
```

### Database Patterns

1. **Always use parameterized queries:**
   ```python
   # GOOD
   cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
   
   # BAD - SQL injection risk
   cursor.execute(f"SELECT * FROM users WHERE id = {user_id}")
   ```

2. **Use context managers for connections:**
   ```python
   with get_db_connection() as conn:
       # Connection auto-closed on exit
       cursor = conn.cursor()
       ...
   ```

3. **Batch operations for large datasets:**
   ```python
   for i in range(0, len(records), BATCH_SIZE):
       batch = records[i:i+BATCH_SIZE]
       cursor.executemany(sql, batch)
       conn.commit()
   ```

### API Response Patterns

1. **Always use Pydantic schemas for validation:**
   ```python
   @router.post("/", response_model=UserResponse)
   async def create_user(user: UserCreate):
       ...
   ```

2. **Include proper status codes:**
   ```python
   return JSONResponse(content=result, status_code=201)  # Created
   raise HTTPException(status_code=404, detail="Not found")
   ```

3. **Handle authentication consistently:**
   ```python
   @router.get("/protected")
   async def protected_endpoint(
       current_user: dict = Depends(get_current_user)
   ):
       ...
   ```

---

## Pipeline Execution

### Running the Full Pipeline

**Windows:**
```cmd
cd C:\path\to\FPL-Prediction
run_pipeline.bat
```

**Unix/Linux/Mac:**
```bash
cd /path/to/FPL-Prediction
chmod +x run_pipeline.sh
./run_pipeline.sh
```

### Running Individual Scripts

```bash
# Activate virtual environment first
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Unix

# Set PYTHONPATH
set PYTHONPATH=%cd%  # Windows
export PYTHONPATH=$(pwd)  # Unix

# Run individual scripts
python Scripts/player_snapshot.py
python Scripts/build_fact_table.py
python Scripts/ingest_fpl_github.py
```

### Running the Web Application

```bash
# Development (with auto-reload)
uvicorn app.main:app --reload --port 8000

# Production
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

### API Documentation

Once running, access:
- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc

---

## Summary

This FPL Prediction system provides:

1. **Automated Data Ingestion**: Multi-source data collection from FPL API, GitHub history, and Kaggle
2. **Clean Data Architecture**: Properly normalized tables with pre-merged analytics
3. **RESTful API**: JWT-secured endpoints for all dashboard features
4. **AI Predictions**: Weighted prediction algorithm with confidence scoring
5. **Squad Optimization**: Budget-constrained team selection
6. **Interactive Dashboard**: Real-time analytics with Chart.js visualizations
7. **Robust Error Handling**: Hierarchical exceptions with retry logic
8. **Comprehensive Logging**: Unified logging across all components

---

*Documentation generated on: 2026-01-29*
*Version: 1.0.0*
