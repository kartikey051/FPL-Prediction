"""Configuration for FBref scraper."""

# URLs
WAGES_URL = "https://fbref.com/en/comps/9/wages/Premier-League-Wages"
MATCHLOG_URL_TEMPLATE = "https://fbref.com/en/players/{player_id}/matchlogs/{season}/{player_name}-Match-Logs"

# Seasons to scrape
SEASONS = [
    "2015-2016",
    "2016-2017",
    "2017-2018",
    "2018-2019",
    "2019-2020",
    "2020-2021",
    "2021-2022",
    "2022-2023",
    "2023-2024",
    "2024-2025",
    "2025-2026",
]

# Paths
OUTPUT_DIR = "output/players"
STATE_FILE = "output/scraper_state.json"
DB_PATH = "output/scraper.db"

# Selenium settings
HEADLESS = True
PAGE_LOAD_TIMEOUT = 30
IMPLICIT_WAIT = 10

# Retry settings (used by utils.retry)
MAX_RETRIES = 3
BASE_DELAY = 2
MAX_DELAY = 10