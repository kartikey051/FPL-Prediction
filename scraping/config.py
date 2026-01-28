"""
Configuration constants for FBref scraping.
"""

# Base URLs
FBREF_BASE_URL = "https://fbref.com"
WAGES_URL = f"{FBREF_BASE_URL}/en/comps/9/wages/Premier-League-Wages"

# Seasons to scrape (format: "2015-2016")
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

# Output paths
OUTPUT_DIR = "output/players"
STATE_DIR = "state/scraper"

# Selenium settings
HEADLESS = True
PAGE_LOAD_TIMEOUT = 30
IMPLICIT_WAIT = 10

# Retry settings
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0

# Rate limiting
REQUEST_DELAY = 2.0  # seconds between requests