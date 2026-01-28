"""
Scraper for extracting season-wise match logs for players.
Uses direct URL construction to avoid UI interaction.
"""

import time
import pandas as pd
from typing import Optional, List
from io import StringIO

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from Utils.logging_config import get_logger
from Utils.retry import retry_request
from scraping import config

logger = get_logger("matchlog_scraper")


def build_matchlog_url(player_id: str, season: str) -> str:
    """
    Build direct URL to player's match log for a specific season.
    
    Args:
        player_id: Player's FBref ID (e.g., "1c7012b8")
        season: Season string (e.g., "2023-2024")
        
    Returns:
        Full URL to match log page
    """
    return f"{config.FBREF_BASE_URL}/en/players/{player_id}/matchlogs/{season}/summary"


def normalize_column_names(columns: List) -> List[str]:
    """
    Normalize multi-level column headers from FBref tables.
    
    FBref uses multi-level headers like ('Performance', 'Gls').
    We flatten them to 'Performance_Gls'.
    
    Args:
        columns: Column names (could be tuples or strings)
        
    Returns:
        Flattened column name list
    """
    normalized = []
    
    for col in columns:
        if isinstance(col, tuple):
            # Join non-empty parts with underscore
            parts = [str(c).strip() for c in col if c and str(c).strip()]
            normalized.append('_'.join(parts))
        else:
            normalized.append(str(col).strip())
    
    return normalized


def scrape_season_matchlog(
    driver,
    player_id: str,
    player_name: str,
    season: str
) -> Optional[pd.DataFrame]:
    """
    Scrape match log data for a player in a specific season.
    
    Args:
        driver: Selenium WebDriver instance
        player_id: Player's FBref ID
        player_name: Player's name (for logging)
        season: Season string (e.g., "2023-2024")
        
    Returns:
        DataFrame with match log data, or None if season doesn't exist
    """
    url = build_matchlog_url(player_id, season)
    logger.info(f"Scraping {player_name} - {season}")
    
    def _fetch_and_parse():
        driver.get(url)
        
        # Wait for page to load
        time.sleep(2)
        
        page_source = driver.page_source
        
        # Check if page exists (look for 404 or no data indicators)
        if "Page not found" in page_source or "404" in page_source:
            logger.debug(f"Season {season} not found for {player_name}")
            return None
        
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Look for match log table (usually id contains "matchlogs")
        table = soup.find('table', {'id': lambda x: x and 'matchlogs' in x.lower()})
        
        if not table:
            # Try alternative: find any stats table
            table = soup.find('table', {'class': 'stats_table'})
        
        if not table:
            logger.debug(f"No match log table found for {player_name} in {season}")
            return None
        
        # Parse table with pandas
        try:
            # Convert table to HTML string
            table_html = str(table)
            
            # Read with pandas
            dfs = pd.read_html(StringIO(table_html))
            
            if not dfs or len(dfs) == 0:
                logger.debug(f"No data in table for {player_name} - {season}")
                return None
            
            df = dfs[0]
            
            # Handle multi-level columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = normalize_column_names(df.columns)
            
            # Basic validation: check if we have rows
            if len(df) == 0:
                logger.debug(f"Empty match log for {player_name} - {season}")
                return None
            
            # Add metadata
            df['player_name'] = player_name
            df['player_id'] = player_id
            df['season'] = season
            
            logger.info(f"✓ Scraped {len(df)} matches for {player_name} - {season}")
            return df
            
        except Exception as e:
            logger.warning(f"Failed to parse table for {player_name} - {season}: {e}")
            return None
    
    try:
        result = retry_request(
            _fetch_and_parse,
            retries=config.MAX_RETRIES,
            backoff=config.RETRY_BACKOFF
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to scrape {player_name} - {season}: {e}")
        return None


def scrape_all_seasons(
    driver,
    player_id: str,
    player_name: str,
    seasons: List[str]
) -> List[pd.DataFrame]:
    """
    Scrape match logs for a player across multiple seasons.
    
    Args:
        driver: Selenium WebDriver instance
        player_id: Player's FBref ID
        player_name: Player's name
        seasons: List of season strings to scrape
        
    Returns:
        List of DataFrames (one per successful season)
    """
    all_data = []
    
    for season in seasons:
        try:
            df = scrape_season_matchlog(driver, player_id, player_name, season)
            
            if df is not None:
                all_data.append(df)
            
            # Rate limiting between requests
            time.sleep(config.REQUEST_DELAY)
            
        except Exception as e:
            logger.warning(f"Season {season} failed for {player_name}: {e}")
            continue
    
    logger.info(f"Completed {player_name}: {len(all_data)}/{len(seasons)} seasons scraped")
    return all_data