"""
Scraper for validating player pages and extracting player IDs.
This module ensures player URLs are valid before scraping match logs.
"""

import time
from typing import Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from Utils.logging_config import get_logger
from Utils.retry import retry_request
from scraping import config

logger = get_logger("player_scraper")


def validate_player_page(driver, player_url: str, player_name: str) -> bool:
    """
    Validate that a player page exists and is accessible.
    
    Args:
        driver: Selenium WebDriver instance
        player_url: Full URL to player page
        player_name: Player name for logging
        
    Returns:
        True if page is valid, False otherwise
    """
    logger.debug(f"Validating player page for {player_name}")
    
    def _check_page():
        driver.get(player_url)
        
        # Wait for page to load - look for player info or tables
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "h1"))
        )
        
        time.sleep(1)
        
        # Check if we got a valid player page (not a 404 or error)
        page_source = driver.page_source.lower()
        
        # Simple validation: check for error indicators
        if "page not found" in page_source or "404" in page_source:
            return False
        
        return True
    
    try:
        result = retry_request(
            _check_page,
            retries=2,  # Fewer retries for validation
            backoff=1.5
        )
        
        if result:
            logger.debug(f"Player page validated for {player_name}")
        else:
            logger.warning(f"Invalid player page for {player_name}")
        
        return result
        
    except Exception as e:
        logger.warning(f"Failed to validate player page for {player_name}: {e}")
        return False


def get_player_info(driver, player_url: str) -> Optional[dict]:
    """
    Extract basic player information from their profile page.
    
    Args:
        driver: Selenium WebDriver instance
        player_url: Full URL to player page
        
    Returns:
        Dict with player info or None if extraction fails
    """
    try:
        driver.get(player_url)
        
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "h1"))
        )
        
        time.sleep(1)
        
        # Extract player name from h1
        h1_element = driver.find_element(By.TAG_NAME, "h1")
        player_name = h1_element.text.strip()
        
        info = {
            'player_name': player_name,
            'player_url': player_url
        }
        
        logger.debug(f"Extracted info for {player_name}")
        return info
        
    except Exception as e:
        logger.warning(f"Failed to extract player info from {player_url}: {e}")
        return None