"""
Scraper for extracting player links from FBref wages page.
"""

import re
import time
from typing import List, Dict
from urllib.parse import urljoin

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from Utils.logging_config import get_logger
from Utils.retry import retry_request
from scraping import config

logger = get_logger("wages_scraper")


def extract_player_id(url: str) -> str:
    """
    Extract player ID from FBref URL.
    
    Example: /en/players/1c7012b8/Erling-Haaland -> 1c7012b8
    
    Args:
        url: Player profile URL
        
    Returns:
        Player ID string
    """
    match = re.search(r'/players/([a-f0-9]+)/', url)
    if match:
        return match.group(1)
    raise ValueError(f"Could not extract player_id from URL: {url}")


def scrape_players(driver) -> List[Dict[str, str]]:
    """
    Scrape player information from the Premier League wages page.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        List of dicts containing:
        - player_name: str
        - player_url: str (full URL)
        - player_id: str
    """
    logger.info("Starting player discovery from wages page")
    
    def _fetch_page():
        driver.get(config.WAGES_URL)
        
        # Wait for table to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        
        time.sleep(2)  # Additional wait for JavaScript rendering
        return driver.page_source
    
    try:
        # Use retry mechanism for page load
        page_source = retry_request(
            _fetch_page,
            retries=config.MAX_RETRIES,
            backoff=config.RETRY_BACKOFF
        )
        
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Find all player links in the wages table
        players = []
        table = soup.find('table', {'id': 'player_wages'})
        
        if not table:
            logger.warning("Could not find player_wages table")
            return []
        
        # Find all player links within the table
        player_links = table.find_all('a', href=re.compile(r'/players/[a-f0-9]+/'))
        
        seen_ids = set()
        
        for link in player_links:
            try:
                player_name = link.get_text(strip=True)
                relative_url = link.get('href')
                
                if not player_name or not relative_url:
                    continue
                
                # Build full URL
                player_url = urljoin(config.FBREF_BASE_URL, relative_url)
                
                # Extract player ID
                player_id = extract_player_id(relative_url)
                
                # Avoid duplicates (some players appear multiple times)
                if player_id in seen_ids:
                    continue
                
                seen_ids.add(player_id)
                
                players.append({
                    'player_name': player_name,
                    'player_url': player_url,
                    'player_id': player_id
                })
                
                logger.debug(f"Found player: {player_name} ({player_id})")
                
            except Exception as e:
                logger.warning(f"Failed to parse player link: {e}")
                continue
        
        logger.info(f"Discovered {len(players)} unique players")
        return players
        
    except Exception as e:
        logger.error(f"Failed to scrape players from wages page: {e}")
        raise