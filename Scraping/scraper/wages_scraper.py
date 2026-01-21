"""Scraper for extracting player links from FBref wages page."""

import logging
import re
from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class WagesScraper:
    """Scrapes player information from Premier League wages page."""
    
    def __init__(self, driver: webdriver.Chrome):
        """
        Initialize wages scraper.
        
        Args:
            driver: Selenium WebDriver instance
        """
        self.driver = driver
    
    @retry_with_backoff(max_retries=3)
    def scrape_players(self, url: str) -> List[Dict[str, str]]:
        """
        Scrape player information from wages page.
        
        Args:
            url: Wages page URL
            
        Returns:
            List of player dictionaries with name, url, and id
        """
        logger.info(f"Scraping players from: {url}")
        
        try:
            self.driver.get(url)
            
            # Wait for table to load
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            players = self._extract_players(soup)
            
            logger.info(f"Successfully extracted {len(players)} players")
            return players
            
        except Exception as e:
            logger.error(f"Failed to scrape players: {e}")
            raise
    
    def _extract_players(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """
        Extract player information from parsed HTML.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            List of player dictionaries
        """
        players = []
        
        # Find wages table
        table = soup.find('table', {'id': 'player_wages'})
        
        if not table:
            logger.warning("Wages table not found on page")
            return players
        
        tbody = table.find('tbody')
        if not tbody:
            logger.warning("Table body not found")
            return players
        
        rows = tbody.find_all('tr')
        logger.info(f"Found {len(rows)} rows in wages table")
        
        for row in rows:
            try:
                player_data = self._extract_player_from_row(row)
                if player_data:
                    players.append(player_data)
            except Exception as e:
                logger.warning(f"Failed to extract player from row: {e}")
                continue
        
        return players
    
    def _extract_player_from_row(self, row) -> Optional[Dict[str, str]]:
        """
        Extract player data from table row.
        
        Args:
            row: BeautifulSoup table row element
            
        Returns:
            Player dictionary or None if extraction fails
        """
        # Find player link
        player_cell = row.find('th', {'data-stat': 'player'})
        if not player_cell:
            return None
        
        player_link = player_cell.find('a')
        if not player_link:
            return None
        
        player_name = player_link.text.strip()
        player_url = player_link.get('href', '')
        
        if not player_url:
            logger.warning(f"No URL found for player: {player_name}")
            return None
        
        # Extract player_id from URL
        # URL format: /en/players/1c7012b8/Mohamed-Salah
        player_id = self._extract_player_id(player_url)
        
        if not player_id:
            logger.warning(f"Could not extract player_id for: {player_name}")
            return None
        
        # Construct full URL
        if not player_url.startswith('http'):
            player_url = f"https://fbref.com{player_url}"
        
        return {
            'player_name': player_name,
            'player_url': player_url,
            'player_id': player_id
        }
    
    @staticmethod
    def _extract_player_id(url: str) -> Optional[str]:
        """
        Extract player ID from FBref URL.
        
        Args:
            url: Player profile URL
            
        Returns:
            Player ID or None if not found
        """
        # Pattern: /en/players/{player_id}/{player-name}
        pattern = r'/players/([a-f0-9]+)/'
        match = re.search(pattern, url)
        
        if match:
            return match.group(1)
        
        return None