"""Scraper for validating player IDs and profiles."""

import logging
import re
from typing import Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class PlayerScraper:
    """Validates and extracts player profile information."""
    
    def __init__(self, driver: webdriver.Chrome):
        """
        Initialize player scraper.
        
        Args:
            driver: Selenium WebDriver instance
        """
        self.driver = driver
    
    @retry_with_backoff(max_retries=3)
    def validate_player(self, player_url: str, player_name: str) -> bool:
        """
        Validate that player profile exists and is accessible.
        
        Args:
            player_url: Player profile URL
            player_name: Player name for logging
            
        Returns:
            True if player profile is valid
        """
        logger.info(f"Validating player: {player_name}")
        
        try:
            self.driver.get(player_url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            
            # Check if we got a valid player page
            page_source = self.driver.page_source.lower()
            
            if "page not found" in page_source or "404" in page_source:
                logger.warning(f"Player page not found: {player_name}")
                return False
            
            logger.info(f"Player validated successfully: {player_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to validate player {player_name}: {e}")
            return False
    
    @staticmethod
    def extract_player_id_from_url(url: str) -> Optional[str]:
        """
        Extract player ID from FBref URL.
        
        Args:
            url: Player profile URL
            
        Returns:
            Player ID or None if not found
        """
        pattern = r'/players/([a-f0-9]+)/'
        match = re.search(pattern, url)
        
        if match:
            return match.group(1)
        
        return None
    
    @staticmethod
    def normalize_player_name_for_url(name: str) -> str:
        """
        Normalize player name for URL construction.
        
        Args:
            name: Player name
            
        Returns:
            URL-safe player name
        """
        # Replace spaces with hyphens
        name = name.strip().replace(' ', '-')
        
        # Remove special characters except hyphens
        name = re.sub(r'[^\w\-]', '', name)
        
        return name