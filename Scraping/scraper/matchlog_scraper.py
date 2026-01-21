"""Scraper for extracting player match logs by season."""

import logging
import pandas as pd
from typing import Optional, List, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class MatchLogScraper:
    """Scrapes match logs for a player across seasons."""
    
    def __init__(self, driver: webdriver.Chrome):
        """
        Initialize match log scraper.
        
        Args:
            driver: Selenium WebDriver instance
        """
        self.driver = driver
    
    @retry_with_backoff(max_retries=3)
    def scrape_season(
        self, 
        player_id: str, 
        player_name: str, 
        season: str,
        url_template: str
    ) -> Optional[pd.DataFrame]:
        """
        Scrape match logs for a specific season.
        
        Args:
            player_id: Player ID
            player_name: Player name
            season: Season string (e.g., "2023-2024")
            url_template: URL template for match logs
            
        Returns:
            DataFrame with match logs or None if not available
        """
        # Normalize player name for URL
        url_name = self._normalize_name_for_url(player_name)
        
        # Construct URL
        url = url_template.format(
            player_id=player_id,
            season=season,
            player_name=url_name
        )
        
        logger.info(f"Scraping {player_name} - {season}: {url}")
        
        try:
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Check if page exists
            if not self._is_valid_page():
                logger.warning(f"No data for {player_name} - {season}")
                return None
            
            # Parse match log table
            df = self._extract_match_logs()
            
            if df is None or df.empty:
                logger.warning(f"No match logs found for {player_name} - {season}")
                return None
            
            # Add metadata
            df['player_name'] = player_name
            df['player_id'] = player_id
            df['season'] = season
            
            logger.info(f"Extracted {len(df)} matches for {player_name} - {season}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to scrape {player_name} - {season}: {e}")
            raise
    
    def _is_valid_page(self) -> bool:
        """
        Check if page exists and has data.
        
        Returns:
            True if page is valid
        """
        page_source = self.driver.page_source.lower()
        
        # Check for error indicators
        if any(x in page_source for x in ["page not found", "404", "no data available"]):
            return False
        
        # Check for match log table
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        table = soup.find('table', {'id': 'matchlogs_all'})
        
        return table is not None
    
    def _extract_match_logs(self) -> Optional[pd.DataFrame]:
        """
        Extract match logs from current page.
        
        Returns:
            DataFrame with match logs or None
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        # Find match logs table
        table = soup.find('table', {'id': 'matchlogs_all'})
        
        if not table:
            logger.warning("Match logs table not found")
            return None
        
        try:
            # Parse table with pandas
            df = pd.read_html(str(table))[0]
            
            # Handle multi-level columns
            df = self._normalize_columns(df)
            
            # Clean data
            df = self._clean_dataframe(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to parse match log table: {e}")
            return None
    
    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize multi-level column headers.
        
        Args:
            df: DataFrame with potentially multi-level columns
            
        Returns:
            DataFrame with flattened columns
        """
        if isinstance(df.columns, pd.MultiIndex):
            # Flatten multi-level columns
            df.columns = ['_'.join(str(col).strip() for col in columns if str(col) != 'nan' and str(col).strip())
                         for columns in df.columns.values]
        
        # Clean column names
        df.columns = [col.strip().replace(' ', '_').lower() for col in df.columns]
        
        return df
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate DataFrame.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        # Remove header rows that sometimes appear in the data
        if 'date' in df.columns:
            df = df[df['date'] != 'Date']
        
        # Remove empty rows
        df = df.dropna(how='all')
        
        # Reset index
        df = df.reset_index(drop=True)
        
        return df
    
    @staticmethod
    def _normalize_name_for_url(name: str) -> str:
        """
        Normalize player name for URL.
        
        Args:
            name: Player name
            
        Returns:
            URL-safe name
        """
        import re
        
        # Replace spaces with hyphens
        name = name.strip().replace(' ', '-')
        
        # Remove special characters except hyphens
        name = re.sub(r'[^\w\-]', '', name)
        
        return name
    
    def combine_seasons(self, season_dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Combine multiple season DataFrames.
        
        Args:
            season_dfs: List of season DataFrames
            
        Returns:
            Combined DataFrame
        """
        if not season_dfs:
            return pd.DataFrame()
        
        try:
            combined = pd.concat(season_dfs, ignore_index=True)
            logger.info(f"Combined {len(season_dfs)} seasons into {len(combined)} total matches")
            return combined
            
        except Exception as e:
            logger.error(f"Failed to combine seasons: {e}")
            return pd.DataFrame()