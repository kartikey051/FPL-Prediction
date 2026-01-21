"""Main orchestrator for FBref scraping system."""

import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logging_config import setup_logging
from utils.state import StateManager
from utils.db import DatabaseManager

import config
from driver_factory import create_driver, close_driver
from scrapers.wages_scraper import WagesScraper
from scrapers.player_scraper import PlayerScraper
from scrapers.matchlog_scraper import MatchLogScraper

logger = logging.getLogger(__name__)


class FBrefScraper:
    """Main orchestrator for FBref scraping."""
    
    def __init__(self):
        """Initialize scraper components."""
        # Setup logging
        setup_logging()
        logger.info("="*80)
        logger.info("FBref Premier League Scraper Initialized")
        logger.info("="*80)
        
        # Initialize utilities
        self.state_manager = StateManager(config.STATE_FILE)
        self.db_manager = DatabaseManager(config.DB_PATH)
        
        # Initialize Selenium driver
        self.driver = None
        
        # Create output directory
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        logger.info(f"Output directory: {config.OUTPUT_DIR}")
    
    def run(self):
        """Execute full scraping workflow."""
        try:
            # Initialize driver
            self.driver = create_driver(
                headless=config.HEADLESS,
                page_load_timeout=config.PAGE_LOAD_TIMEOUT
            )
            
            # Initialize scrapers
            wages_scraper = WagesScraper(self.driver)
            player_scraper = PlayerScraper(self.driver)
            matchlog_scraper = MatchLogScraper(self.driver)
            
            # Step 1: Discover players
            logger.info("Step 1: Discovering players from wages page")
            players = self._get_players(wages_scraper)
            
            if not players:
                logger.error("No players found. Exiting.")
                return
            
            logger.info(f"Found {len(players)} players to scrape")
            
            # Step 2: Scrape match logs for each player
            logger.info("Step 2: Scraping match logs for all players")
            self._scrape_all_players(players, matchlog_scraper)
            
            logger.info("="*80)
            logger.info("Scraping completed successfully")
            logger.info("="*80)
            
        except KeyboardInterrupt:
            logger.warning("Scraping interrupted by user")
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}", exc_info=True)
            
        finally:
            # Cleanup
            close_driver(self.driver)
            logger.info("Scraper shutdown complete")
    
    def _get_players(self, wages_scraper: WagesScraper) -> List[Dict[str, str]]:
        """
        Get list of players to scrape.
        
        Args:
            wages_scraper: WagesScraper instance
            
        Returns:
            List of player dictionaries
        """
        try:
            players = wages_scraper.scrape_players(config.WAGES_URL)
            logger.info(f"Successfully discovered {len(players)} players")
            return players
            
        except Exception as e:
            logger.error(f"Failed to discover players: {e}")
            return []
    
    def _scrape_all_players(
        self, 
        players: List[Dict[str, str]], 
        matchlog_scraper: MatchLogScraper
    ):
        """
        Scrape match logs for all players.
        
        Args:
            players: List of player dictionaries
            matchlog_scraper: MatchLogScraper instance
        """
        total = len(players)
        
        for idx, player in enumerate(players, 1):
            player_name = player['player_name']
            player_id = player['player_id']
            
            logger.info(f"\n[{idx}/{total}] Processing: {player_name}")
            
            try:
                # Check if already completed
                state = self.state_manager.load_state()
                player_state = state.get('players', {}).get(player_id, {})
                
                if player_state.get('completed', False):
                    logger.info(f"Player {player_name} already completed. Skipping.")
                    continue
                
                # Get last scraped season for resume capability
                last_season = player_state.get('last_scraped_season')
                seasons_to_scrape = self._get_seasons_to_scrape(last_season)
                
                # Scrape seasons
                success = self._scrape_player_seasons(
                    player_id,
                    player_name,
                    seasons_to_scrape,
                    matchlog_scraper
                )
                
                if success:
                    # Mark as completed
                    self._update_player_state(player_id, player_name, completed=True)
                    logger.info(f"✓ Completed {player_name}")
                else:
                    logger.warning(f"⚠ Partial completion for {player_name}")
                
            except Exception as e:
                logger.error(f"Failed to process {player_name}: {e}")
                self._log_player_failure(player_id, player_name, str(e))
                continue
    
    def _scrape_player_seasons(
        self,
        player_id: str,
        player_name: str,
        seasons: List[str],
        matchlog_scraper: MatchLogScraper
    ) -> bool:
        """
        Scrape all seasons for a player.
        
        Args:
            player_id: Player ID
            player_name: Player name
            seasons: List of seasons to scrape
            matchlog_scraper: MatchLogScraper instance
            
        Returns:
            True if all seasons scraped successfully
        """
        season_dfs = []
        all_success = True
        
        for season in seasons:
            try:
                df = matchlog_scraper.scrape_season(
                    player_id=player_id,
                    player_name=player_name,
                    season=season,
                    url_template=config.MATCHLOG_URL_TEMPLATE
                )
                
                if df is not None and not df.empty:
                    season_dfs.append(df)
                    self._update_player_state(player_id, player_name, last_season=season)
                    logger.info(f"  ✓ {season}: {len(df)} matches")
                    
                    # Update DB
                    self.db_manager.log_scrape(
                        player_id=player_id,
                        player_name=player_name,
                        season=season,
                        status='success',
                        records_scraped=len(df)
                    )
                else:
                    logger.info(f"  - {season}: No data")
                    self.db_manager.log_scrape(
                        player_id=player_id,
                        player_name=player_name,
                        season=season,
                        status='no_data',
                        records_scraped=0
                    )
                
            except Exception as e:
                logger.warning(f"  ✗ {season}: {e}")
                all_success = False
                
                self.db_manager.log_scrape(
                    player_id=player_id,
                    player_name=player_name,
                    season=season,
                    status='error',
                    error_message=str(e)
                )
                
                continue
        
        # Save combined data
        if season_dfs:
            self._save_player_data(player_name, season_dfs, matchlog_scraper)
            return True
        
        return all_success
    
    def _save_player_data(
        self, 
        player_name: str, 
        season_dfs: List[pd.DataFrame],
        matchlog_scraper: MatchLogScraper
    ):
        """
        Save combined player data to CSV.
        
        Args:
            player_name: Player name
            season_dfs: List of season DataFrames
            matchlog_scraper: MatchLogScraper instance
        """
        try:
            combined_df = matchlog_scraper.combine_seasons(season_dfs)
            
            if combined_df.empty:
                logger.warning(f"No data to save for {player_name}")
                return
            
            # Create safe filename
            safe_name = player_name.replace(' ', '_').replace('/', '_')
            filename = f"{safe_name}_match_logs.csv"
            filepath = os.path.join(config.OUTPUT_DIR, filename)
            
            # Save to CSV
            combined_df.to_csv(filepath, index=False)
            logger.info(f"Saved {len(combined_df)} matches to {filename}")
            
        except Exception as e:
            logger.error(f"Failed to save data for {player_name}: {e}")
    
    def _get_seasons_to_scrape(self, last_season: Optional[str]) -> List[str]:
        """
        Get list of seasons to scrape (for resume capability).
        
        Args:
            last_season: Last successfully scraped season
            
        Returns:
            List of seasons to scrape
        """
        if not last_season:
            return config.SEASONS
        
        try:
            last_idx = config.SEASONS.index(last_season)
            return config.SEASONS[last_idx + 1:]
        except ValueError:
            return config.SEASONS
    
    def _update_player_state(
        self,
        player_id: str,
        player_name: str,
        last_season: Optional[str] = None,
        completed: bool = False
    ):
        """
        Update player state.
        
        Args:
            player_id: Player ID
            player_name: Player name
            last_season: Last scraped season
            completed: Whether player is completed
        """
        state = self.state_manager.load_state()
        
        if 'players' not in state:
            state['players'] = {}
        
        if player_id not in state['players']:
            state['players'][player_id] = {'player_name': player_name}
        
        if last_season:
            state['players'][player_id]['last_scraped_season'] = last_season
        
        if completed:
            state['players'][player_id]['completed'] = True
        
        self.state_manager.save_state(state)
    
    def _log_player_failure(self, player_id: str, player_name: str, error: str):
        """
        Log player-level failure.
        
        Args:
            player_id: Player ID
            player_name: Player name
            error: Error message
        """
        self.db_manager.log_scrape(
            player_id=player_id,
            player_name=player_name,
            season='ALL',
            status='failed',
            error_message=error
        )


def main():
    """Entry point."""
    scraper = FBrefScraper()
    scraper.run()


if __name__ == "__main__":
    main()