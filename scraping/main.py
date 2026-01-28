"""
Main orchestrator for FBref Premier League scraping.

This module coordinates all scraping operations:
1. Initialize driver and utilities
2. Discover players from wages page
3. Scrape match logs for each player across all seasons
4. Save data and maintain state
"""

import sys
import time
from typing import List, Dict

from Utils.logging_config import get_logger
from Utils.scraper_state import (
    load_player_state,
    update_player_progress,
    mark_player_failed,
    get_incomplete_players
)
from Utils.storage import save_player_data, file_exists

from scraping import config
from scraping.driver_factory import create_driver
from scraping.scrapers.wages_scrapers import scrape_players
from scraping.scrapers.matchlog_scrapers import scrape_all_seasons

logger = get_logger("main")


def scrape_player(
    driver,
    player_info: Dict[str, str],
    seasons: List[str],
    resume: bool = False
) -> bool:
    """
    Scrape all seasons for a single player.
    
    Args:
        driver: Selenium WebDriver instance
        player_info: Dict with player_name, player_id, player_url
        seasons: List of seasons to scrape
        resume: Whether this is resuming a previous attempt
        
    Returns:
        True if successful, False otherwise
    """
    player_id = player_info['player_id']
    player_name = player_info['player_name']
    
    logger.info(f"{'Resuming' if resume else 'Starting'} scrape for {player_name} ({player_id})")
    
    try:
        # Update state to in_progress
        update_player_progress(player_id, player_name, status="in_progress")
        
        # Scrape all seasons
        dataframes = scrape_all_seasons(driver, player_id, player_name, seasons)
        
        if not dataframes:
            logger.warning(f"No data collected for {player_name}")
            mark_player_failed(player_id, player_name, "No data available")
            return False
        
        # Save combined data
        filepath = save_player_data(
            player_name,
            player_id,
            dataframes,
            config.OUTPUT_DIR
        )
        
        if filepath:
            # Mark as completed
            update_player_progress(
                player_id,
                player_name,
                season=seasons[-1],  # Last season
                status="completed"
            )
            logger.info(f"✓ Successfully completed {player_name}")
            return True
        else:
            mark_player_failed(player_id, player_name, "Failed to save data")
            return False
            
    except Exception as e:
        logger.error(f"Failed to scrape {player_name}: {e}")
        mark_player_failed(player_id, player_name, str(e))
        return False


def main(resume: bool = True, headless: bool = True):
    """
    Main entry point for the scraping pipeline.
    
    Args:
        resume: Whether to resume incomplete players from previous runs
        headless: Whether to run browser in headless mode
    """
    logger.info("=" * 80)
    logger.info("FBref Premier League Scraper - Starting")
    logger.info("=" * 80)
    
    driver = None
    
    try:
        # Initialize WebDriver
        driver = create_driver(headless=headless)
        logger.info("WebDriver initialized successfully")
        
        # Load or discover players
        if resume:
            logger.info("Resume mode: checking for incomplete players")
            state = load_player_state()
            
            if state:
                incomplete_ids = get_incomplete_players()
                logger.info(f"Found {len(incomplete_ids)} incomplete players")
                
                # Rebuild player info from state
                players_to_scrape = [
                    {
                        'player_id': pid,
                        'player_name': state[pid]['player_name'],
                        'player_url': ''  # Not needed for match log URLs
                    }
                    for pid in incomplete_ids
                ]
                
                if not players_to_scrape:
                    logger.info("All players completed. Discovering new players...")
                    players_to_scrape = scrape_players(driver)
            else:
                logger.info("No previous state found. Discovering players...")
                players_to_scrape = scrape_players(driver)
        else:
            logger.info("Fresh start: discovering all players")
            players_to_scrape = scrape_players(driver)
        
        if not players_to_scrape:
            logger.error("No players found to scrape")
            return
        
        logger.info(f"Total players to scrape: {len(players_to_scrape)}")
        
        # Process each player
        successful = 0
        failed = 0
        
        for idx, player_info in enumerate(players_to_scrape, 1):
            player_name = player_info['player_name']
            
            logger.info(f"\n[{idx}/{len(players_to_scrape)}] Processing: {player_name}")
            
            # Check if already completed
            if file_exists(player_name, config.OUTPUT_DIR) and resume:
                logger.info(f"Skipping {player_name} - already exists")
                continue
            
            # Scrape player
            success = scrape_player(
                driver,
                player_info,
                config.SEASONS,
                resume=resume
            )
            
            if success:
                successful += 1
            else:
                failed += 1
            
            # Rate limiting between players
            if idx < len(players_to_scrape):
                time.sleep(config.REQUEST_DELAY)
        
        # Final summary
        logger.info("=" * 80)
        logger.info("SCRAPING COMPLETE")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Total: {len(players_to_scrape)}")
        logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.warning("\n\nScraping interrupted by user")
        logger.info("Progress has been saved. Run again with resume=True to continue.")
        
    except Exception as e:
        logger.error(f"Fatal error in main pipeline: {e}", exc_info=True)
        raise
        
    finally:
        # Clean up
        if driver:
            try:
                driver.quit()
                logger.info("WebDriver closed successfully")
            except Exception as e:
                logger.warning(f"Error closing WebDriver: {e}")


if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Scrape Premier League player match logs from FBref"
    )
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start fresh instead of resuming incomplete players'
    )
    parser.add_argument(
        '--no-headless',
        action='store_true',
        help='Run browser in visible mode (not headless)'
    )
    
    args = parser.parse_args()
    
    try:
        main(
            resume=not args.no_resume,
            headless=not args.no_headless
        )
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        sys.exit(1)