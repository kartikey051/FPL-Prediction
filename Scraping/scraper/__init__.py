"""Scrapers package for FBref data extraction."""

from .wages_scraper import WagesScraper
from .player_scraper import PlayerScraper
from .matchlog_scraper import MatchLogScraper

__all__ = ['WagesScraper', 'PlayerScraper', 'MatchLogScraper']