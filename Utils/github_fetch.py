"""
Utilities for fetching raw data from GitHub efficiently.
Uses retries and caching.
"""

import os
import requests
import pandas as pd
from io import StringIO
from typing import Optional

from Utils.logging_config import get_logger
from Utils.http import safe_get

logger = get_logger("github_fetch")

RAW_BASE_URL = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
CACHE_DIR = "Data/github_cache"


def fetch_csv_from_github(
    path: str,
    use_cache: bool = True,
    encoding: str = "utf-8"
) -> Optional[pd.DataFrame]:
    """
    Fetch a CSV file from the FPL GitHub repository.
    
    Args:
        path: Relative path from data/ directory (e.g., "2023-24/teams.csv")
        use_cache: Whether to use local cache if available
        encoding: File encoding
        
    Returns:
        DataFrame or None if fetch fails
    """
    
    cache_path = os.path.join(CACHE_DIR, path.replace("/", os.sep))
    
    # Check cache first
    if use_cache and os.path.exists(cache_path):
        try:
            df = pd.read_csv(cache_path, encoding=encoding)
            logger.info(f"Loaded from cache: {path}")
            return df
        except Exception:
            logger.warning(f"Cache corrupt for {path}, refetching...")
    
    # Fetch from GitHub
    url = f"{RAW_BASE_URL}/{path}"
    try:
        logger.info(f"Fetching from GitHub: {url}")
        response = safe_get(url)
        response.encoding = encoding
        
        # Parse CSV
        content = response.text
        df = pd.read_csv(StringIO(content))
        
        # Save to cache
        if use_cache:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            df.to_csv(cache_path, index=False)
        
        return df
        
    except Exception as e:
        logger.warning(f"Failed to fetch {path}: {e}")
        return None
