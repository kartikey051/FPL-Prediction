"""
Storage utilities for saving scraped data to files.
"""

import os
import pandas as pd
from typing import List

from Utils.logging_config import get_logger

logger = get_logger("storage")


def save_player_data(
    player_name: str,
    player_id: str,
    dataframes: List[pd.DataFrame],
    output_dir: str
) -> str:
    """
    Save all season data for a player to a single CSV file.
    
    Args:
        player_name: Player's name
        player_id: Player's ID
        dataframes: List of DataFrames (one per season)
        output_dir: Directory to save CSV files
        
    Returns:
        Path to saved file
    """
    if not dataframes:
        logger.warning(f"No data to save for {player_name}")
        return None
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Combine all seasons
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Clean filename (remove special characters)
    safe_name = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' 
                       for c in player_name)
    safe_name = safe_name.strip().replace(' ', '_')
    
    filename = f"{safe_name}_match_logs.csv"
    filepath = os.path.join(output_dir, filename)
    
    try:
        combined_df.to_csv(filepath, index=False, encoding='utf-8')
        logger.info(f"✓ Saved {len(combined_df)} rows to {filepath}")
        return filepath
        
    except Exception as e:
        logger.error(f"Failed to save data for {player_name}: {e}")
        raise


def file_exists(player_name: str, output_dir: str) -> bool:
    """
    Check if a player's data file already exists.
    
    Args:
        player_name: Player's name
        output_dir: Output directory
        
    Returns:
        True if file exists
    """
    safe_name = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' 
                       for c in player_name)
    safe_name = safe_name.strip().replace(' ', '_')
    
    filename = f"{safe_name}_match_logs.csv"
    filepath = os.path.join(output_dir, filename)
    
    return os.path.exists(filepath)