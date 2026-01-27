import os
import glob
import re
import pandas as pd
from dotenv import load_dotenv

# Load env vars first so Kaggle API finds KAGGLE_USERNAME and KAGGLE_KEY
load_dotenv()

from kaggle.api.kaggle_api_extended import KaggleApi

from Utils.logging_config import get_logger
from Utils.db import upsert_dataframe, get_connection, execute_query

logger = get_logger("ingest_understat_roster")

ROSTER_DATASET = "yarknyorulmaz/understat-match-roster-metrics-dataset-epl-15-24"
MATCH_DATASET = "yarknyorulmaz/understat-match-team-metrics-dataset-epl-14-24"
ROSTER_DIR = "Data/kaggle/roster"
MATCH_DIR = "Data/kaggle/teams"


def extract_match_id(match_link: str) -> int:
    """Extract numeric match ID from Understat match link."""
    if pd.isna(match_link):
        return None
    match = re.search(r'/match/(\d+)', str(match_link))
    return int(match.group(1)) if match else None


def load_match_seasons() -> pd.DataFrame:
    """Load match data to create match_id -> season mapping."""
    csv_files = glob.glob(os.path.join(MATCH_DIR, "*.csv"))
    if not csv_files:
        logger.warning("No match CSV files found, will try DB lookup")
        return None
    
    all_matches = []
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        if 'id' in df.columns and 'season' in df.columns:
            all_matches.append(df[['id', 'season']])
    
    if all_matches:
        match_df = pd.concat(all_matches, ignore_index=True).drop_duplicates(subset=['id'])
        match_df = match_df.rename(columns={'id': 'match_id'})
        logger.info(f"Loaded {len(match_df)} match->season mappings from CSV")
        return match_df
    return None


def ensure_season_column():
    """Add season column to understat_roster_metrics if it doesn't exist."""
    try:
        # Check if column exists
        check_query = """
            SELECT COUNT(*) as cnt 
            FROM information_schema.columns 
            WHERE table_name = 'understat_roster_metrics' 
            AND column_name = 'season'
        """
        result = execute_query(check_query)
        
        if result and result[0]["cnt"] == 0:
            logger.info("Adding 'season' column to understat_roster_metrics...")
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("ALTER TABLE understat_roster_metrics ADD COLUMN season INT")
                conn.commit()
                cursor.close()
            logger.info("Successfully added 'season' column")
        else:
            logger.info("'season' column already exists in table")
    except Exception as e:
        logger.warning(f"Could not ensure season column: {e}")


def ingest_roster():
    try:
        api = KaggleApi()
        api.authenticate()

        # Download roster dataset
        logger.info(f"Downloading roster dataset {ROSTER_DATASET}...")
        api.dataset_download_files(ROSTER_DATASET, path=ROSTER_DIR, unzip=True)
        
        # Download match dataset if not already present (for season mapping)
        if not glob.glob(os.path.join(MATCH_DIR, "*.csv")):
            logger.info(f"Downloading match dataset {MATCH_DATASET} for season mapping...")
            api.dataset_download_files(MATCH_DATASET, path=MATCH_DIR, unzip=True)
        
        # Ensure database table has season column
        ensure_season_column()
        
        # Load match->season mapping
        match_seasons = load_match_seasons()
        
        csv_files = glob.glob(os.path.join(ROSTER_DIR, "*.csv"))
        if not csv_files:
            logger.error("No CSV files found in downloaded roster dataset.")
            return

        for csv_file in csv_files:
            logger.info(f"Processing {csv_file}...")
            df = pd.read_csv(csv_file)
            
            logger.info(f"Original columns: {df.columns.tolist()}")
            
            # Extract match_id from match_link and add season column
            if 'match_link' in df.columns and match_seasons is not None:
                df['match_id_extracted'] = df['match_link'].apply(extract_match_id)
                df = df.merge(
                    match_seasons,
                    left_on='match_id_extracted',
                    right_on='match_id',
                    how='left',
                    suffixes=('', '_match')
                )
                # Drop all merge artifact columns (keep only 'season')
                df = df.drop(columns=['match_id_extracted', 'match_id'], errors='ignore')
                if 'match_id_match' in df.columns:
                    df = df.drop(columns=['match_id_match'], errors='ignore')
                logger.info(f"Added season column. Seasons found: {df['season'].dropna().unique().tolist()}")
            else:
                logger.warning("Cannot add season column - match_link or match data missing")
            
            logger.info(f"Final columns: {df.columns.tolist()}")
            
            # Determine PKs
            pks = []
            if "id" in df.columns:
                pks = ["id"]
            
            upsert_dataframe(df, "understat_roster_metrics", primary_keys=pks)
            logger.info(f"Upserted {len(df)} rows from {csv_file}")

    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")
        raise


if __name__ == "__main__":
    ingest_roster()

