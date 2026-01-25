import os
import glob
import pandas as pd
from dotenv import load_dotenv

# Load env vars first so Kaggle API finds KAGGLE_USERNAME and KAGGLE_KEY
load_dotenv()

from kaggle.api.kaggle_api_extended import KaggleApi

from Utils.logging_config import get_logger
from Utils.db import upsert_dataframe

logger = get_logger("ingest_understat_roster")

DATASET = "yarknyorulmaz/understat-match-roster-metrics-dataset-epl-15-24"
DOWNLOAD_DIR = "Data/kaggle/roster"

def ingest_roster():
    try:
        api = KaggleApi()
        api.authenticate()

        logger.info(f"Downloading dataset {DATASET}...")
        api.dataset_download_files(DATASET, path=DOWNLOAD_DIR, unzip=True)
        
        csv_files = glob.glob(os.path.join(DOWNLOAD_DIR, "*.csv"))
        if not csv_files:
            logger.error("No CSV files found in downloaded dataset.")
            return

        for csv_file in csv_files:
            logger.info(f"Processing {csv_file}...")
            df = pd.read_csv(csv_file)
            
            logger.info(f"Columns: {df.columns.tolist()}")
            
            # Determine PKs.
            pks = []
            if "id" in df.columns:
                pks = ["id"]
            elif "match_id" in df.columns and "player_id" in df.columns:
                pks = ["match_id", "player_id"]
            
            upsert_dataframe(df, "understat_roster_metrics", primary_keys=pks)
            logger.info(f"Upserted {len(df)} rows from {csv_file}")

    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")
        raise

if __name__ == "__main__":
    ingest_roster()
