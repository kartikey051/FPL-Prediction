import json
import os
from contextlib import contextmanager
from typing import Iterable, Mapping

import mysql.connector
from mysql.connector import Error as MySQLError
from dotenv import load_dotenv

from Utils.logging_config import get_logger
from Exceptions.api_errors import DBConnectionError, DBWriteError


logger = get_logger("db")

# Load environment variables from a .env file in the project root if present.
# This is a no-op if the file does not exist or variables are already set.
load_dotenv()


def _get_db_config() -> dict:
    """
    Read database configuration from environment variables.

    Expected variables:
      - FPL_DB_HOST
      - FPL_DB_PORT
      - FPL_DB_USER
      - FPL_DB_PASSWORD
      - FPL_DB_NAME
    """
    host = os.getenv("FPL_DB_HOST", "localhost")
    port = int(os.getenv("FPL_DB_PORT", "3306"))
    user = os.getenv("FPL_DB_USER")
    password = os.getenv("FPL_DB_PASSWORD")
    database = os.getenv("FPL_DB_NAME")

    if not all([user, password, database]):
        raise DBConnectionError(
            "Database credentials are not fully configured. "
            "Please set FPL_DB_USER, FPL_DB_PASSWORD and FPL_DB_NAME."
        )

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }


@contextmanager
def get_connection():
    """
    Context manager that yields a MySQL connection.
    """
    cfg = _get_db_config()

    try:
        conn = mysql.connector.connect(**cfg)
        yield conn
    except MySQLError as e:
        logger.error(f"Failed to connect to MySQL: {e}")
        raise DBConnectionError(f"Could not connect to MySQL: {e}") from e
    finally:
        try:
            if "conn" in locals() and conn.is_connected():
                conn.close()
        except Exception:
            # Best-effort close, don't mask original errors
            pass


def _ensure_events_table(conn) -> None:
    """
    Ensure the raw events table exists.

    Schema:
      - id: auto-increment primary key
      - event_id: unique identifier for the gameweek
      - payload: JSON payload from the FPL API
      - created_at: insert timestamp
      - updated_at: last update timestamp
    """
    create_sql = """
        CREATE TABLE IF NOT EXISTS events_raw (
            id INT AUTO_INCREMENT PRIMARY KEY,
            event_id INT NOT NULL UNIQUE,
            payload JSON NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        ENGINE=InnoDB
        DEFAULT CHARSET=utf8mb4;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
    except MySQLError as e:
        logger.error(f"Failed to ensure events_raw table exists: {e}")
        raise DBWriteError(f"Could not create or verify events_raw table: {e}") from e


def upsert_events(records: Iterable[Mapping]) -> None:
    """
    Insert or update a collection of event records into the database.

    Each record is expected to contain:
      - event_id: int
      - data: dict (JSON-serializable)
    """
    records = list(records)
    if not records:
        return

    with get_connection() as conn:
        _ensure_events_table(conn)

        sql = """
            INSERT INTO events_raw (event_id, payload)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE payload = VALUES(payload),
                                    updated_at = CURRENT_TIMESTAMP
        """

        params = []
        for rec in records:
            try:
                event_id = rec["event_id"]
                data = rec["data"]
            except KeyError as e:
                logger.error(f"Malformed record missing key {e}: {rec}")
                continue

            payload_json = json.dumps(data)
            params.append((event_id, payload_json))

        if not params:
            return

        try:
            with conn.cursor() as cur:
                cur.executemany(sql, params)
            conn.commit()
            logger.info(f"Upserted {len(params)} raw event records into MySQL.")
        except MySQLError as e:
            logger.error(f"Failed to upsert events into MySQL: {e}")
            raise DBWriteError(f"Could not upsert events into database: {e}") from e

