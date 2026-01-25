import json
import os
from contextlib import contextmanager
from typing import Iterable, Mapping, List

import mysql.connector
from mysql.connector import Error as MySQLError
from dotenv import load_dotenv
import pandas as pd
import numpy as np

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


def _map_dtype_to_mysql_type(dtype) -> str:
    """
    Map pandas dtype to MySQL column type.
    """
    str_type = str(dtype)
    if "int" in str_type:
        return "INT"
    if "float" in str_type:
        return "FLOAT"
    if "bool" in str_type:
        return "BOOLEAN"
    if "datetime" in str_type:
        return "DATETIME"
    return "TEXT"


def create_table_from_df(conn, table_name: str, df: pd.DataFrame, primary_keys: List[str] = None):
    """
    Create a MySQL table based on DataFrame columns and types.
    """
    cols = []
    for col_name, dtype in df.dtypes.items():
        mysql_type = _map_dtype_to_mysql_type(dtype)
        # Use backticks for column names to handle reserved words
        cols.append(f"`{col_name}` {mysql_type}")

    if primary_keys:
        # Ensure PK columns are not nullable if possible, though MySQL might enforce it anyway.
        # We'll just define the key constraints.
        pk_str = ", ".join([f"`{pk}`" for pk in primary_keys])
        cols.append(f"PRIMARY KEY ({pk_str})")

    cols_sql = ",\n".join(cols)
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {cols_sql}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
    except MySQLError as e:
        logger.error(f"Failed to create table {table_name}: {e}")
        raise DBWriteError(f"Could not create table {table_name}: {e}") from e


def upsert_dataframe(df: pd.DataFrame, table_name: str, primary_keys: List[str] = None, batch_size: int = 1000):
    """
    Insert or update rows from a DataFrame into the database.
    If the table does not exist, it is created.
    """
    if df.empty:
        logger.info(f"Empty dataframe provided for {table_name}, skipping.")
        return

    # Clean column names to ensure they work in SQL (simple sanitization)
    df.columns = [c.replace(" ", "_").lower() for c in df.columns]

    # Convert NaN to None (NULL in SQL)
    df = df.replace({np.nan: None})

    with get_connection() as conn:
        create_table_from_df(conn, table_name, df, primary_keys)
        
        columns = [f"`{col}`" for col in df.columns]
        placeholders = ["%s"] * len(df.columns)
        
        columns_str = ", ".join(columns)
        placeholders_str = ", ".join(placeholders)
        
        update_clause = ""
        if primary_keys:
            # ON DUPLICATE KEY UPDATE col1=VALUES(col1), ...
            updates = []
            for col in df.columns:
                if col not in primary_keys:
                    updates.append(f"`{col}` = VALUES(`{col}`)")
            if updates:
                update_clause = "ON DUPLICATE KEY UPDATE " + ", ".join(updates)
            else:
                # If only PKs and they match, do nothing (IGNORE behavior essentially in an INSERT context, but strict SQL)
                # But to trigger 'upsert' semantics validly we often just update a PK to itself or similar no-op if no other columns?
                # Actually if there are no non-PK columns, we can just IGNORE.
                pass
        
        sql = f"""
            INSERT INTO `{table_name}` ({columns_str})
            VALUES ({placeholders_str})
            {update_clause}
        """

        records = df.to_dict("records")
        total = len(records)
        
        try:
            with conn.cursor() as cur:
                for i in range(0, total, batch_size):
                    batch = records[i : i + batch_size]
                    batch_values = [list(row.values()) for row in batch]
                    cur.executemany(sql, batch_values)
                    conn.commit()
                    logger.info(f"Processed batch {i // batch_size + 1} for {table_name}")
            
            logger.info(f"Successfully upserted {total} rows into {table_name}.")
        except MySQLError as e:
            logger.error(f"Failed to upsert dataframe into {table_name}: {e}")
            raise DBWriteError(f"Could not upsert dataframe into {table_name}: {e}") from e

