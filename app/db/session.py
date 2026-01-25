"""
Database session management.
Reuses the existing Utils.db pattern for MySQL connections.
"""

from contextlib import contextmanager
from typing import Generator

import mysql.connector
from mysql.connector import Error as MySQLError

from app.core.config import settings
from Utils.logging_config import get_logger

logger = get_logger("db_session")


def get_db_config() -> dict:
    """Get database configuration from settings."""
    return {
        "host": settings.FPL_DB_HOST,
        "port": settings.FPL_DB_PORT,
        "user": settings.FPL_DB_USER,
        "password": settings.FPL_DB_PASSWORD,
        "database": settings.FPL_DB_NAME,
    }


@contextmanager
def get_db_connection():
    """
    Context manager that yields a MySQL connection.
    For use in FastAPI dependencies.
    """
    cfg = get_db_config()
    conn = None
    
    try:
        conn = mysql.connector.connect(**cfg)
        yield conn
    except MySQLError as e:
        logger.error(f"Database connection failed: {e}")
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()


def get_db() -> Generator:
    """
    FastAPI dependency that yields a database connection.
    """
    with get_db_connection() as conn:
        yield conn


def execute_query(query: str, params: tuple = None, fetch: bool = True):
    """
    Execute a SQL query and return results.
    
    Args:
        query: SQL query string
        params: Query parameters
        fetch: Whether to fetch results
    
    Returns:
        List of dictionaries for SELECT queries, or affected row count
    """
    with get_db_connection() as conn:
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(query, params or ())
            
            if fetch:
                results = cursor.fetchall()
                return results
            else:
                conn.commit()
                return cursor.rowcount
        finally:
            cursor.close()


def execute_write(query: str, params: tuple = None) -> int:
    """
    Execute a write query (INSERT, UPDATE, DELETE).
    
    Returns:
        Last inserted ID for INSERT, or affected row count
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query, params or ())
            conn.commit()
            return cursor.lastrowid if cursor.lastrowid else cursor.rowcount
        finally:
            cursor.close()
