"""
User model and database operations.
"""

from datetime import datetime
from typing import Optional

from app.db.session import execute_query, execute_write, get_db_connection
from Utils.logging_config import get_logger

logger = get_logger("user_model")


# SQL for creating users table
CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    hashed_password VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username),
    INDEX idx_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_users_table():
    """Create users table if it doesn't exist."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(CREATE_USERS_TABLE)
            conn.commit()
            cursor.close()
        logger.info("Users table ensured")
    except Exception as e:
        logger.error(f"Failed to create users table: {e}")
        raise


def get_user_by_username(username: str) -> Optional[dict]:
    """
    Fetch a user by username.
    
    Returns:
        User dict or None if not found
    """
    query = "SELECT * FROM users WHERE username = %s"
    results = execute_query(query, (username,))
    return results[0] if results else None


def get_user_by_email(email: str) -> Optional[dict]:
    """
    Fetch a user by email.
    
    Returns:
        User dict or None if not found
    """
    query = "SELECT * FROM users WHERE email = %s"
    results = execute_query(query, (email,))
    return results[0] if results else None


def get_user_by_id(user_id: int) -> Optional[dict]:
    """
    Fetch a user by ID.
    
    Returns:
        User dict or None if not found
    """
    query = "SELECT * FROM users WHERE id = %s"
    results = execute_query(query, (user_id,))
    return results[0] if results else None


def create_user(username: str, email: str, hashed_password: str) -> int:
    """
    Create a new user.
    
    Returns:
        The new user's ID
    """
    query = """
        INSERT INTO users (username, email, hashed_password)
        VALUES (%s, %s, %s)
    """
    user_id = execute_write(query, (username, email, hashed_password))
    logger.info(f"Created user: {username} with id: {user_id}")
    return user_id


def user_exists(username: str = None, email: str = None) -> bool:
    """
    Check if a user exists by username or email.
    """
    if username:
        return get_user_by_username(username) is not None
    if email:
        return get_user_by_email(email) is not None
    return False
