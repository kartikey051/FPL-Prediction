"""
Authentication service - business logic for user operations.
"""

from typing import Optional

from app.core.security import verify_password, get_password_hash, create_access_token
from app.core.exceptions import UserExistsException, CredentialsException
from app.db.models.user import (
    get_user_by_username,
    get_user_by_email,
    create_user as db_create_user,
    user_exists,
)
from app.api.auth.schemas import UserCreate, Token
from Utils.logging_config import get_logger

logger = get_logger("auth_service")


def register_user(user_data: UserCreate) -> dict:
    """
    Register a new user.
    
    Args:
        user_data: User registration data
    
    Returns:
        The created user (without password)
    
    Raises:
        UserExistsException: If username or email already exists
    """
    # Check if username exists
    if user_exists(username=user_data.username):
        logger.warning(f"Registration failed: username {user_data.username} exists")
        raise UserExistsException("Username already registered")
    
    # Check if email exists
    if user_exists(email=user_data.email):
        logger.warning(f"Registration failed: email {user_data.email} exists")
        raise UserExistsException("Email already registered")
    
    # Hash password and create user
    hashed_password = get_password_hash(user_data.password)
    user_id = db_create_user(
        username=user_data.username,
        email=user_data.email,
        hashed_password=hashed_password
    )
    
    logger.info(f"User registered successfully: {user_data.username}")
    
    return {
        "id": user_id,
        "username": user_data.username,
        "email": user_data.email,
        "is_active": True,
    }


def authenticate_user(username: str, password: str) -> Optional[dict]:
    """
    Authenticate a user by username and password.
    
    Returns:
        User dict if authentication succeeds, None otherwise
    """
    user = get_user_by_username(username)
    
    if not user:
        logger.warning(f"Login failed: user {username} not found")
        return None
    
    if not verify_password(password, user["hashed_password"]):
        logger.warning(f"Login failed: invalid password for {username}")
        return None
    
    logger.info(f"User authenticated: {username}")
    return user


def login_user(username: str, password: str) -> Token:
    """
    Authenticate user and generate JWT token.
    
    Returns:
        Token object with access_token
    
    Raises:
        CredentialsException: If authentication fails
    """
    user = authenticate_user(username, password)
    
    if not user:
        raise CredentialsException("Incorrect username or password")
    
    # Create JWT token
    access_token = create_access_token(data={"sub": user["username"]})
    
    return Token(access_token=access_token, token_type="bearer")
