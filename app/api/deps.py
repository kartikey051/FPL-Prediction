"""
FastAPI dependencies for authentication and database.
"""

from typing import Optional

from fastapi import Depends, Header
from fastapi.security import OAuth2PasswordBearer

from app.core.security import decode_access_token
from app.core.exceptions import CredentialsException
from app.db.models.user import get_user_by_username

# OAuth2 scheme for JWT token extraction
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    """
    Dependency that extracts and validates the current user from JWT.
    
    Raises:
        CredentialsException: If token is missing or invalid
    """
    if not token:
        raise CredentialsException("Not authenticated")
    
    payload = decode_access_token(token)
    
    if payload is None:
        raise CredentialsException("Invalid or expired token")
    
    username: str = payload.get("sub")
    
    if username is None:
        raise CredentialsException("Invalid token payload")
    
    user = get_user_by_username(username)
    
    if user is None:
        raise CredentialsException("User not found")
    
    return user


async def get_current_user_optional(
    authorization: Optional[str] = Header(None)
) -> Optional[dict]:
    """
    Optional user dependency - returns None if not authenticated.
    Useful for pages that work with or without auth.
    """
    if not authorization:
        return None
    
    try:
        # Extract token from "Bearer <token>"
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            return None
        
        payload = decode_access_token(token)
        if payload is None:
            return None
        
        username = payload.get("sub")
        if username:
            return get_user_by_username(username)
    except (ValueError, AttributeError):
        pass
    
    return None
