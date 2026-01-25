"""
Custom HTTP exceptions for the application.
"""

from fastapi import HTTPException, status


class CredentialsException(HTTPException):
    """Raised when authentication credentials are invalid."""
    
    def __init__(self, detail: str = "Could not validate credentials"):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
        )


class UserExistsException(HTTPException):
    """Raised when attempting to create a user that already exists."""
    
    def __init__(self, detail: str = "Username already registered"):
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        )


class UserNotFoundException(HTTPException):
    """Raised when a user is not found."""
    
    def __init__(self, detail: str = "User not found"):
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=detail,
        )


class DatabaseException(HTTPException):
    """Raised when a database operation fails."""
    
    def __init__(self, detail: str = "Database operation failed"):
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail,
        )
