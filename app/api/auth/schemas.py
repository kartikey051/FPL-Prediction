"""
Pydantic schemas for authentication endpoints.
"""

from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime


class UserCreate(BaseModel):
    """Schema for user registration."""
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=6)


class UserLogin(BaseModel):
    """Schema for user login."""
    username: str
    password: str


class Token(BaseModel):
    """Schema for JWT token response."""
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """Schema for decoded token data."""
    username: Optional[str] = None


class UserResponse(BaseModel):
    """Schema for user data response (excludes password)."""
    id: int
    username: str
    email: str
    is_active: bool = True
    created_at: Optional[datetime] = None


class MessageResponse(BaseModel):
    """Generic message response."""
    message: str
    success: bool = True
