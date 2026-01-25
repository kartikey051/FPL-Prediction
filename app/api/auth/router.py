"""
Authentication router - endpoints for login and registration.
"""

from fastapi import APIRouter, Depends, status
from fastapi.security import OAuth2PasswordRequestForm

from app.api.auth.schemas import UserCreate, UserLogin, Token, UserResponse, MessageResponse
from app.api.auth.service import register_user, login_user
from app.api.deps import get_current_user

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(user_data: UserCreate):
    """
    Register a new user.
    
    - **username**: Unique username (3-50 characters)
    - **email**: Valid email address
    - **password**: Password (min 6 characters)
    """
    user = register_user(user_data)
    return user


@router.post("/login", response_model=Token)
async def login(user_data: UserLogin):
    """
    Login and receive JWT access token.
    
    - **username**: Your username
    - **password**: Your password
    
    Returns JWT token for authenticated requests.
    """
    token = login_user(user_data.username, user_data.password)
    return token


@router.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    OAuth2 compatible token endpoint.
    Used by Swagger UI for authentication.
    """
    token = login_user(form_data.username, form_data.password)
    return token


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """
    Get current authenticated user's information.
    
    Requires valid JWT token in Authorization header.
    """
    return UserResponse(
        id=current_user["id"],
        username=current_user["username"],
        email=current_user["email"],
        is_active=current_user.get("is_active", True),
        created_at=current_user.get("created_at"),
    )
