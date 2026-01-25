"""
Application configuration using Pydantic Settings.
Loads from environment variables with fallbacks.
"""

import os
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment."""
    
    # Application
    APP_NAME: str = "FPL Dashboard"
    DEBUG: bool = False
    
    # Database - reuse existing FPL_DB_* pattern
    FPL_DB_HOST: str = "localhost"
    FPL_DB_PORT: int = 3306
    FPL_DB_USER: str = ""
    FPL_DB_PASSWORD: str = ""
    FPL_DB_NAME: str = ""
    
    # JWT Configuration
    JWT_SECRET_KEY: str = "your-super-secret-key-change-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Cached settings instance."""
    return Settings()


settings = get_settings()
