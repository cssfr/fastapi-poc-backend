from pydantic_settings import BaseSettings
from typing import Optional
from functools import lru_cache

class Settings(BaseSettings):
    # Application settings
    app_name: str = "Backtesting API"
    version: str = "1.0.0"
    environment: str = "development"
    log_level: str = "INFO"
    log_format: str = "json"
    
    # Database settings (required)
    database_url: str
    db_pool_min: int = 1
    db_pool_max: int = 5
    
    # Authentication (required)
    supabase_url: str
    supabase_jwt_secret: str
    
    # Storage (optional)
    minio_endpoint: Optional[str] = None
    minio_access_key: Optional[str] = None
    minio_secret_key: Optional[str] = None
    minio_secure: bool = False
    minio_bucket: str = "dukascopy-node"
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        # Add env prefix if needed
        # env_prefix = "APP_"

@lru_cache()
def get_settings():
    return Settings()

# Global instance
settings = get_settings() 