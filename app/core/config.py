from pydantic_settings import BaseSettings
from typing import Optional, Dict
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
    
    # OHLCV Request Limits - Updated for yearly timeframes
    max_records_per_request: int = 50000
    max_days_by_timeframe: Dict[str, int] = {
        "1m": 7,        # 1-minute data: max 1 week
        "5m": 30,       # 5-minute data: max 1 month  
        "15m": 90,      # 15-minute data: max 3 months
        "30m": 180,     # 30-minute data: max 6 months
        "1h": 365,      # 1-hour data: max 1 year
        "4h": 1095,     # 4-hour data: max 3 years
        "1d": 3650,     # 1-day data: max 10 years
        "1w": 18250,    # 1-week data: max 50 years
        "1M": 36500,    # 1-month data: max 100 years
        "1Y": 7300,     # 20 years - ENSURE THIS EXISTS
    }
    
    # Auto-adjustment thresholds
    auto_adjust_timeframe: bool = True
    auto_adjust_thresholds: Dict[str, int] = {
        "to_1d": 1095,    # > 3 years -> daily
        "to_1h": 365,     # > 1 year -> hourly  
        "to_15m": 30,     # > 1 month -> 15min
    }
    
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