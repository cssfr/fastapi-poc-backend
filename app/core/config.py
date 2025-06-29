from pydantic_settings import BaseSettings
from typing import Optional, Dict, List
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
    
    # SINGLE SOURCE OF TRUTH - All timeframe configuration
    supported_timeframes: List[str] = ["1m", "3m", "5m", "10m", "15m", "30m", "1h", "4h", "1d", "1w", "1M", "1Y"]
    
    # Timeframe intervals in seconds
    timeframe_intervals: Dict[str, int] = {
        "1m": 60, "3m": 180, "5m": 300, "10m": 600, "15m": 900, "30m": 1800,
        "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800, "1M": 2592000, "1Y": 31536000
    }
    
    # Performance-optimized limits 
    max_days_by_timeframe: Dict[str, int] = {
        "1m": 1, "3m": 2, "5m": 7, "10m": 14, "15m": 30, "30m": 180,
        "1h": 365, "4h": 1095, "1d": 3650, "1w": 18250, "1M": 36500, "1Y": 7300
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