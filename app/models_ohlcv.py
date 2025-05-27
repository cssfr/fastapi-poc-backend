"""Models for OHLCV data"""
from pydantic import BaseModel, Field, validator
from typing import List, Optional
from datetime import date, datetime
from decimal import Decimal

class OHLCVRequest(BaseModel):
    """Request model for OHLCV data"""
    symbol: str = Field(..., min_length=1, max_length=20)
    start_date: date
    end_date: date
    timeframe: str = Field(default="1d", regex="^(1m|5m|15m|30m|1h|4h|1d|1w)$")
    
    @validator('symbol')
    def normalize_symbol(cls, v):
        """Normalize symbol to uppercase"""
        return v.upper().strip()
    
    @validator('end_date')
    def validate_dates(cls, v, values):
        """Ensure end_date is after start_date"""
        if 'start_date' in values and v < values['start_date']:
            raise ValueError('end_date must be after or equal to start_date')
        if v > date.today():
            raise ValueError('end_date cannot be in the future')
        return v

class OHLCVData(BaseModel):
    """Single OHLCV data point"""
    symbol: str
    timestamp: str  # ISO format string
    unix_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float

class OHLCVResponse(BaseModel):
    """Response model for OHLCV data"""
    symbol: str
    timeframe: str
    start_date: str
    end_date: str
    count: int
    data: List[OHLCVData]