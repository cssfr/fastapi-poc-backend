from pydantic import BaseModel, validator
from datetime import datetime
from typing import List, Optional
from decimal import Decimal

class Candle(BaseModel):
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    
    class Config:
        json_encoders = {
            Decimal: float
        }

class MarketDataQuery(BaseModel):
    symbol: str
    start_date: datetime
    end_date: datetime
    timeframe: str
    
    @validator('timeframe')
    def validate_timeframe(cls, v):
        valid = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M', '1Y']
        if v not in valid:
            raise ValueError(f"Invalid timeframe. Must be one of: {valid}")
        return v
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        if 'start_date' in values and v < values['start_date']:
            raise ValueError("End date must be after start date")
        return v 