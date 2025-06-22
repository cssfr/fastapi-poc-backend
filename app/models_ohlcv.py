"""Models for OHLCV data"""
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import date, datetime
from decimal import Decimal

class OHLCVRequest(BaseModel):
    """Request model for OHLCV data"""
    symbol: str = Field(..., min_length=1, max_length=20)
    start_date: date
    end_date: date
    timeframe: str = Field(default="1d", pattern="^(1m|5m|15m|30m|1h|4h|1d|1w)$")
    source_resolution: str = Field(default="1m", description="Source data resolution (1m or 1Y)")
    
    @validator('symbol')
    def normalize_symbol(cls, v):
        """Normalize symbol to uppercase"""
        return v.upper().strip()
    
    @validator('source_resolution')
    def validate_source_resolution(cls, v):
        """Validate source resolution"""
        if v not in ["1m", "1Y"]:
            raise ValueError('source_resolution must be either "1m" or "1Y"')
        return v
    
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
    source_resolution: str = Field(default="1m", description="Source data resolution used (1m or 1Y)")
    start_date: str
    end_date: str
    count: int
    data: List[OHLCVData]

class PerformanceTestResult(BaseModel):
    """Result model for performance testing"""
    duration_seconds: Optional[float]
    record_count: int
    success: bool
    error: Optional[str] = None

class PerformanceTestResponse(BaseModel):
    """Response model for performance test comparison"""
    test_metadata: dict
    one_m: PerformanceTestResult = Field(..., alias="1m")
    one_y: PerformanceTestResult = Field(..., alias="1Y")
    performance_improvement_percent: Optional[float] = None
    
    class Config:
        allow_population_by_field_name = True

class StorageStructureInfo(BaseModel):
    """Storage structure information model"""
    source_resolution: str
    total_files: int
    total_size_bytes: int
    total_size_mb: float
    symbol_count: int
    symbols: List[str]
    date_ranges: dict

class StorageComparisonResponse(BaseModel):
    """Response model for storage structure comparison"""
    one_m: StorageStructureInfo = Field(..., alias="1m")
    one_y: StorageStructureInfo = Field(..., alias="1Y")
    comparison: dict
    
    class Config:
        allow_population_by_field_name = True

class DataRange(BaseModel):
    """Data availability range for an instrument"""
    earliest: Optional[str] = Field(None, description="Earliest available date (YYYY-MM-DD)")
    latest: Optional[str] = Field(None, description="Latest available date (YYYY-MM-DD)")
    sources: Optional[Dict[str, Dict[str, Optional[str]]]] = Field(
        None, 
        description="Data ranges per source resolution"
    )

class InstrumentMetadata(BaseModel):
    """Comprehensive metadata for a trading instrument"""
    symbol: str = Field(..., description="Instrument symbol/ticker")
    exchange: str = Field(..., description="Exchange where instrument is traded")
    market: str = Field(..., description="Market type (FUTURES, STOCKS, FOREX, etc.)")
    name: str = Field(..., description="Full instrument name")
    shortName: str = Field(..., description="Short display name")
    ticker: str = Field(..., description="Trading ticker symbol")
    type: str = Field(..., description="Instrument type (FUT, STK, OPT, etc.)")
    currency: str = Field(..., description="Trading currency")
    description: str = Field(..., description="Instrument description")
    sector: str = Field(..., description="Market sector/category")
    country: str = Field(..., description="Country code")
    dataRange: Optional[DataRange] = Field(None, description="Available data date range")
    availableTimeframes: List[str] = Field(
        default=["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"],
        description="Available aggregation timeframes"
    )
    sourceResolutions: List[str] = Field(
        default=["1m", "1Y"], 
        description="Available source data resolutions"
    )

class InstrumentsResponse(BaseModel):
    """Response model for instruments metadata endpoint"""
    count: int = Field(..., description="Number of instruments returned")
    instruments: List[InstrumentMetadata] = Field(..., description="List of instruments with metadata")
    lastUpdated: str = Field(..., description="ISO timestamp when data was last updated")
    cacheInfo: Dict[str, Any] = Field(
        default_factory=dict,
        description="Information about data caching"
    )