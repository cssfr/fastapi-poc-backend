from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from typing import List, Optional
from datetime import datetime, timezone, date
import logging

from app.auth import verify_token
from app.models_ohlcv import OHLCVRequest, OHLCVResponse, OHLCVData
from app.services.market_data_service import MarketDataService
from app.services.storage_service import StorageService
from app.services.instrument_service import InstrumentService
from app.infrastructure.cache import market_data_cache
from app.core.config import settings

logger = logging.getLogger(__name__)

# Services will be created with dependency injection in each endpoint

router = APIRouter(
    prefix="/api/v1/ohlcv",
    tags=["ohlcv"],
    # Remove router-level auth to allow OPTIONS preflight requests
)

# Create a global instrument service instance (singleton pattern)
_global_instrument_service = None

def get_instrument_service() -> InstrumentService:
    """Get or create the global instrument service instance"""
    global _global_instrument_service
    if _global_instrument_service is None:
        _global_instrument_service = InstrumentService()
    return _global_instrument_service

@router.get("/symbols", response_model=List[str])
async def get_available_symbols(request: Request, user_id: str = Depends(verify_token)):
    """Get all available symbols in the dataset"""
    logger.info(
        "Fetching available symbols",
        extra={"request_id": getattr(request.state, "request_id", "unknown")}
    )
    
    market_data_service = MarketDataService()
    symbols = await market_data_service.get_available_symbols()
    
    logger.info(
        f"Found {len(symbols)} symbols",
        extra={
            "symbol_count": len(symbols),
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    return symbols

@router.get("/timeframes")
async def get_supported_timeframes():
    """Get supported timeframes from centralized config"""
    return {"timeframes": settings.supported_timeframes}

@router.get("/date-range/{symbol}")
async def get_symbol_date_range(
    request: Request,
    symbol: str,
    timeframe: str = Query(..., description="Timeframe (e.g., '1m', '5m', '1h', '1d')"),
    source_resolution: str = Query("1Y", description="Source resolution (1m or 1Y)"),
    user_id: str = Depends(verify_token)
):
    """Get the available date range for a specific symbol and timeframe"""
    logger.info(
        f"Fetching date range for symbol: {symbol}, timeframe: {timeframe}",
        extra={
            "symbol": symbol,
            "timeframe": timeframe,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    # Create services with dependency injection
    instrument_service = InstrumentService()
    market_data_service = MarketDataService(instrument_service=instrument_service)
    
    # NEW: Get date range from instruments metadata first
    data_range_tuple = await instrument_service.get_data_range(symbol, source_resolution)
    if data_range_tuple:
        earliest, latest = data_range_tuple
        date_range = {"earliest": earliest, "latest": latest}
    else:
        # Fallback to existing method if no metadata available
        dates = await market_data_service.get_available_dates(symbol, source_resolution)
        if dates:
            date_range = {"earliest": dates[0], "latest": dates[-1]}
        else:
            date_range = None
    
    if not date_range:
        logger.warning(
            f"No data found for symbol: {symbol}, timeframe: {timeframe}",
            extra={
                "symbol": symbol,
                "timeframe": timeframe,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No data found for symbol {symbol} with timeframe {timeframe}"
        )
    
    return date_range

@router.get("/data", response_model=OHLCVResponse)
async def get_ohlcv_data_get(
    request: Request,
    symbol: str = Query(..., description="Trading symbol"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    timeframe: str = Query("1d", description="Timeframe"),
    source_resolution: str = Query("1Y", description="Source resolution (1m or 1Y)"),
    user_id: str = Depends(verify_token)
):
    """Get OHLCV data using GET with query parameters"""
    # Parse dates and create OHLCVRequest object for validation
    try:
        parsed_start_date = date.fromisoformat(start_date)
        parsed_end_date = date.fromisoformat(end_date)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid date format: {str(e)}"
        )
    
    # Create request object to reuse validation logic
    ohlcv_request = OHLCVRequest(
        symbol=symbol,
        start_date=parsed_start_date,
        end_date=parsed_end_date,
        timeframe=timeframe,
        source_resolution=source_resolution
    )
    
    # Use common internal logic
    return await _get_ohlcv_data_internal(request, ohlcv_request)

async def _get_ohlcv_data_internal(request: Request, ohlcv_request: OHLCVRequest) -> OHLCVResponse:
    """Internal method for getting OHLCV data - used by both GET and POST endpoints"""
    logger.info(
        f"OHLCV data request: {ohlcv_request.symbol}, {ohlcv_request.timeframe}, {ohlcv_request.start_date} to {ohlcv_request.end_date}",
        extra={
            "symbol": ohlcv_request.symbol,
            "timeframe": ohlcv_request.timeframe,
            "start_date": ohlcv_request.start_date.isoformat(),
            "end_date": ohlcv_request.end_date.isoformat(),
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    # Validate request
    if ohlcv_request.start_date > ohlcv_request.end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Start date must not be after end date"
        )
    
    # Use shared instrument service instance
    instrument_service = get_instrument_service()
    market_data_service = MarketDataService(instrument_service=instrument_service)
    
    # Get data from market data service
    data = await market_data_service.get_ohlcv_data(
        symbol=ohlcv_request.symbol,
        start_date=ohlcv_request.start_date,
        end_date=ohlcv_request.end_date,
        timeframe=ohlcv_request.timeframe,
        source_resolution=ohlcv_request.source_resolution
    )
    
    if not data:
        logger.warning(
            f"No OHLCV data found for request",
            extra={
                "symbol": ohlcv_request.symbol,
                "timeframe": ohlcv_request.timeframe,
                "start_date": ohlcv_request.start_date.isoformat(),
                "end_date": ohlcv_request.end_date.isoformat(),
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No data found for the specified parameters"
        )
    
    # Convert to response format
    ohlcv_data = [
        OHLCVData(
            symbol=ohlcv_request.symbol,
            timestamp=row['timestamp'],
            unix_time=row.get('unix_time', int(datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00')).replace(tzinfo=timezone.utc).timestamp())),
            open=row['open'],
            high=row['high'],
            low=row['low'],
            close=row['close'],
            volume=row['volume']
        )
        for row in data
    ]
    
    response = OHLCVResponse(
        symbol=ohlcv_request.symbol,
        timeframe=ohlcv_request.timeframe,
        source_resolution=ohlcv_request.source_resolution,
        start_date=ohlcv_request.start_date.isoformat(),  # Convert to string for response
        end_date=ohlcv_request.end_date.isoformat(),  # Convert to string for response
        data=ohlcv_data,
        count=len(ohlcv_data)
    )
    
    logger.info(
        f"OHLCV data retrieved: {len(ohlcv_data)} records",
        extra={
            "symbol": ohlcv_request.symbol,
            "timeframe": ohlcv_request.timeframe,
            "record_count": len(ohlcv_data),
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    return response

@router.post("/data", response_model=OHLCVResponse)
async def get_ohlcv_data_post(request: Request, ohlcv_request: OHLCVRequest, user_id: str = Depends(verify_token)):
    """Get OHLCV data for specified parameters using POST with request body"""
    return await _get_ohlcv_data_internal(request, ohlcv_request)

@router.get("/instruments")
async def get_instruments_metadata(request: Request, user_id: str = Depends(verify_token)):
    """Get metadata for all available instruments including data ranges"""
    logger.info(
        "Fetching instruments metadata",
        extra={"request_id": getattr(request.state, "request_id", "unknown")}
    )
    
    instrument_service = InstrumentService()
    instruments_metadata = await instrument_service.get_instruments_metadata()
    
    return {
        "count": len(instruments_metadata),
        "instruments": list(instruments_metadata.values()),
        "lastUpdated": instrument_service._instruments_data.get("_updated", "unknown") if instrument_service._instruments_data else "unknown"
    }

@router.get("/instruments/{symbol}")
async def get_instrument_metadata(request: Request, symbol: str, user_id: str = Depends(verify_token)):
    """Get metadata for a specific instrument"""
    logger.info(
        f"Fetching metadata for symbol: {symbol}",
        extra={
            "symbol": symbol,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    instrument_service = InstrumentService()
    metadata = instrument_service.get_instrument_metadata(symbol)
    
    if not metadata:
        logger.warning(
            f"No metadata found for symbol: {symbol}",
            extra={
                "symbol": symbol,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No metadata found for symbol {symbol}"
        )
    
    return metadata

@router.get("/cache/clear")
async def clear_cache(request: Request, user_id: str = Depends(verify_token)):
    """Clear the OHLCV data cache"""
    logger.info(
        "Clearing OHLCV cache",
        extra={"request_id": getattr(request.state, "request_id", "unknown")}
    )
    
    # Clear market data cache using new infrastructure
    market_data_cache._memory_cache.clear()  # Clear in-memory cache
    
    return {"message": "Cache cleared successfully"} 