from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from typing import List, Optional
from datetime import datetime, timezone
import logging

from app.auth import verify_token
from app.models_ohlcv import OHLCVRequest, OHLCVResponse, OHLCVData
from app.services.market_data_service import MarketDataService
from app.services.storage_service import StorageService
from app.infrastructure.cache import market_data_cache

logger = logging.getLogger(__name__)

# Create market data service instance
market_data_service = MarketDataService()

router = APIRouter(
    prefix="/api/v1/ohlcv",
    tags=["ohlcv"],
    dependencies=[Depends(verify_token)]
)

@router.get("/symbols", response_model=List[str])
async def get_available_symbols(request: Request):
    """Get all available symbols in the dataset"""
    try:
        logger.info(
            "Fetching available symbols",
            extra={"request_id": getattr(request.state, "request_id", "unknown")}
        )
        
        symbols = await market_data_service.get_available_symbols()
        
        logger.info(
            f"Found {len(symbols)} symbols",
            extra={
                "symbol_count": len(symbols),
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        return symbols
        
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch available symbols"
        )

@router.get("/timeframes", response_model=List[str])
async def get_available_timeframes(request: Request):
    """Get all available timeframes"""
    try:
        logger.info(
            "Fetching available timeframes",
            extra={"request_id": getattr(request.state, "request_id", "unknown")}
        )
        
        # Return supported timeframes (static list)
        timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]
        
        logger.info(
            f"Found {len(timeframes)} timeframes",
            extra={
                "timeframe_count": len(timeframes),
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        return timeframes
        
    except Exception as e:
        logger.error(f"Error fetching timeframes: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch available timeframes"
        )

@router.get("/date-range/{symbol}")
async def get_symbol_date_range(
    request: Request,
    symbol: str,
    timeframe: str = Query(..., description="Timeframe (e.g., '1m', '5m', '1h', '1d')")
):
    """Get the available date range for a specific symbol and timeframe"""
    try:
        logger.info(
            f"Fetching date range for symbol: {symbol}, timeframe: {timeframe}",
            extra={
                "symbol": symbol,
                "timeframe": timeframe,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        # Get available dates for the symbol
        dates = await market_data_service.get_available_dates(symbol, "1Y")  # Use 1Y as default
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
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching date range for {symbol}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch date range"
        )

@router.get("/data", response_model=OHLCVResponse)
async def get_ohlcv_data_get(
    request: Request,
    symbol: str = Query(..., description="Trading symbol"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    timeframe: str = Query("1d", pattern="^(1m|5m|15m|30m|1h|4h|1d|1w)$", description="Timeframe"),
    source_resolution: str = Query("1Y", description="Source resolution (1m or 1Y)")
):
    """Get OHLCV data using GET with query parameters"""
    try:
        # Parse dates and create OHLCVRequest object for validation
        from datetime import date
        parsed_start_date = date.fromisoformat(start_date)
        parsed_end_date = date.fromisoformat(end_date)
        
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
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid date format: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error in GET /data endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve OHLCV data"
        )

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
    if ohlcv_request.start_date >= ohlcv_request.end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Start date must be before end date"
        )
    
    # Get data from market data service
    data = await market_data_service.get_ohlcv_data(
        symbol=ohlcv_request.symbol,
        start_date=ohlcv_request.start_date,
        end_date=ohlcv_request.end_date,
        timeframe=ohlcv_request.timeframe,
        source_resolution=ohlcv_request.source_resolution  # Use from request instead of hardcoded
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
            symbol=ohlcv_request.symbol,  # Add missing symbol field
            timestamp=row['timestamp'],
                            unix_time=row.get('unix_time', int(datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00')).replace(tzinfo=timezone.utc).timestamp())),  # Add missing unix_time field (UTC-aware)
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
        source_resolution=ohlcv_request.source_resolution,  # Add source_resolution to response
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
async def get_ohlcv_data_post(request: Request, ohlcv_request: OHLCVRequest):
    """Get OHLCV data for specified parameters using POST with request body"""
    try:
        return await _get_ohlcv_data_internal(request, ohlcv_request)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving OHLCV data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve OHLCV data"
        )

@router.get("/metadata/{symbol}")
async def get_symbol_metadata(request: Request, symbol: str):
    """Get metadata for a specific symbol"""
    try:
        logger.info(
            f"Fetching metadata for symbol: {symbol}",
            extra={
                "symbol": symbol,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        # Basic metadata - this method doesn't exist in original service
        # Return basic symbol information
        metadata = {
            "symbol": symbol,
            "available_timeframes": ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"],
            "source_resolutions": ["1m", "1Y"]
        }
        
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
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching metadata for {symbol}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch symbol metadata"
        )

@router.get("/storage/status")
async def get_storage_status(request: Request):
    """Get storage system status"""
    try:
        logger.info(
            "Checking storage status",
            extra={"request_id": getattr(request.state, "request_id", "unknown")}
        )
        
        service = StorageService()
        status_info = await service.get_bucket_status()
        
        return status_info
        
    except Exception as e:
        logger.error(f"Error checking storage status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to check storage status"
        )

@router.get("/cache/clear")
async def clear_cache(request: Request):
    """Clear the OHLCV data cache"""
    try:
        logger.info(
            "Clearing OHLCV cache",
            extra={"request_id": getattr(request.state, "request_id", "unknown")}
        )
        
        # Clear market data cache using new infrastructure
        market_data_cache._memory_cache.clear()  # Clear in-memory cache
        
        return {"message": "Cache cleared successfully"}
        
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to clear cache"
        ) 