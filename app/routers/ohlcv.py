"""OHLCV data endpoints"""
from fastapi import APIRouter, Depends, HTTPException, status, Query, Response
from typing import List, Optional
from datetime import date, datetime, timedelta
from ..auth import verify_token
from ..models_ohlcv import OHLCVRequest, OHLCVResponse, OHLCVData
from ..duckdb_service import duckdb_service
from ..minio_client import minio_service
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/ohlcv",
    tags=["ohlcv"]
)

@router.get("/symbols", response_model=List[str])
async def get_available_symbols(
    timeframe: str = Query(default="1m", pattern="^(1m|5m|15m|30m|1h|4h|1d|1w)$"),
    user_id: str = Depends(verify_token)
):
    """Get list of available symbols"""
    try:
        symbols = await duckdb_service.get_available_symbols(timeframe)
        return symbols
    except Exception as e:
        logger.error(f"Failed to get symbols: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve available symbols"
        )

@router.get("/dates/{symbol}", response_model=List[str])
async def get_available_dates(
    symbol: str,
    timeframe: str = Query(default="1m", pattern="^(1m|5m|15m|30m|1h|4h|1d|1w)$"),
    user_id: str = Depends(verify_token)
):
    """Get list of available dates for a symbol"""
    try:
        dates = await duckdb_service.get_available_dates(symbol.upper(), timeframe)
        return dates
    except Exception as e:
        logger.error(f"Failed to get dates for {symbol}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve dates for symbol {symbol}"
        )

@router.post("/data", response_model=OHLCVResponse)
async def get_ohlcv_data(
    request: OHLCVRequest,
    user_id: str = Depends(verify_token)
):
    """
    Get OHLCV data for a symbol within date range.
    
    Supports multiple timeframes by aggregating 1-minute data on the fly:
    - 1m: Raw data
    - 5m, 15m, 30m: Aggregated from 1m
    - 1h, 4h: Aggregated from 1m
    - 1d, 1w: Aggregated from 1m
    """
    try:
        # For now, retrieve 1m data
        data = await duckdb_service.get_ohlcv_data(
            symbol=request.symbol,
            start_date=request.start_date,
            end_date=request.end_date,
            timeframe="1m"  # Always fetch 1m data
        )
        
        # If requested timeframe is not 1m, aggregate the data
        if request.timeframe != "1m":
            data = await aggregate_ohlcv_data(data, request.timeframe)
        
        # Convert to response format
        ohlcv_data = [
            OHLCVData(
                symbol=row.get("symbol", request.symbol),
                timestamp=row["timestamp"],
                unix_time=int(datetime.fromisoformat(row["timestamp"]).timestamp() * 1000),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"])
            )
            for row in data
        ]
        
        response = OHLCVResponse(
            symbol=request.symbol,
            timeframe=request.timeframe,
            start_date=request.start_date.isoformat(),
            end_date=request.end_date.isoformat(),
            count=len(ohlcv_data),
            data=ohlcv_data
        )
        
        # Set cache headers for 5 minutes
        # Frontend can cache this data to reduce API calls
        return response
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to get OHLCV data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve OHLCV data"
        )

@router.get("/data/{symbol}")
async def get_ohlcv_data_simple(
    symbol: str,
    start_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: date = Query(..., description="End date (YYYY-MM-DD)"),
    timeframe: str = Query(default="1d", pattern="^(1m|5m|15m|30m|1h|4h|1d|1w)$"),
    response: Response = None,
    user_id: str = Depends(verify_token)
):
    """
    Simplified GET endpoint for OHLCV data.
    Returns data in a format optimized for charting libraries.
    """
    request = OHLCVRequest(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        timeframe=timeframe
    )
    
    ohlcv_response = await get_ohlcv_data(request, user_id)
    
    # Set cache headers
    # Cache for 5 minutes for recent data, longer for historical
    if end_date < date.today() - timedelta(days=7):
        # Historical data can be cached longer
        response.headers["Cache-Control"] = "public, max-age=3600"  # 1 hour
    else:
        # Recent data cached for shorter time
        response.headers["Cache-Control"] = "public, max-age=300"  # 5 minutes
    
    # Return simplified format for charting libraries
    # Most charting libraries expect arrays of [timestamp, open, high, low, close, volume]
    chart_data = {
        "symbol": ohlcv_response.symbol,
        "timeframe": ohlcv_response.timeframe,
        "data": [
            [
                point.unix_time,  # Unix timestamp in milliseconds
                point.open,
                point.high,
                point.low,
                point.close,
                point.volume
            ]
            for point in ohlcv_response.data
        ]
    }
    
    return chart_data

async def aggregate_ohlcv_data(data: List[dict], timeframe: str) -> List[dict]:
    """
    Aggregate 1-minute OHLCV data to higher timeframes.
    This is a placeholder - in production, this should be done in DuckDB for efficiency.
    """
    # For now, return the 1m data
    # TODO: Implement proper aggregation logic in DuckDB
    logger.warning(f"Aggregation to {timeframe} not yet implemented, returning 1m data")
    return data