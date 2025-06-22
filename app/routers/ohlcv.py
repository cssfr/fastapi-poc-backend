"""OHLCV data endpoints"""
from fastapi import APIRouter, Depends, HTTPException, status, Query, Response
from typing import List, Optional, Dict, Any
from datetime import date, datetime, timedelta
from ..auth import verify_token
from ..models_ohlcv import OHLCVRequest, OHLCVResponse, OHLCVData, InstrumentMetadata, InstrumentsResponse, DataRange
from ..duckdb_service import duckdb_service
from ..minio_client import minio_service
from ..instrument_metadata_service import instrument_metadata_service
import logging
import asyncio

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/ohlcv",
    tags=["ohlcv"]
)

# Cache for data boundaries - expires every hour
_data_boundaries_cache = {}
_cache_timestamp = None
CACHE_DURATION = 3600  # 1 hour in seconds

async def get_data_boundaries_for_symbol(symbol: str, source_resolution: str = "1m") -> Dict[str, str]:
    """Get earliest and latest available dates for a symbol"""
    try:
        dates = await duckdb_service.get_available_dates(symbol, source_resolution)
        if not dates:
            return {"earliest": None, "latest": None}
        
        if source_resolution == "1Y":
            # For yearly data, convert years to date ranges
            years = sorted([int(d) for d in dates])
            return {
                "earliest": f"{years[0]}-01-01",
                "latest": f"{years[-1]}-12-31"
            }
        else:
            # For daily data, dates are already in YYYY-MM-DD format
            sorted_dates = sorted(dates)
            return {
                "earliest": sorted_dates[0],
                "latest": sorted_dates[-1]
            }
    except Exception as e:
        logger.warning(f"Failed to get data boundaries for {symbol}: {e}")
        return {"earliest": None, "latest": None}

async def get_cached_data_boundaries() -> Dict[str, Dict[str, str]]:
    """Get cached data boundaries for all symbols"""
    global _data_boundaries_cache, _cache_timestamp
    
    current_time = datetime.utcnow().timestamp()
    
    # Check if cache is valid
    if (_cache_timestamp and 
        current_time - _cache_timestamp < CACHE_DURATION and 
        _data_boundaries_cache):
        return _data_boundaries_cache
    
    # Refresh cache
    logger.info("Refreshing data boundaries cache...")
    
    try:
        # Get available symbols from both sources
        symbols_1m = await duckdb_service.get_available_symbols("1m")
        symbols_1y = await duckdb_service.get_available_symbols("1Y")
        all_symbols = set(symbols_1m + symbols_1y)
        
        # Get boundaries for each symbol from both sources concurrently
        tasks = []
        symbol_list = list(all_symbols)
        
        for symbol in symbol_list:
            tasks.append(get_data_boundaries_for_symbol(symbol, "1m"))
            tasks.append(get_data_boundaries_for_symbol(symbol, "1Y"))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        boundaries = {}
        for i, symbol in enumerate(symbol_list):
            boundaries_1m = results[i * 2] if not isinstance(results[i * 2], Exception) else {"earliest": None, "latest": None}
            boundaries_1y = results[i * 2 + 1] if not isinstance(results[i * 2 + 1], Exception) else {"earliest": None, "latest": None}
            
            # Combine boundaries from both sources
            earliest_dates = [d for d in [boundaries_1m.get("earliest"), boundaries_1y.get("earliest")] if d]
            latest_dates = [d for d in [boundaries_1m.get("latest"), boundaries_1y.get("latest")] if d]
            
            boundaries[symbol] = {
                "earliest": min(earliest_dates) if earliest_dates else None,
                "latest": max(latest_dates) if latest_dates else None,
                "sources": {
                    "1m": boundaries_1m,
                    "1Y": boundaries_1y
                }
            }
        
        _data_boundaries_cache = boundaries
        _cache_timestamp = current_time
        
        logger.info(f"Data boundaries cache refreshed for {len(boundaries)} symbols")
        return boundaries
        
    except Exception as e:
        logger.error(f"Failed to refresh data boundaries cache: {e}")
        return _data_boundaries_cache or {}

@router.get("/instruments", response_model=InstrumentsResponse)
async def get_instruments_metadata(
    include_boundaries: bool = Query(default=True, description="Include data range boundaries"),
    source_resolution: str = Query(default="both", description="Source resolution to check (1m, 1Y, or both)"),
    user_id: str = Depends(verify_token)
) -> InstrumentsResponse:
    """
    Get comprehensive metadata for all available instruments from external sources.
    
    Metadata is loaded from MinIO at: metadata/instruments.json
    Data boundaries are determined by scanning actual data files.
    """
    try:
        # Get available symbols from data files
        if source_resolution == "both":
            symbols_1m = await duckdb_service.get_available_symbols("1m")
            symbols_1y = await duckdb_service.get_available_symbols("1Y")
            all_symbols = sorted(set(symbols_1m + symbols_1y))
            available_sources = ["1m", "1Y"]
        elif source_resolution in ["1m", "1Y"]:
            all_symbols = await duckdb_service.get_available_symbols(source_resolution)
            available_sources = [source_resolution]
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="source_resolution must be '1m', '1Y', or 'both'"
            )
        
        # Get instrument metadata from external source
        all_metadata = await instrument_metadata_service.get_all_metadata()
        
        # Get data boundaries if requested
        boundaries = {}
        if include_boundaries:
            boundaries = await get_cached_data_boundaries()
        
        # Build instrument metadata
        instruments = []
        
        for symbol in all_symbols:
            # Get metadata from external source
            metadata = await instrument_metadata_service.get_metadata(symbol)
            
            # Get data boundaries
            data_range = None
            if include_boundaries and symbol in boundaries:
                boundary = boundaries[symbol]
                data_range = DataRange(
                    earliest=boundary.get("earliest"),
                    latest=boundary.get("latest"),
                    sources=boundary.get("sources", {})
                )
            
            # Build instrument object
            instrument = InstrumentMetadata(
                symbol=symbol,
                exchange=metadata["exchange"],
                market=metadata["market"],
                name=metadata["name"],
                shortName=metadata["shortName"],
                ticker=metadata["ticker"],
                type=metadata["type"],
                currency=metadata["currency"],
                description=metadata["description"],
                sector=metadata["sector"],
                country=metadata["country"],
                dataRange=data_range,
                availableTimeframes=["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"],
                sourceResolutions=available_sources
            )
            
            instruments.append(instrument)
        
        # Get cache information
        metadata_cache_info = instrument_metadata_service.get_cache_info()
        
        return InstrumentsResponse(
            count=len(instruments),
            instruments=instruments,
            lastUpdated=datetime.utcnow().isoformat(),
            cacheInfo={
                "boundariesCached": include_boundaries and bool(_cache_timestamp),
                "boundariesCacheAge": int(datetime.utcnow().timestamp() - _cache_timestamp) if _cache_timestamp else None,
                "boundariesCacheDuration": CACHE_DURATION,
                "metadataCache": metadata_cache_info
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get instruments metadata: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve instruments metadata"
        )

@router.post("/instruments/refresh-cache")
async def refresh_instruments_cache(user_id: str = Depends(verify_token)):
    """Force refresh of both metadata and data boundaries caches"""
    global _data_boundaries_cache, _cache_timestamp
    
    try:
        # Refresh metadata cache
        metadata_refreshed = await instrument_metadata_service.refresh_cache()
        
        # Refresh boundaries cache
        _cache_timestamp = None
        _data_boundaries_cache = {}
        boundaries = await get_cached_data_boundaries()
        
        return {
            "status": "success",
            "message": "Caches refreshed successfully",
            "metadataRefreshed": metadata_refreshed,
            "boundariesRefreshed": bool(boundaries),
            "symbolCount": len(boundaries),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to refresh caches: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to refresh caches"
        )

@router.post("/instruments/metadata")
async def upload_instruments_metadata(
    metadata: Dict[str, Dict[str, Any]],
    user_id: str = Depends(verify_token)
):
    """Upload new instrument metadata to MinIO (admin function)"""
    try:
        success = await instrument_metadata_service.upload_metadata(metadata)
        
        if success:
            return {
                "status": "success",
                "message": f"Metadata uploaded for {len(metadata)} instruments",
                "instrumentCount": len(metadata),
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to upload metadata"
            )
            
    except Exception as e:
        logger.error(f"Failed to upload metadata: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to upload metadata"
        )

@router.get("/symbols", response_model=List[str])
async def get_available_symbols(
    source_resolution: str = Query(default="1m", description="Source data resolution (1m or 1Y)"),
    user_id: str = Depends(verify_token)
):
    """Get list of available symbols from source data"""
    try:
        if source_resolution not in ["1m", "1Y"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="source_resolution must be either '1m' or '1Y'"
            )
        
        symbols = await duckdb_service.get_available_symbols(source_resolution)
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
    source_resolution: str = Query(default="1m", description="Source data resolution (1m or 1Y)"),
    user_id: str = Depends(verify_token)
):
    """Get list of available dates/years for a symbol from source data"""
    try:
        if source_resolution not in ["1m", "1Y"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="source_resolution must be either '1m' or '1Y'"
            )
        
        dates = await duckdb_service.get_available_dates(symbol.upper(), source_resolution)
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
    source_resolution: str = Query(default="1m", description="Source data resolution (1m or 1Y)"),
    user_id: str = Depends(verify_token)
):
    """
    Get OHLCV data for a symbol within date range.
    
    Args:
        request: OHLCV request parameters (symbol, dates, timeframe)
        source_resolution: Source data folder ("1m" for daily files, "1Y" for yearly files)
    
    The timeframe in the request specifies the desired aggregation:
    - 1m: Raw data (if source_resolution is also 1m)
    - 5m, 15m, 30m: Aggregated from source using floor(unix_time / interval)
    - 1h, 4h: Aggregated from source using floor(unix_time / interval)
    - 1d, 1w: Aggregated from source using floor(unix_time / interval)
    """
    try:
        if source_resolution not in ["1m", "1Y"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="source_resolution must be either '1m' or '1Y'"
            )
        
        # Get data with proper aggregation
        data = await duckdb_service.get_ohlcv_data(
            symbol=request.symbol,
            start_date=request.start_date,
            end_date=request.end_date,
            timeframe=request.timeframe,
            source_resolution=source_resolution
        )
        
        # Convert to response format
        ohlcv_data = [
            OHLCVData(
                symbol=row.get("symbol", request.symbol),
                timestamp=row["timestamp"],
                unix_time=int(row.get("unix_time", 0)),
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
    source_resolution: str = Query(default="1m", description="Source data resolution (1m or 1Y)"),
    response: Response = None,
    user_id: str = Depends(verify_token)
):
    """
    Simplified GET endpoint for OHLCV data with proper aggregation.
    Returns data in a format optimized for charting libraries.
    
    All timeframes are properly aggregated from source data:
    - Uses floor(unix_time / interval_seconds) for precise time bucketing
    - OHLC aggregation: first(open), max(high), min(low), last(close)
    - Volume aggregation: sum(volume)
    - Source resolution specifies which folder to read from ("1m" or "1Y")
    """
    if source_resolution not in ["1m", "1Y"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="source_resolution must be either '1m' or '1Y'"
        )
    
    request = OHLCVRequest(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        timeframe=timeframe
    )
    
    ohlcv_response = await get_ohlcv_data(request, source_resolution, user_id)
    
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
        "source_resolution": source_resolution,
        "start_date": ohlcv_response.start_date,
        "end_date": ohlcv_response.end_date,
        "count": ohlcv_response.count,
        "data": [
            [
                point.unix_time,  # Unix timestamp
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

@router.get("/performance-test/{symbol}")
async def performance_test(
    symbol: str,
    start_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: date = Query(..., description="End date (YYYY-MM-DD)"),
    timeframe: str = Query(default="1d", pattern="^(1m|5m|15m|30m|1h|4h|1d|1w)$"),
    user_id: str = Depends(verify_token)
) -> Dict[str, Any]:
    """
    Performance test comparing 1m vs 1Y source resolution for the same query.
    
    Returns timing information and performance metrics for both approaches.
    Useful for determining which source resolution is more efficient for different scenarios.
    """
    try:
        logger.info(f"Running performance test for {symbol} from {start_date} to {end_date} ({timeframe})")
        
        results = await duckdb_service.performance_test(
            symbol=symbol.upper(),
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe
        )
        
        # Add metadata about the test
        results["test_metadata"] = {
            "symbol": symbol.upper(),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "timeframe": timeframe,
            "date_range_days": (end_date - start_date).days + 1,
            "test_timestamp": datetime.utcnow().isoformat()
        }
        
        return results
        
    except Exception as e:
        logger.error(f"Performance test failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Performance test failed"
        )

@router.get("/storage-info")
async def get_storage_info(
    source_resolution: str = Query(default="1m", description="Source data resolution (1m or 1Y)"),
    user_id: str = Depends(verify_token)
) -> Dict[str, Any]:
    """Get information about the storage structure for a given resolution"""
    try:
        if source_resolution not in ["1m", "1Y"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="source_resolution must be either '1m' or '1Y'"
            )
        
        info = await minio_service.get_storage_structure_info(source_resolution)
        return info
        
    except Exception as e:
        logger.error(f"Failed to get storage info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve storage information"
        )

@router.get("/storage-comparison")
async def compare_storage_structures(
    user_id: str = Depends(verify_token)
) -> Dict[str, Any]:
    """Compare storage structures between 1m and 1Y resolutions"""
    try:
        comparison = await minio_service.compare_storage_structures()
        return comparison
        
    except Exception as e:
        logger.error(f"Failed to compare storage structures: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to compare storage structures"
        )