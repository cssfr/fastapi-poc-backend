"""Market data service for business logic for OHLCV data"""
import logging
from typing import List, Dict, Any
from datetime import datetime, date, timedelta, timezone
from app.infrastructure.duckdb_adapter import duckdb_adapter
from app.infrastructure.cache import market_data_cache
from app.infrastructure.performance_monitor import performance_monitor
from app.repositories.market_data_repository import MarketDataRepository
from app.minio_client import MinIOService, MINIO_BUCKET
from app.services.instrument_service import InstrumentService
from app.core.config import settings
from app.core.exceptions import OHLCVRequestTooLargeError, OHLCVResultTooLargeError

logger = logging.getLogger(__name__)

class MarketDataService:
    """Service for OHLCV data business logic, timeframe aggregations, and data validation"""
    
    def __init__(self, repository: MarketDataRepository = None, instrument_service: InstrumentService = None):
        self.repository = repository or MarketDataRepository(duckdb_adapter.conn)
        self.instrument_service = instrument_service or InstrumentService()
    
    def _build_daily_paths(self, symbol: str, start_date: date, end_date: date, source_resolution: str = "1m") -> List[str]:
        """Build list of S3 paths for daily files (original 1m structure)"""
        s3_paths = []
        current_date = start_date
        
        while current_date <= end_date:
            # Build S3 path: s3://dukascopy-node/ohlcv/1m/symbol=DAX/date=2013-10-01/DAX_2013-10-01.parquet
            s3_path = f"s3://{MINIO_BUCKET}/ohlcv/{source_resolution}/symbol={symbol}/date={current_date.isoformat()}/{symbol}_{current_date.isoformat()}.parquet"
            s3_paths.append(s3_path)
            current_date += timedelta(days=1)
        
        return s3_paths
    
    def _build_yearly_paths(self, symbol: str, start_date: date, end_date: date, source_resolution: str = "1Y") -> List[str]:
        """Build list of S3 paths for yearly files (new 1Y structure)"""
        s3_paths = []
        
        # Extract unique years from the date range
        start_year = start_date.year
        end_year = end_date.year
        
        for year in range(start_year, end_year + 1):
            # Build S3 path: s3://dukascopy-node/ohlcv/1Y/symbol=BTC/year=2017/BTC_2017.parquet
            s3_path = f"s3://{MINIO_BUCKET}/ohlcv/{source_resolution}/symbol={symbol}/year={year}/{symbol}_{year}.parquet"
            s3_paths.append(s3_path)
        
        return s3_paths
    
    def _build_s3_paths(self, symbol: str, start_date: date, end_date: date, source_resolution: str = "1m") -> List[str]:
        """Build list of S3 paths for the date range based on source resolution"""
        if source_resolution == "1Y":
            return self._build_yearly_paths(symbol, start_date, end_date, source_resolution)
        else:
            # Default to daily paths for 1m and any other resolution
            return self._build_daily_paths(symbol, start_date, end_date, source_resolution)
    
    def _get_interval_seconds(self, timeframe: str) -> int:
        """Get interval in seconds for a given timeframe"""
        interval_mapping = {
            "1m": 60,       # 1 minute
            "5m": 300,      # 5 minutes
            "15m": 900,     # 15 minutes  
            "30m": 1800,    # 30 minutes
            "1h": 3600,     # 1 hour
            "4h": 14400,    # 4 hours
            "1d": 86400,    # 1 day
            "1w": 604800,   # 1 week
            "1M": 2592000   # 1 month (30 days)
        }
        return interval_mapping.get(timeframe, 86400)  # Default to 1 day
    
    def _validate_timeframe(self, timeframe: str):
        """Validate that timeframe is supported"""
        valid_timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"]
        if timeframe not in valid_timeframes:  # Don't normalize case for 1M
            raise ValueError(f"Invalid timeframe: {timeframe}. Must be one of: {valid_timeframes}")
    
    def _validate_source_resolution(self, source_resolution: str):
        """Validate that source resolution is supported"""
        valid_resolutions = ["1m", "1Y"]
        if source_resolution not in valid_resolutions:
            raise ValueError(f"Invalid source resolution: {source_resolution}. Must be one of: {valid_resolutions}")
    
    def _estimate_record_count(self, start_date: date, end_date: date, timeframe: str) -> int:
        """Estimate the number of records for a given request for smarter validation"""
        days_requested = (end_date - start_date).days
        
        # Records per day by timeframe (fixed the 1m/1M conflict)
        records_per_day = {
            "1m": 1440,    # 1-minute data: 24 * 60 minutes
            "5m": 288,     # 5-minute data: 24 * 12 intervals
            "15m": 96,     # 15-minute data: 24 * 4 intervals 
            "30m": 48,     # 30-minute data: 24 * 2 intervals
            "1h": 24,      # 1-hour data: 24 hours
            "4h": 6,       # 4-hour data: 24 / 4 hours
            "1d": 1,       # 1-day data: 1 per day
            "1w": 0.143,   # 1-week data: 1/7 days
            "1M": 0.033,   # 1-month data: 1/30 days (capital M!)
        }
        
        daily_records = records_per_day.get(timeframe, 1)  # Use timeframe as-is
        estimated_records = int(days_requested * daily_records)
        
        logger.debug(
            f"Record count estimation",
            extra={
                "timeframe": timeframe,
                "days_requested": days_requested,
                "daily_records": daily_records,
                "estimated_records": estimated_records
            }
        )
        
        return estimated_records

    def _validate_request_size(self, start_date: date, end_date: date, timeframe: str):
        """Enhanced validation with both day limits and record estimation"""
        days_requested = (end_date - start_date).days
        estimated_records = self._estimate_record_count(start_date, end_date, timeframe)
        
        # Check estimated record count first
        if estimated_records > settings.max_records_per_request:
            logger.warning(
                f"Request rejected - too many estimated records",
                extra={
                    "timeframe": timeframe,
                    "days_requested": days_requested,
                    "estimated_records": estimated_records,
                    "max_records": settings.max_records_per_request,
                    "rejection_reason": "record_count_exceeded"
                }
            )
            raise OHLCVRequestTooLargeError(timeframe, days_requested, settings.max_records_per_request, estimated_records)
        
        # Check day limits - Fixed: handle 1M case properly
        timeframe_key = timeframe if timeframe == "1M" else timeframe.lower()
        max_days = settings.max_days_by_timeframe.get(timeframe_key, 365)
        
        if days_requested > max_days:
            logger.warning(
                f"Request rejected - too large date range",
                extra={
                    "timeframe": timeframe,
                    "days_requested": days_requested,
                    "max_days": max_days,
                    "estimated_records": estimated_records,
                    "rejection_reason": "date_range_exceeded"
                }
            )
            raise OHLCVRequestTooLargeError(timeframe, days_requested, max_days, estimated_records)
        
        # Log successful validation
        utilization_percent = round((estimated_records / settings.max_records_per_request) * 100, 1)
        logger.info(
            f"Request validation passed",
            extra={
                "timeframe": timeframe,
                "days_requested": days_requested,
                "estimated_records": estimated_records,
                "utilization_percent": utilization_percent
            }
        )
    
    def _auto_adjust_timeframe(self, start_date: date, end_date: date, timeframe: str) -> str:
        """Auto-adjust timeframe for large date ranges to improve performance"""
        if not settings.auto_adjust_timeframe:
            return timeframe
            
        days_requested = (end_date - start_date).days
        original_timeframe = timeframe
        
        # Apply auto-adjustment rules based on date range
        if days_requested > settings.auto_adjust_thresholds["to_1d"] and timeframe in ["1m", "5m", "15m", "30m", "1h"]:
            timeframe = "1d"
        elif days_requested > settings.auto_adjust_thresholds["to_1h"] and timeframe in ["1m", "5m", "15m"]:
            timeframe = "1h"
        elif days_requested > settings.auto_adjust_thresholds["to_15m"] and timeframe == "1m":
            timeframe = "15m"
        
        if timeframe != original_timeframe:
            logger.info(
                f"Auto-adjusted timeframe for performance",
                extra={
                    "original_timeframe": original_timeframe,
                    "adjusted_timeframe": timeframe,
                    "days_requested": days_requested,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                }
            )
        
        return timeframe
    
    def _validate_result_size(self, data: List[Dict[str, Any]], symbol: str, timeframe: str):
        """Validate that result size doesn't exceed limits"""
        record_count = len(data)
        
        if record_count > settings.max_records_per_request:
            logger.warning(
                f"Result rejected - too many records",
                extra={
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "record_count": record_count,
                    "max_records": settings.max_records_per_request
                }
            )
            raise OHLCVResultTooLargeError(record_count, settings.max_records_per_request)
    
    def _optimize_source_resolution(self, timeframe: str, days_requested: int) -> str:
        """Choose optimal source resolution based on timeframe and date range"""
        # For short periods with minute-level timeframes, use 1m source
        if timeframe in ["1m", "5m", "15m"] and days_requested <= 30:
            return "1m"
        
        # For longer periods or larger timeframes, use 1Y source
        return "1Y"
    
    async def get_ohlcv_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1m",
        source_resolution: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Get OHLCV data for a symbol within date range with proper aggregation
        
        Now includes automatic request validation, timeframe adjustment, and result limiting
        """
        
        # Input validation
        self._validate_timeframe(timeframe)
        self._validate_source_resolution(source_resolution)
        
        if start_date > end_date:
            raise ValueError("Start date must be before or equal to end date")
        
        days_requested = (end_date - start_date).days
        
        # 1. VALIDATE REQUEST SIZE - prevent system hangs
        self._validate_request_size(start_date, end_date, timeframe)
        
        # 2. AUTO-ADJUST TIMEFRAME - improve performance for large ranges
        adjusted_timeframe = self._auto_adjust_timeframe(start_date, end_date, timeframe)
        
        # 3. OPTIMIZE SOURCE RESOLUTION - choose best source
        optimized_source = self._optimize_source_resolution(adjusted_timeframe, days_requested)
        
        # Log optimization decisions
        if adjusted_timeframe != timeframe or optimized_source != source_resolution:
            logger.info(
                f"Request optimized for performance",
                extra={
                    "symbol": symbol,
                    "original_timeframe": timeframe,
                    "adjusted_timeframe": adjusted_timeframe,
                    "original_source": source_resolution,
                    "optimized_source": optimized_source,
                    "days_requested": days_requested
                }
            )
        
        # 4. BOUND DATE RANGE - use bounded dates for data retrieval
        bounded_start, bounded_end = await self.instrument_service.bound_date_range(
            symbol, start_date, end_date, optimized_source
        )
        
        # Log if dates were bounded
        if bounded_start != start_date or bounded_end != end_date:
            logger.info(
                f"Date range bounded for {symbol}",
                extra={
                    "requested_start": start_date.isoformat(),
                    "requested_end": end_date.isoformat(),
                    "bounded_start": bounded_start.isoformat(),
                    "bounded_end": bounded_end.isoformat(),
                    "symbol": symbol
                }
            )
        
        if not MinIOService.is_available():
            raise RuntimeError("MinIO service not available")
        
        # 5. BUILD S3 PATHS - use optimized source and bounded dates
        s3_paths = self._build_s3_paths(symbol, bounded_start, bounded_end, optimized_source)
        
        if not s3_paths:
            raise ValueError(f"No data paths generated for symbol {symbol} between {bounded_start} and {bounded_end}")
        
        # Convert bounded dates to unix timestamps for filtering (UTC)
        start_unix = int(datetime.combine(bounded_start, datetime.min.time()).replace(tzinfo=timezone.utc).timestamp())
        end_unix = int(datetime.combine(bounded_end, datetime.max.time()).replace(tzinfo=timezone.utc).timestamp())
        
        # 6. CHECK CACHE - with adjusted parameters
        cache_key_timeframe = adjusted_timeframe
        cached_data = await market_data_cache.get_market_data(symbol, cache_key_timeframe, start_unix, end_unix)
        if cached_data:
            # Validate cached result size
            self._validate_result_size(cached_data, symbol, cache_key_timeframe)
            
            tracking = await performance_monitor.track_query("get_ohlcv_data", symbol)
            await performance_monitor.complete_query(tracking, len(cached_data), cache_hit=True)
            logger.info(f"Retrieved {len(cached_data)} records from cache for {symbol} ({cache_key_timeframe})")
            return cached_data
        
        # 7. EXECUTE QUERY - with performance tracking
        tracking = await performance_monitor.track_query("get_ohlcv_data", symbol)
        
        try:
            # Choose query strategy based on optimized parameters
            if optimized_source == "1m" and adjusted_timeframe == "1m":
                # Raw 1m data from 1m source - no aggregation needed
                data = await self.repository.query_ohlcv_raw(s3_paths, symbol, start_unix, end_unix)
            else:
                # Aggregated data or 1Y source (always needs date filtering)
                interval_seconds = self._get_interval_seconds(adjusted_timeframe)
                data = await self.repository.query_ohlcv_aggregated(s3_paths, symbol, start_unix, end_unix, interval_seconds)
            
            # 8. VALIDATE RESULT SIZE - prevent memory issues
            self._validate_result_size(data, symbol, adjusted_timeframe)
            
            # 9. PROCESS RESULTS - convert timestamps to ISO format
            for row in data:
                if isinstance(row.get('timestamp'), datetime):
                    row['timestamp'] = row['timestamp'].isoformat()
                elif 'unix_time' in row:
                    # Convert unix timestamp back to ISO format (UTC)
                    row['timestamp'] = datetime.fromtimestamp(row['unix_time'], tz=timezone.utc).isoformat()
            
            # 10. CACHE RESULTS - with adjusted timeframe
            await market_data_cache.set_market_data(symbol, cache_key_timeframe, start_unix, end_unix, data)
            
            # 11. COMPLETE PERFORMANCE TRACKING
            data_size = len(str(data).encode('utf-8')) if data else 0
            await performance_monitor.complete_query(tracking, len(data), cache_hit=False, data_size_bytes=data_size)
            
            logger.info(
                f"Successfully retrieved OHLCV data",
                extra={
                    "symbol": symbol,
                    "timeframe": adjusted_timeframe,
                    "source": optimized_source,
                    "record_count": len(data),
                    "days_requested": days_requested,
                    "performance_optimized": adjusted_timeframe != timeframe or optimized_source != source_resolution
                }
            )
            
            return data
            
        except Exception as e:
            logger.error(
                f"Failed to get OHLCV data",
                extra={
                    "symbol": symbol,
                    "timeframe": adjusted_timeframe,
                    "source": optimized_source,
                    "error": str(e),
                    "days_requested": days_requested
                }
            )
            raise
    
    async def get_available_symbols(self, source_resolution: str = "1m") -> List[str]:
        """Get list of available symbols from MinIO source data"""
        self._validate_source_resolution(source_resolution)
        return await self.repository.get_symbols(source_resolution)
    
    async def get_available_dates(self, symbol: str, source_resolution: str = "1m") -> List[str]:
        """Get list of available dates for a symbol from source data"""
        self._validate_source_resolution(source_resolution)
        return await self.repository.get_available_dates(symbol, source_resolution)
    
    async def get_available_years(self, symbol: str, source_resolution: str = "1Y") -> List[str]:
        """Get list of available years for a symbol from yearly source data"""
        if source_resolution != "1Y":
            raise ValueError("get_available_years only works with 1Y source resolution")
        
        return await self.get_available_dates(symbol, source_resolution)
    
    async def performance_test(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1d"
    ) -> Dict[str, Any]:
        """Compare performance between 1m and 1Y sources for the same query"""
        import time
        
        results = {}
        
        # Test 1m source
        try:
            start_time = time.time()
            data_1m = await self.get_ohlcv_data(symbol, start_date, end_date, timeframe, "1m")
            end_time = time.time()
            
            results["1m"] = {
                "duration_seconds": round(end_time - start_time, 3),
                "record_count": len(data_1m),
                "success": True
            }
        except Exception as e:
            results["1m"] = {
                "duration_seconds": None,
                "record_count": 0,
                "success": False,
                "error": str(e)
            }
        
        # Test 1Y source
        try:
            start_time = time.time()
            data_1y = await self.get_ohlcv_data(symbol, start_date, end_date, timeframe, "1Y")
            end_time = time.time()
            
            results["1Y"] = {
                "duration_seconds": round(end_time - start_time, 3),
                "record_count": len(data_1y),
                "success": True
            }
        except Exception as e:
            results["1Y"] = {
                "duration_seconds": None,
                "record_count": 0,
                "success": False,
                "error": str(e)
            }
        
        # Calculate improvement
        if results["1m"]["success"] and results["1Y"]["success"]:
            if results["1m"]["duration_seconds"] > 0:
                improvement = ((results["1m"]["duration_seconds"] - results["1Y"]["duration_seconds"]) / results["1m"]["duration_seconds"]) * 100
                results["performance_improvement_percent"] = round(improvement, 2)
        
        return results
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics"""
        return {
            "performance_summary": performance_monitor.get_performance_summary(),
            "cache_stats": market_data_cache.get_cache_stats()
        } 