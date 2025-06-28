"""Market data service for business logic for OHLCV data"""
import logging
from typing import List, Dict, Any
from datetime import datetime, date, timedelta, timezone
from app.infrastructure.duckdb_adapter import duckdb_adapter
from app.infrastructure.cache import market_data_cache
from app.infrastructure.performance_monitor import performance_monitor
from app.repositories.market_data_repository import MarketDataRepository
from app.minio_client import MinIOService, MINIO_BUCKET

logger = logging.getLogger(__name__)

class MarketDataService:
    """Service for OHLCV data business logic, timeframe aggregations, and data validation"""
    
    def __init__(self, repository: MarketDataRepository = None):
        self.repository = repository or MarketDataRepository(duckdb_adapter.conn)
    
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
        normalized_timeframe = timeframe.lower()
        return interval_mapping.get(normalized_timeframe, 86400)  # Default to 1 day
    
    def _validate_timeframe(self, timeframe: str):
        """Validate that timeframe is supported"""
        valid_timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"]
        normalized_timeframe = timeframe.lower()
        if normalized_timeframe not in valid_timeframes:
            raise ValueError(f"Invalid timeframe: {timeframe}. Must be one of: {valid_timeframes}")
    
    def _validate_source_resolution(self, source_resolution: str):
        """Validate that source resolution is supported"""
        valid_resolutions = ["1m", "1Y"]
        if source_resolution not in valid_resolutions:
            raise ValueError(f"Invalid source resolution: {source_resolution}. Must be one of: {valid_resolutions}")
    
    async def get_ohlcv_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1m",
        source_resolution: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Get OHLCV data for a symbol within date range with proper aggregation
        
        Args:
            symbol: Trading symbol
            start_date: Start date for data
            end_date: End date for data  
            timeframe: Target aggregation timeframe (1m, 5m, 15m, 1h, 1d, etc.)
            source_resolution: Source data folder ("1m" for daily files, "1Y" for yearly files)
        """
        
        # Validation
        self._validate_timeframe(timeframe)
        self._validate_source_resolution(source_resolution)
        
        if start_date > end_date:
            raise ValueError("Start date must be before or equal to end date")
        
        if not MinIOService.is_available():
            raise RuntimeError("MinIO service not available")
        
        # Build paths based on source resolution
        s3_paths = self._build_s3_paths(symbol, start_date, end_date, source_resolution)
        
        if not s3_paths:
            raise ValueError(f"No data paths generated for symbol {symbol} between {start_date} and {end_date}")
        
        # Convert dates to unix timestamps for filtering (UTC)
        start_unix = int(datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc).timestamp())
        end_unix = int(datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc).timestamp())
        
        # Check cache first
        cached_data = await market_data_cache.get_market_data(symbol, timeframe, start_unix, end_unix)
        if cached_data:
            tracking = await performance_monitor.track_query("get_ohlcv_data", symbol)
            await performance_monitor.complete_query(tracking, len(cached_data), cache_hit=True)
            logger.info(f"Retrieved {len(cached_data)} records from cache for {symbol} ({timeframe})")
            return cached_data
        
        # Track performance for database query
        tracking = await performance_monitor.track_query("get_ohlcv_data", symbol)
        
        try:
            # For 1Y source, we always need to filter by date since files contain full years
            # For 1m source, we can optimize by not aggregating if timeframe matches
            if source_resolution == "1m" and timeframe == "1m":
                # Raw 1m data from 1m source - no aggregation needed
                data = await self.repository.query_ohlcv_raw(s3_paths, symbol, start_unix, end_unix)
            else:
                # Aggregated data or 1Y source (always needs date filtering)
                interval_seconds = self._get_interval_seconds(timeframe)
                data = await self.repository.query_ohlcv_aggregated(s3_paths, symbol, start_unix, end_unix, interval_seconds)
            
            # Convert timestamps to ISO format for JSON serialization
            for row in data:
                if isinstance(row.get('timestamp'), datetime):
                    row['timestamp'] = row['timestamp'].isoformat()
                elif 'unix_time' in row:
                    # Convert unix timestamp back to ISO format (UTC)
                    row['timestamp'] = datetime.fromtimestamp(row['unix_time'], tz=timezone.utc).isoformat()
            
            # Cache the results
            await market_data_cache.set_market_data(symbol, timeframe, start_unix, end_unix, data)
            
            # Complete performance tracking
            data_size = len(str(data).encode('utf-8')) if data else 0
            await performance_monitor.complete_query(tracking, len(data), cache_hit=False, data_size_bytes=data_size)
            
            logger.info(f"Retrieved {len(data)} records for {symbol} ({timeframe} from {source_resolution} source)")
            return data
            
        except Exception as e:
            logger.error(f"Failed to get OHLCV data for {symbol}: {e}")
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