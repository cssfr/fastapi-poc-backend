"""DuckDB service for querying Parquet files from MinIO"""
import duckdb
import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timedelta
from .minio_client import MinIOService, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
import tempfile

logger = logging.getLogger(__name__)

class DuckDBService:
    """Service for querying Parquet files using DuckDB"""
    
    def __init__(self):
        # Initialize DuckDB connection (in-memory, read-only)
        self.conn = duckdb.connect(':memory:', read_only=False)  # Can't be read-only for in-memory
        
        # Configure S3 settings for DuckDB
        if MinIOService.is_available():
            self._configure_s3_settings()
        
        logger.info("DuckDB initialized with S3 configuration")
    
    def _configure_s3_settings(self):
        """Configure DuckDB S3 settings for MinIO"""
        try:
            # Install and load httpfs extension for S3 access
            self.conn.execute("INSTALL httpfs;")
            self.conn.execute("LOAD httpfs;")
            
            # Configure S3 settings for MinIO
            self.conn.execute(f"SET s3_region='us-east-1';")
            self.conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
            self.conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
            self.conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
            self.conn.execute("SET s3_use_ssl=false;")  # Adjust based on your MinIO setup
            self.conn.execute("SET s3_url_style='path';")
            
            logger.info("DuckDB S3 configuration completed")
        except Exception as e:
            logger.error(f"Failed to configure DuckDB S3 settings: {e}")
            raise
    
    def _build_s3_paths(self, symbol: str, start_date: date, end_date: date, source_resolution: str = "1m") -> List[str]:
        """Build list of S3 paths for the date range from source data"""
        s3_paths = []
        current_date = start_date
        
        while current_date <= end_date:
            # Build S3 path: s3://dukascopy-node/ohlcv/1m/symbol=DAX/date=2013-10-01/DAX_2013-10-01.parquet
            # source_resolution is the folder name (currently only "1m")
            # timeframe parameter is used for aggregation logic
            s3_path = f"s3://{MINIO_BUCKET}/ohlcv/{source_resolution}/symbol={symbol}/date={current_date.isoformat()}/{symbol}_{current_date.isoformat()}.parquet"
            s3_paths.append(s3_path)
            current_date += timedelta(days=1)
        
        return s3_paths
    
    async def get_ohlcv_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1m",
        source_resolution: str = "1m"  # Source data resolution (folder name)
    ) -> List[Dict[str, Any]]:
        """Get OHLCV data for a symbol within date range with proper aggregation
        
        Args:
            symbol: Trading symbol
            start_date: Start date for data
            end_date: End date for data  
            timeframe: Target aggregation timeframe (1m, 5m, 15m, 1h, 1d, etc.)
            source_resolution: Source data folder (currently only "1m")
        """
        
        if not MinIOService.is_available():
            raise RuntimeError("MinIO service not available")
        
        # Build paths from source data (currently only 1m available)
        s3_paths = self._build_s3_paths(symbol, start_date, end_date, source_resolution)
        
        if not s3_paths:
            raise ValueError(f"No data paths generated for symbol {symbol} between {start_date} and {end_date}")
        
        # Convert dates to unix timestamps
        start_unix = int(datetime.combine(start_date, datetime.min.time()).timestamp())
        end_unix = int(datetime.combine(end_date, datetime.max.time()).timestamp())
        
        try:
            if timeframe == source_resolution:
                # Raw data - no aggregation needed (e.g., requesting 1m from 1m source)
                query = self._build_raw_query(s3_paths, symbol, start_unix, end_unix)
            else:
                # Aggregated data (e.g., requesting 5m/1h/1d from 1m source)
                query = self._build_aggregated_query(s3_paths, symbol, start_unix, end_unix, timeframe)
            
            logger.debug(f"Executing DuckDB query for {timeframe} from {source_resolution} source: {query}")
            
            # Execute query directly on S3
            result = self.conn.execute(query).fetchall()
            
            # Get column names
            columns = [desc[0] for desc in self.conn.description]
            
            # Convert to list of dicts
            data = [dict(zip(columns, row)) for row in result]
            
            # Convert timestamps to ISO format for JSON serialization
            for row in data:
                if isinstance(row.get('timestamp'), datetime):
                    row['timestamp'] = row['timestamp'].isoformat()
                elif 'unix_time' in row:
                    # Convert unix timestamp back to ISO format
                    row['timestamp'] = datetime.fromtimestamp(row['unix_time']).isoformat()
            
            logger.info(f"Retrieved {len(data)} records for {symbol} ({timeframe} from {source_resolution} source)")
            return data
            
        except Exception as e:
            logger.error(f"Failed to get OHLCV data for {symbol}: {e}")
            raise
    
    def _build_raw_query(self, s3_paths: List[str], symbol: str, start_unix: int, end_unix: int) -> str:
        """Build query for raw 1-minute data"""
        paths_str = "['" + "', '".join(s3_paths) + "']"
        
        return f"""
            SELECT 
                symbol,
                timestamp,
                unix_time,
                open,
                high,
                low,
                close,
                volume
            FROM read_parquet({paths_str})
            WHERE symbol = '{symbol}'
                AND unix_time >= {start_unix}
                AND unix_time <= {end_unix}
            ORDER BY unix_time ASC
        """
    
    def _build_aggregated_query(self, s3_paths: List[str], symbol: str, start_unix: int, end_unix: int, timeframe: str) -> str:
        """Build query for aggregated data using custom floor approach"""
        paths_str = "['" + "', '".join(s3_paths) + "']"
        
        # Calculate interval in seconds
        interval_seconds = {
            "5m": 300,      # 5 minutes
            "15m": 900,     # 15 minutes  
            "30m": 1800,    # 30 minutes
            "1h": 3600,     # 1 hour
            "4h": 14400,    # 4 hours
            "1d": 86400,    # 1 day
            "1w": 604800    # 1 week
        }.get(timeframe, 86400)
        
        return f"""
            SELECT 
                symbol,
                to_timestamp(bucket_start) as timestamp,
                bucket_start as unix_time,
                first(open ORDER BY unix_time) as open,
                max(high) as high,
                min(low) as low,
                last(close ORDER BY unix_time) as close,
                sum(volume) as volume
            FROM (
                SELECT 
                    symbol,
                    timestamp,
                    unix_time,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    (unix_time // {interval_seconds}) * {interval_seconds} as bucket_start
                FROM read_parquet({paths_str})
                WHERE symbol = '{symbol}'
                    AND unix_time >= {start_unix}
                    AND unix_time <= {end_unix}
            ) 
            GROUP BY symbol, bucket_start
            ORDER BY bucket_start ASC
        """
    
    async def get_available_symbols(self, source_resolution: str = "1m") -> List[str]:
        """Get list of available symbols from MinIO source data"""
        try:
            # List all objects in the source resolution directory
            objects = await MinIOService.list_objects(MINIO_BUCKET, prefix=f"ohlcv/{source_resolution}/")
            
            # Extract unique symbols from object names
            symbols = set()
            for obj in objects:
                # Parse symbol from path: ohlcv/1m/symbol=DAX/date=2013-10-01/DAX_2013-10-01.parquet
                parts = obj['name'].split('/')
                if len(parts) >= 3 and parts[2].startswith('symbol='):
                    symbol = parts[2].replace('symbol=', '')
                    symbols.add(symbol)
            
            return sorted(list(symbols))
            
        except Exception as e:
            logger.error(f"Failed to get available symbols: {e}")
            raise
    
    async def get_available_dates(self, symbol: str, source_resolution: str = "1m") -> List[str]:
        """Get list of available dates for a symbol from source data"""
        try:
            # List objects for the specific symbol
            prefix = f"ohlcv/{source_resolution}/symbol={symbol}/"
            objects = await MinIOService.list_objects(MINIO_BUCKET, prefix=prefix)
            
            # Extract dates from object names
            dates = []
            for obj in objects:
                # Parse date from path: ohlcv/1m/symbol=DAX/date=2013-10-01/DAX_2013-10-01.parquet
                parts = obj['name'].split('/')
                if len(parts) >= 4 and parts[3].startswith('date='):
                    date_str = parts[3].replace('date=', '')
                    dates.append(date_str)
            
            return sorted(dates)
            
        except Exception as e:
            logger.error(f"Failed to get available dates for {symbol}: {e}")
            raise
    
    def close(self):
        """Close DuckDB connection"""
        if self.conn:
            self.conn.close()

# Create global DuckDB service instance
duckdb_service = DuckDBService()