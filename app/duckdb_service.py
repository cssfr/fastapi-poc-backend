"""DuckDB service for querying Parquet files from MinIO"""
import duckdb
import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timedelta
from .minio_client import minio_service, MINIO_BUCKET
import tempfile

logger = logging.getLogger(__name__)

class DuckDBService:
    """Service for querying Parquet files using DuckDB"""
    
    def __init__(self):
        # Initialize DuckDB connection (in-memory)
        self.conn = duckdb.connect(':memory:')
        logger.info("DuckDB initialized")
    
    async def query_parquet_from_minio(self, object_names: List[str], query: str) -> List[Dict[str, Any]]:
        """Query multiple Parquet files from MinIO using DuckDB"""
        if not minio_service.is_available():
            raise RuntimeError("MinIO service not available")
        
        temp_files = []
        try:
            # Download all files to temp location
            for object_name in object_names:
                tmp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
                temp_files.append(tmp_file)
                
                # Get the object stream from MinIO
                stream = minio_service.get_object_stream(object_name, MINIO_BUCKET)
                
                # Write stream to temporary file
                for data in stream:
                    tmp_file.write(data)
                tmp_file.flush()
                tmp_file.close()
                
                stream.close()
                stream.release_conn()
            
            # Build file list for DuckDB query
            file_list = [f"'{tf.name}'" for tf in temp_files]
            files_str = f"[{', '.join(file_list)}]"
            
            # Query all Parquet files together
            result = self.conn.execute(query).fetchall()
            
            # Get column names
            columns = [desc[0] for desc in self.conn.description]
            
            # Convert to list of dicts
            return [dict(zip(columns, row)) for row in result]
            
        except Exception as e:
            logger.error(f"Failed to query Parquet files: {e}")
            raise
        finally:
            # Clean up temp files
            for tf in temp_files:
                try:
                    os.unlink(tf.name)
                except:
                    pass
    
    async def get_ohlcv_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Get OHLCV data for a symbol within date range"""
        
        # Build list of object names for the date range
        object_names = []
        current_date = start_date
        
        while current_date <= end_date:
            # Construct object name: ohlcv_1m/symbol=ES/date=2025-05-25.parquet
            object_name = f"ohlcv_1m/symbol={symbol}/date={current_date.isoformat()}.parquet"
            
            # Check if this file exists
            if await minio_service.check_object_exists(object_name):
                object_names.append(object_name)
            else:
                logger.warning(f"Missing data file: {object_name}")
            
            current_date += timedelta(days=1)
        
        if not object_names:
            raise ValueError(f"No data found for symbol {symbol} between {start_date} and {end_date}")
        
        # Build query based on timeframe
        if timeframe == "1m":
            # Raw 1-minute data
            query = """
                SELECT 
                    symbol,
                    timestamp,
                    unix_time,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM read_parquet({})
                WHERE 
                    symbol = '{}'
                    AND timestamp >= TIMESTAMP '{}'
                    AND timestamp <= TIMESTAMP '{} 23:59:59'
                ORDER BY timestamp ASC
            """.format(files_str, symbol, start_date.isoformat(), end_date.isoformat())
        else:
            # Aggregate to requested timeframe
            interval_map = {
                "5m": 5,
                "15m": 15,
                "30m": 30,
                "1h": 60,
                "4h": 240,
                "1d": 1440,
                "1w": 10080
            }
            
            minutes = interval_map.get(timeframe, 1440)
            
            # DuckDB aggregation query using window functions
            query = """
                WITH ordered_data AS (
                    SELECT 
                        symbol,
                        timestamp,
                        unix_time,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        EXTRACT(EPOCH FROM timestamp)::BIGINT / ({} * 60) AS time_group,
                        ROW_NUMBER() OVER (PARTITION BY symbol, EXTRACT(EPOCH FROM timestamp)::BIGINT / ({} * 60) ORDER BY timestamp ASC) as rn_first,
                        ROW_NUMBER() OVER (PARTITION BY symbol, EXTRACT(EPOCH FROM timestamp)::BIGINT / ({} * 60) ORDER BY timestamp DESC) as rn_last
                    FROM read_parquet({})
                    WHERE 
                        symbol = '{}'
                        AND timestamp >= TIMESTAMP '{}'
                        AND timestamp <= TIMESTAMP '{} 23:59:59'
                ),
                aggregated AS (
                    SELECT 
                        symbol,
                        time_group,
                        MIN(timestamp) as timestamp,
                        MIN(unix_time) as unix_time,
                        MAX(CASE WHEN rn_first = 1 THEN open END) as open,
                        MAX(high) as high,
                        MIN(low) as low,
                        MAX(CASE WHEN rn_last = 1 THEN close END) as close,
                        SUM(volume) as volume
                    FROM ordered_data
                    GROUP BY symbol, time_group
                )
                SELECT 
                    symbol,
                    timestamp,
                    unix_time,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM aggregated
                ORDER BY timestamp ASC
            """.format(minutes, minutes, minutes, files_str, symbol, start_date.isoformat(), end_date.isoformat())
        
        try:
            data = await self.query_parquet_from_minio(object_names, query)
            
            # Convert timestamps to ISO format for JSON serialization
            for row in data:
                if isinstance(row.get('timestamp'), (datetime, date)):
                    row['timestamp'] = row['timestamp'].isoformat()
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to get OHLCV data for {symbol}: {e}")
            raise
    
    async def get_available_symbols(self, timeframe: str = "1m") -> List[str]:
        """Get list of available symbols from MinIO"""
        try:
            # List all objects in the timeframe directory
            objects = await minio_service.list_objects(prefix=f"ohlcv_{timeframe}/")
            
            # Extract unique symbols from object names
            symbols = set()
            for obj in objects:
                # Parse symbol from path: ohlcv_1m/symbol=ES/date=2025-05-25.parquet
                parts = obj['name'].split('/')
                if len(parts) >= 2 and parts[1].startswith('symbol='):
                    symbol = parts[1].replace('symbol=', '')
                    symbols.add(symbol)
            
            return sorted(list(symbols))
            
        except Exception as e:
            logger.error(f"Failed to get available symbols: {e}")
            raise
    
    async def get_available_dates(self, symbol: str, timeframe: str = "1m") -> List[str]:
        """Get list of available dates for a symbol"""
        try:
            # List objects for the specific symbol
            prefix = f"ohlcv_{timeframe}/symbol={symbol}/"
            objects = await minio_service.list_objects(prefix=prefix)
            
            # Extract dates from object names
            dates = []
            for obj in objects:
                # Parse date from filename: date=2025-05-25.parquet
                filename = obj['name'].split('/')[-1]
                if filename.startswith('date=') and filename.endswith('.parquet'):
                    date_str = filename.replace('date=', '').replace('.parquet', '')
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