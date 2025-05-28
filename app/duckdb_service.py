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
            
            # Execute query on the parquet files
            if "{FILES}" in query:
                # For complex queries with placeholder
                full_query = query.replace("{FILES}", files_str)
            else:
                # For simple WHERE clause queries
                full_query = f"SELECT * FROM read_parquet({files_str}) {query}"
            
            result = self.conn.execute(full_query).fetchall()
            
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
            # Construct object name: ohlcv/1m/symbol=ES/date=2025-05-27/ES_2025-05-27.parquet
            object_name = f"ohlcv/{timeframe}/symbol={symbol}/date={current_date.isoformat()}/{symbol}_{current_date.isoformat()}.parquet"
            
            # Check if this file exists
            if await minio_service.check_object_exists(object_name):
                object_names.append(object_name)
            else:
                logger.warning(f"Missing data file: {object_name}")
            
            current_date += timedelta(days=1)
        
        if not object_names:
            raise ValueError(f"No data found for symbol {symbol} between {start_date} and {end_date}")
        
        # Build query based on timeframe
        # Convert dates to unix timestamps (start of day and end of day)
        start_unix = int(datetime.combine(start_date, datetime.min.time()).timestamp())
        end_unix = int(datetime.combine(end_date, datetime.max.time()).timestamp())
        
        if timeframe == "1m":
            # Raw 1-minute data
            query = f"""
                WHERE symbol = '{symbol}'
                AND unix_time >= {start_unix}
                AND unix_time <= {end_unix}
                ORDER BY unix_time ASC
            """
        else:
            # Aggregate to requested timeframe
            interval_seconds = {
                "5m": 300,
                "15m": 900,
                "30m": 1800,
                "1h": 3600,
                "4h": 14400,
                "1d": 86400,
                "1w": 604800
            }.get(timeframe, 86400)
            
            # DuckDB aggregation using unix time
            query = f"""
                SELECT 
                    symbol,
                    (unix_time / {interval_seconds}) * {interval_seconds} as unix_time,
                    MIN(timestamp) as timestamp,
                    FIRST(open ORDER BY unix_time) as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    LAST(close ORDER BY unix_time) as close,
                    SUM(volume) as volume
                FROM read_parquet({{FILES}})
                WHERE symbol = '{symbol}'
                AND unix_time >= {start_unix}
                AND unix_time <= {end_unix}
                GROUP BY symbol, (unix_time / {interval_seconds})
                ORDER BY unix_time ASC
            """
        
        try:
            data = await self.query_parquet_from_minio(object_names, query)
            
            # Convert timestamps to ISO format for JSON serialization
            for row in data:
                if isinstance(row.get('timestamp'), datetime):
                    row['timestamp'] = row['timestamp'].isoformat()
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to get OHLCV data for {symbol}: {e}")
            raise
    
    async def get_available_symbols(self, timeframe: str = "1m") -> List[str]:
        """Get list of available symbols from MinIO"""
        try:
            # List all objects in the timeframe directory
            objects = await minio_service.list_objects(prefix=f"ohlcv/{timeframe}/")
            
            # Extract unique symbols from object names
            symbols = set()
            for obj in objects:
                # Parse symbol from path: ohlcv/1m/symbol=ES/date=2025-05-27/ES_2025-05-27.parquet
                parts = obj['name'].split('/')
                if len(parts) >= 3 and parts[2].startswith('symbol='):
                    symbol = parts[2].replace('symbol=', '')
                    symbols.add(symbol)
            
            return sorted(list(symbols))
            
        except Exception as e:
            logger.error(f"Failed to get available symbols: {e}")
            raise
    
    async def get_available_dates(self, symbol: str, timeframe: str = "1m") -> List[str]:
        """Get list of available dates for a symbol"""
        try:
            # List objects for the specific symbol
            prefix = f"ohlcv/{timeframe}/symbol={symbol}/"
            objects = await minio_service.list_objects(prefix=prefix)
            
            # Extract dates from object names
            dates = []
            for obj in objects:
                # Parse date from path: ohlcv/1m/symbol=ES/date=2025-05-27/ES_2025-05-27.parquet
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