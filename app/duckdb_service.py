import duckdb
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
import pytz
import os

from app.minio_client import MinioClient

class DuckDBService:
    def __init__(self):
        # Create read-only connection with S3 configuration
        self.conn = duckdb.connect(":memory:", read_only=False)  # Need write for S3 config
        
        # Configure S3 access
        self._configure_s3_access()
        
        self.minio_client = MinioClient()
        
    def _configure_s3_access(self):
        """Configure DuckDB for direct S3 access"""
        # Set S3 endpoint and credentials
        self.conn.execute(f"""
            SET s3_endpoint = '{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}';
            SET s3_access_key_id = '{os.getenv('MINIO_ACCESS_KEY', 'minioadmin')}';
            SET s3_secret_access_key = '{os.getenv('MINIO_SECRET_KEY', 'minioadmin')}';
            SET s3_use_ssl = false;
            SET s3_url_style = 'path';
        """)
        
    def construct_s3_paths(self, symbol: str, start_date: date, end_date: date, source_timeframe: str = "1m") -> List[str]:
        """Construct S3 paths for the date range"""
        paths = []
        current = start_date
        while current <= end_date:
            path = f"s3://dukascopy-node/ohlcv/{source_timeframe}/symbol={symbol}/date={current.isoformat()}/{symbol}_{current.isoformat()}.parquet"
            paths.append(f"'{path}'")  # Quote for SQL
            current += timedelta(days=1)
        return paths
        
    async def query_parquet_from_s3(self, symbol: str, start_date: date, end_date: date, timeframe: str = "1m") -> List[Dict[str, Any]]:
        """Query parquet files directly from S3 with optional aggregation"""
        try:
            # Construct S3 paths
            s3_paths = self.construct_s3_paths(symbol, start_date, end_date)
            
            if not s3_paths:
                return []
            
            # Join paths for SQL query
            files_list = f"[{', '.join(s3_paths)}]"
            
            # Build query based on timeframe
            if timeframe == "1m":
                # Raw 1-minute data - no aggregation needed
                query = f"""
                    SELECT 
                        symbol,
                        timestamp,
                        unix_time,
                        open,
                        high,
                        low,
                        close,
                        volume
                    FROM read_parquet({files_list})
                    WHERE symbol = '{symbol}'
                    ORDER BY unix_time ASC
                """
            else:
                # Aggregate to requested timeframe using unix_time
                interval_seconds = {
                    "5m": 300,
                    "15m": 900,
                    "30m": 1800,
                    "1h": 3600,
                    "4h": 14400,
                    "1d": 86400,
                    "1w": 604800
                }.get(timeframe, 86400)
                
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
                    FROM read_parquet({files_list})
                    WHERE symbol = '{symbol}'
                    GROUP BY symbol, (unix_time / {interval_seconds})
                    ORDER BY unix_time ASC
                """
            
            # Execute query
            result = self.conn.execute(query).fetchall()
            
            # Get column names
            columns = [desc[0] for desc in self.conn.description]
            
            # Convert to list of dicts
            return [dict(zip(columns, row)) for row in result]
            
        except Exception as e:
            print(f"Error querying S3: {e}")
            return []
    
    async def get_ohlcv_data(self, symbol: str, start_date: str, end_date: str, timeframe: str = "1d") -> Dict[str, Any]:
        """Get OHLCV data for a symbol within date range"""
        try:
            # Parse dates
            start = datetime.fromisoformat(start_date).date()
            end = datetime.fromisoformat(end_date).date()
            
            # Query data directly from S3
            data = await self.query_parquet_from_s3(symbol, start, end, timeframe)
            
            # Format response
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "start_date": start_date,
                "end_date": end_date,
                "count": len(data),
                "data": data
            }
            
        except Exception as e:
            print(f"Error getting OHLCV data: {e}")
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "start_date": start_date,
                "end_date": end_date,
                "count": 0,
                "data": [],
                "error": str(e)
            }
    
    async def get_available_symbols(self, timeframe: str = "1m") -> List[str]:
        """Get list of available symbols"""
        try:
            # List all objects in the timeframe directory
            objects = await self.minio_client.list_objects(prefix=f"ohlcv/{timeframe}/")
            
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
            print(f"Error getting available symbols: {e}")
            return []
    
    async def get_available_dates(self, symbol: str, timeframe: str = "1m") -> List[str]:
        """Get list of available dates for a symbol"""
        try:
            # List objects for the specific symbol
            prefix = f"ohlcv/{timeframe}/symbol={symbol}/"
            objects = await self.minio_client.list_objects(prefix=prefix)
            
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
            print(f"Error getting available dates: {e}")
            return []