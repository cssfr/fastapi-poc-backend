from typing import List, Dict, Optional
from datetime import date
import logging
from app.minio_client import MinIOService, MINIO_BUCKET

logger = logging.getLogger(__name__)

class MarketDataRepository:
    def __init__(self, duckdb_conn):
        self.conn = duckdb_conn
    
    async def get_symbols(self, source_resolution: str) -> List[str]:
        """Move symbol query logic from duckdb_service.get_available_symbols"""
        try:
            # List all objects in the source resolution directory
            objects = await MinIOService.list_objects(MINIO_BUCKET, prefix=f"ohlcv/{source_resolution}/")
            
            # Extract unique symbols from object names
            symbols = set()
            for obj in objects:
                if source_resolution == "1Y":
                    # Parse symbol from path: ohlcv/1Y/symbol=BTC/year=2017/BTC_2017.parquet
                    parts = obj['name'].split('/')
                    if len(parts) >= 3 and parts[2].startswith('symbol='):
                        symbol = parts[2].replace('symbol=', '')
                        symbols.add(symbol)
                else:
                    # Parse symbol from path: ohlcv/1m/symbol=DAX/date=2013-10-01/DAX_2013-10-01.parquet
                    parts = obj['name'].split('/')
                    if len(parts) >= 3 and parts[2].startswith('symbol='):
                        symbol = parts[2].replace('symbol=', '')
                        symbols.add(symbol)
            
            return sorted(list(symbols))
            
        except Exception as e:
            logger.error(f"Failed to get available symbols: {e}")
            raise
    
    async def get_available_dates(self, symbol: str, source_resolution: str) -> List[str]:
        """Move date range query from duckdb_service.get_available_dates"""
        try:
            # List objects for the specific symbol
            prefix = f"ohlcv/{source_resolution}/symbol={symbol}/"
            objects = await MinIOService.list_objects(MINIO_BUCKET, prefix=prefix)
            
            # Extract dates or years from object names
            dates = []
            for obj in objects:
                if source_resolution == "1Y":
                    # Parse year from path: ohlcv/1Y/symbol=BTC/year=2017/BTC_2017.parquet
                    parts = obj['name'].split('/')
                    if len(parts) >= 4 and parts[3].startswith('year='):
                        year = parts[3].replace('year=', '')
                        dates.append(year)
                else:
                    # Parse date from path: ohlcv/1m/symbol=DAX/date=2013-10-01/DAX_2013-10-01.parquet
                    parts = obj['name'].split('/')
                    if len(parts) >= 4 and parts[3].startswith('date='):
                        date_str = parts[3].replace('date=', '')
                        dates.append(date_str)
            
            return sorted(dates)
            
        except Exception as e:
            logger.error(f"Failed to get available dates for {symbol}: {e}")
            raise
    
    async def query_ohlcv_raw(self, s3_paths: List[str], symbol: str,
                             start_unix: int, end_unix: int) -> List[Dict]:
        """Raw 1-minute data query"""
        paths_str = "['" + "', '".join(s3_paths) + "']"
        
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
            FROM read_parquet({paths_str})
            WHERE symbol = '{symbol}'
                AND unix_time >= {start_unix}
                AND unix_time <= {end_unix}
            ORDER BY unix_time ASC
        """
        
        logger.debug(f"Executing raw DuckDB query: {query}")
        
        # Execute query directly on S3
        result = self.conn.execute(query).fetchall()
        
        # Get column names
        columns = [desc[0] for desc in self.conn.description]
        
        # Convert to list of dicts
        return [dict(zip(columns, row)) for row in result]
    
    async def query_ohlcv_aggregated(self, s3_paths: List[str], symbol: str,
                                    start_unix: int, end_unix: int,
                                    interval_seconds: int) -> List[Dict]:
        """Aggregated data query"""
        paths_str = "['" + "', '".join(s3_paths) + "']"
        
        query = f"""
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
        
        logger.debug(f"Executing aggregated DuckDB query: {query}")
        
        # Execute query directly on S3
        result = self.conn.execute(query).fetchall()
        
        # Get column names
        columns = [desc[0] for desc in self.conn.description]
        
        # Convert to list of dicts
        return [dict(zip(columns, row)) for row in result] 