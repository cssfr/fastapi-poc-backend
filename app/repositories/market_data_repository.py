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
    
    async def get_multi_symbol_data(self, symbols: List[str], s3_paths_by_symbol: Dict[str, List[str]],
                                   start_unix: int, end_unix: int,
                                   interval_seconds: int) -> Dict[str, List[Dict]]:
        """
        Optimized query for multiple symbols in one DuckDB query
        Uses UNION ALL for parallel execution
        """
        if not symbols:
            return {}
        
        # Build optimized multi-symbol query with UNION ALL
        union_queries = []
        for symbol in symbols:
            s3_paths = s3_paths_by_symbol.get(symbol, [])
            if not s3_paths:
                continue
                
            paths_str = "['" + "', '".join(s3_paths) + "']"
            
            # Each symbol gets its own subquery
            subquery = f"""
                SELECT 
                    '{symbol}' as symbol,
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
            """
            union_queries.append(subquery)
        
        if not union_queries:
            return {}
        
        # Combine all subqueries with UNION ALL for parallel execution
        final_query = " UNION ALL ".join(union_queries) + " ORDER BY symbol, unix_time ASC"
        
        logger.debug(f"Executing multi-symbol DuckDB query for {len(symbols)} symbols")
        
        # Execute the batched query
        result = self.conn.execute(final_query).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        
        # Group results by symbol
        results_by_symbol = {}
        for row in result:
            row_dict = dict(zip(columns, row))
            symbol = row_dict['symbol']
            if symbol not in results_by_symbol:
                results_by_symbol[symbol] = []
            results_by_symbol[symbol].append(row_dict)
        
        return results_by_symbol
    
    async def query_ohlcv_with_projections(self, s3_paths: List[str], symbol: str,
                                          start_unix: int, end_unix: int,
                                          columns_needed: List[str]) -> List[Dict]:
        """
        Optimized query with column projections to reduce data transfer
        Only reads specified columns from Parquet files
        """
        paths_str = "['" + "', '".join(s3_paths) + "']"
        
        # Build projection list - always include necessary columns for filtering
        base_columns = {'symbol', 'unix_time'}
        projection_columns = base_columns.union(set(columns_needed))
        columns_str = ', '.join(sorted(projection_columns))
        
        query = f"""
            SELECT {columns_str}
            FROM read_parquet({paths_str})
            WHERE symbol = '{symbol}'
                AND unix_time >= {start_unix}
                AND unix_time <= {end_unix}
            ORDER BY unix_time ASC
        """
        
        logger.debug(f"Executing projected DuckDB query with columns: {projection_columns}")
        
        # Execute query with projections
        result = self.conn.execute(query).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        
        return [dict(zip(columns, row)) for row in result]
    
    async def query_ohlcv_parallel_batched(self, symbols_and_paths: List[Dict],
                                          start_unix: int, end_unix: int,
                                          interval_seconds: int,
                                          batch_size: int = 5) -> Dict[str, List[Dict]]:
        """
        Execute parallel batched queries for better performance with many symbols
        Processes symbols in batches to avoid query size limits
        """
        all_results = {}
        
        # Process symbols in batches
        for i in range(0, len(symbols_and_paths), batch_size):
            batch = symbols_and_paths[i:i + batch_size]
            
            # Build batch query
            batch_symbols = [item['symbol'] for item in batch]
            batch_paths = {item['symbol']: item['s3_paths'] for item in batch}
            
            # Execute batch
            batch_results = await self.get_multi_symbol_data(
                batch_symbols, batch_paths, start_unix, end_unix, interval_seconds
            )
            
            # Merge results
            all_results.update(batch_results)
            
            logger.debug(f"Completed batch {i//batch_size + 1} with {len(batch)} symbols")
        
        return all_results