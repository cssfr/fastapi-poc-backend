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
        """Raw 1-minute data query with robust missing file handling"""
        
        if not s3_paths:
            logger.warning(
                f"No S3 paths provided for raw query",
                extra={"symbol": symbol}
            )
            return []
        
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
        
        logger.debug(
            f"Executing raw DuckDB query",
            extra={
                "symbol": symbol,
                "paths_count": len(s3_paths),
                "query_type": "raw"
            }
        )
        
        try:
            # Execute query directly on S3
            result = self.conn.execute(query).fetchall()
            
            # Get column names
            columns = [desc[0] for desc in self.conn.description]
            
            # Convert to list of dicts
            data = [dict(zip(columns, row)) for row in result]
            
            logger.info(
                f"Raw query successful",
                extra={
                    "symbol": symbol,
                    "records_returned": len(data),
                    "paths_queried": len(s3_paths),
                    "query_success": True
                }
            )
            
            return data
            
        except Exception as e:
            error_message = str(e).lower()
            
            # Handle specific missing file errors gracefully
            if "404" in error_message or "not found" in error_message:
                logger.warning(
                    f"Some data files not found for raw query - attempting partial data recovery",
                    extra={
                        "symbol": symbol,
                        "paths_attempted": len(s3_paths),
                        "error_type": "missing_files",
                        "first_path": s3_paths[0] if s3_paths else None,
                        "last_path": s3_paths[-1] if len(s3_paths) > 1 else None,
                        "partial_recovery_attempted": True
                    }
                )
                
                # For raw queries, try individual daily files
                return await self._attempt_partial_raw_recovery(
                    s3_paths, symbol, start_unix, end_unix
                )
            else:
                # Log other types of errors with full context
                logger.error(
                    f"Raw query failed with non-missing-file error",
                    extra={
                        "symbol": symbol,
                        "paths_attempted": len(s3_paths),
                        "error_type": "query_execution_error",
                        "error_message": str(e)
                    },
                    exc_info=True
                )
                raise

    async def query_ohlcv_aggregated(self, s3_paths: List[str], symbol: str,
                                    start_unix: int, end_unix: int,
                                    interval_seconds: int) -> List[Dict]:
        """Aggregated data query with robust missing file handling"""
        
        if not s3_paths:
            logger.warning(
                f"No S3 paths provided for aggregated query",
                extra={"symbol": symbol, "interval_seconds": interval_seconds}
            )
            return []
        
        paths_str = "['" + "', '".join(s3_paths) + "']"
        
        # Handle yearly aggregation differently - use actual first timestamp per year
        if interval_seconds == 31536000:  # 1Y = 31536000 seconds
            query = f"""
                SELECT 
                    symbol,
                    to_timestamp(first_timestamp) as timestamp,
                    first_timestamp as unix_time,
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
                        open, high, low, close, volume,
                        EXTRACT(YEAR FROM to_timestamp(unix_time)) as year_bucket,
                        min(unix_time) OVER (PARTITION BY EXTRACT(YEAR FROM to_timestamp(unix_time))) as first_timestamp
                    FROM read_parquet({paths_str})
                    WHERE symbol = '{symbol}'
                        AND EXTRACT(YEAR FROM to_timestamp(unix_time)) >= EXTRACT(YEAR FROM to_timestamp({start_unix}))
                        AND EXTRACT(YEAR FROM to_timestamp(unix_time)) <= EXTRACT(YEAR FROM to_timestamp({end_unix}))
                ) 
                GROUP BY symbol, year_bucket, first_timestamp
                ORDER BY year_bucket ASC
            """
            
            logger.debug(
                f"Executing yearly aggregated DuckDB query",
                extra={
                    "symbol": symbol,
                    "paths_count": len(s3_paths),
                    "interval_seconds": interval_seconds,
                    "query_type": "yearly_calendar_aggregated",
                    "uses_actual_timestamps": True
                }
            )
        else:
            # Existing logic for other timeframes (minutes, hours, days, weeks, months)
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
        
        logger.debug(
            f"Executing aggregated DuckDB query",
            extra={
                "symbol": symbol,
                "paths_count": len(s3_paths),
                "interval_seconds": interval_seconds,
                "query_type": "yearly_aggregated" if interval_seconds == 31536000 else "aggregated",
                "is_yearly": interval_seconds == 31536000
            }
        )
        
        try:
            # Execute query directly on S3
            result = self.conn.execute(query).fetchall()
            
            # Get column names
            columns = [desc[0] for desc in self.conn.description]
            
            # Convert to list of dicts
            data = [dict(zip(columns, row)) for row in result]
            
            logger.info(
                f"Aggregated query successful",
                extra={
                    "symbol": symbol,
                    "records_returned": len(data),
                    "paths_queried": len(s3_paths),
                    "query_success": True
                }
            )
            
            return data
            
        except Exception as e:
            error_message = str(e).lower()
            
            # Handle specific missing file errors gracefully
            if "404" in error_message or "not found" in error_message:
                logger.warning(
                    f"Some data files not found - attempting partial data recovery",
                    extra={
                        "symbol": symbol,
                        "paths_attempted": len(s3_paths),
                        "error_type": "missing_files",
                        "first_path": s3_paths[0] if s3_paths else None,
                        "last_path": s3_paths[-1] if len(s3_paths) > 1 else None,
                        "partial_recovery_attempted": True
                    }
                )
                
                # Attempt partial data recovery by trying individual years
                return await self._attempt_partial_data_recovery(
                    s3_paths, symbol, start_unix, end_unix, interval_seconds
                )
            else:
                # Log other types of errors with full context
                logger.error(
                    f"Aggregated query failed with non-missing-file error",
                    extra={
                        "symbol": symbol,
                        "paths_attempted": len(s3_paths),
                        "error_type": "query_execution_error",
                        "error_message": str(e),
                        "interval_seconds": interval_seconds
                    },
                    exc_info=True
                )
                raise

    async def _attempt_partial_data_recovery(self, s3_paths: List[str], symbol: str,
                                           start_unix: int, end_unix: int,
                                           interval_seconds: int) -> List[Dict]:
        """Attempt to recover data by querying individual files and combining results"""
        all_data = []
        successful_paths = []
        failed_paths = []
        
        for path in s3_paths:
            try:
                # Handle yearly aggregation differently in recovery too
                if interval_seconds == 31536000:  # 1Y = 31536000 seconds
                    single_path_query = f"""
                        SELECT 
                            symbol,
                            to_timestamp(first_timestamp) as timestamp,
                            first_timestamp as unix_time,
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
                                open, high, low, close, volume,
                                EXTRACT(YEAR FROM to_timestamp(unix_time)) as year_bucket,
                                min(unix_time) OVER (PARTITION BY EXTRACT(YEAR FROM to_timestamp(unix_time))) as first_timestamp
                            FROM read_parquet(['{path}'])
                            WHERE symbol = '{symbol}'
                                AND unix_time >= {start_unix}
                                AND unix_time <= {end_unix}
                        ) 
                        GROUP BY symbol, year_bucket, first_timestamp
                        ORDER BY year_bucket ASC
                    """
                else:
                    # Existing logic for other timeframes
                    single_path_query = f"""
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
                            FROM read_parquet(['{path}'])
                            WHERE symbol = '{symbol}'
                                AND unix_time >= {start_unix}
                                AND unix_time <= {end_unix}
                        ) 
                        GROUP BY symbol, bucket_start
                        ORDER BY bucket_start ASC
                    """
                
                result = self.conn.execute(single_path_query).fetchall()
                
                if result:
                    columns = [desc[0] for desc in self.conn.description]
                    file_data = [dict(zip(columns, row)) for row in result]
                    all_data.extend(file_data)
                    successful_paths.append(path)
                    
            except Exception as file_error:
                failed_paths.append({"path": path, "error": str(file_error)})
                continue
        
        # Log recovery results
        logger.info(
            f"Partial data recovery completed",
            extra={
                "symbol": symbol,
                "total_paths": len(s3_paths),
                "successful_paths": len(successful_paths),
                "failed_paths": len(failed_paths),
                "records_recovered": len(all_data),
                "recovery_rate_percent": round((len(successful_paths) / len(s3_paths)) * 100, 1)
            }
        )
        
        # Sort combined data by timestamp
        if all_data:
            all_data.sort(key=lambda x: x.get('unix_time', 0))
        
        return all_data

    async def _attempt_partial_raw_recovery(self, s3_paths: List[str], symbol: str,
                                          start_unix: int, end_unix: int) -> List[Dict]:
        """Attempt to recover raw data by querying individual files"""
        all_data = []
        successful_paths = []
        failed_paths = []
        
        for path in s3_paths:
            try:
                # Try individual file query for raw data
                single_path_query = f"""
                    SELECT 
                        symbol,
                        timestamp,
                        unix_time,
                        open,
                        high,
                        low,
                        close,
                        volume
                    FROM read_parquet(['{path}'])
                    WHERE symbol = '{symbol}'
                        AND unix_time >= {start_unix}
                        AND unix_time <= {end_unix}
                    ORDER BY unix_time ASC
                """
                
                result = self.conn.execute(single_path_query).fetchall()
                
                if result:
                    columns = [desc[0] for desc in self.conn.description]
                    file_data = [dict(zip(columns, row)) for row in result]
                    all_data.extend(file_data)
                    successful_paths.append(path)
                    
            except Exception as file_error:
                failed_paths.append({"path": path, "error": str(file_error)})
                continue
        
        # Log recovery results
        logger.info(
            f"Partial raw data recovery completed",
            extra={
                "symbol": symbol,
                "total_paths": len(s3_paths),
                "successful_paths": len(successful_paths),
                "failed_paths": len(failed_paths),
                "records_recovered": len(all_data),
                "recovery_rate_percent": round((len(successful_paths) / len(s3_paths)) * 100, 1)
            }
        )
        
        # Sort combined data by timestamp
        if all_data:
            all_data.sort(key=lambda x: x.get('unix_time', 0))
        
        return all_data
    
    async def get_multi_symbol_data(self, symbols: List[str], s3_paths_by_symbol: Dict[str, List[str]],
                                   start_unix: int, end_unix: int,
                                   interval_seconds: int) -> Dict[str, List[Dict]]:
        """
        Optimized query for multiple symbols in one DuckDB query
        Uses UNION ALL for parallel execution with robust error handling
        """
        if not symbols:
            return {}
        
        # Build optimized multi-symbol query with UNION ALL
        union_queries = []
        total_paths = 0
        
        for symbol in symbols:
            s3_paths = s3_paths_by_symbol.get(symbol, [])
            if not s3_paths:
                continue
                
            total_paths += len(s3_paths)
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
        
        logger.debug(
            f"Executing multi-symbol DuckDB query",
            extra={
                "symbols_count": len(symbols),
                "total_paths": total_paths,
                "interval_seconds": interval_seconds,
                "query_type": "multi_symbol"
            }
        )
        
        try:
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
            
            logger.info(
                f"Multi-symbol query successful",
                extra={
                    "symbols_requested": len(symbols),
                    "symbols_with_data": len(results_by_symbol),
                    "total_records": sum(len(data) for data in results_by_symbol.values()),
                    "query_success": True
                }
            )
            
            return results_by_symbol
            
        except Exception as e:
            error_message = str(e).lower()
            
            # Handle specific missing file errors gracefully
            if "404" in error_message or "not found" in error_message:
                logger.warning(
                    f"Some data files not found for multi-symbol query - attempting individual symbol recovery",
                    extra={
                        "symbols_attempted": len(symbols),
                        "total_paths": total_paths,
                        "error_type": "missing_files",
                        "partial_recovery_attempted": True
                    }
                )
                
                # Fall back to individual symbol queries
                return await self._attempt_multi_symbol_recovery(
                    symbols, s3_paths_by_symbol, start_unix, end_unix, interval_seconds
                )
            else:
                # Log other types of errors with full context
                logger.error(
                    f"Multi-symbol query failed with non-missing-file error",
                    extra={
                        "symbols_attempted": len(symbols),
                        "total_paths": total_paths,
                        "error_type": "query_execution_error",
                        "error_message": str(e),
                        "interval_seconds": interval_seconds
                    },
                    exc_info=True
                )
                raise

    async def query_ohlcv_with_projections(self, s3_paths: List[str], symbol: str,
                                          start_unix: int, end_unix: int,
                                          columns_needed: List[str]) -> List[Dict]:
        """
        Optimized query with column projections to reduce data transfer
        Only reads specified columns from Parquet files
        """
        if not s3_paths:
            logger.warning(
                f"No S3 paths provided for projected query",
                extra={"symbol": symbol, "columns_needed": columns_needed}
            )
            return []
            
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
        
        logger.debug(
            f"Executing projected DuckDB query",
            extra={
                "symbol": symbol,
                "paths_count": len(s3_paths),
                "columns": list(projection_columns),
                "query_type": "projected"
            }
        )
        
        try:
            # Execute query with projections
            result = self.conn.execute(query).fetchall()
            columns = [desc[0] for desc in self.conn.description]
            
            data = [dict(zip(columns, row)) for row in result]
            
            logger.info(
                f"Projected query successful",
                extra={
                    "symbol": symbol,
                    "records_returned": len(data),
                    "paths_queried": len(s3_paths),
                    "query_success": True
                }
            )
            
            return data
            
        except Exception as e:
            error_message = str(e).lower()
            
            # Handle specific missing file errors gracefully
            if "404" in error_message or "not found" in error_message:
                logger.warning(
                    f"Some data files not found for projected query - attempting partial recovery",
                    extra={
                        "symbol": symbol,
                        "paths_attempted": len(s3_paths),
                        "error_type": "missing_files",
                        "columns_needed": columns_needed,
                        "partial_recovery_attempted": True
                    }
                )
                
                # Attempt partial data recovery for projections
                return await self._attempt_partial_projection_recovery(
                    s3_paths, symbol, start_unix, end_unix, columns_needed
                )
            else:
                # Log other types of errors with full context
                logger.error(
                    f"Projected query failed with non-missing-file error",
                    extra={
                        "symbol": symbol,
                        "paths_attempted": len(s3_paths),
                        "error_type": "query_execution_error",
                        "error_message": str(e),
                        "columns_needed": columns_needed
                    },
                    exc_info=True
                )
                raise

    async def _attempt_partial_projection_recovery(self, s3_paths: List[str], symbol: str,
                                                  start_unix: int, end_unix: int,
                                                  columns_needed: List[str]) -> List[Dict]:
        """Attempt to recover projected data by querying individual files"""
        all_data = []
        successful_paths = []
        failed_paths = []
        
        # Build projection list
        base_columns = {'symbol', 'unix_time'}
        projection_columns = base_columns.union(set(columns_needed))
        columns_str = ', '.join(sorted(projection_columns))
        
        for path in s3_paths:
            try:
                # Try individual file query for projected data
                single_path_query = f"""
                    SELECT {columns_str}
                    FROM read_parquet(['{path}'])
                    WHERE symbol = '{symbol}'
                        AND unix_time >= {start_unix}
                        AND unix_time <= {end_unix}
                    ORDER BY unix_time ASC
                """
                
                result = self.conn.execute(single_path_query).fetchall()
                
                if result:
                    columns = [desc[0] for desc in self.conn.description]
                    file_data = [dict(zip(columns, row)) for row in result]
                    all_data.extend(file_data)
                    successful_paths.append(path)
                    
            except Exception as file_error:
                failed_paths.append({"path": path, "error": str(file_error)})
                continue
        
        # Log recovery results
        logger.info(
            f"Partial projection recovery completed",
            extra={
                "symbol": symbol,
                "total_paths": len(s3_paths),
                "successful_paths": len(successful_paths),
                "failed_paths": len(failed_paths),
                "records_recovered": len(all_data),
                "columns_projected": list(projection_columns),
                "recovery_rate_percent": round((len(successful_paths) / len(s3_paths)) * 100, 1)
            }
        )
        
        # Sort combined data by timestamp
        if all_data:
            all_data.sort(key=lambda x: x.get('unix_time', 0))
        
        return all_data

    async def _attempt_multi_symbol_recovery(self, symbols: List[str], 
                                           s3_paths_by_symbol: Dict[str, List[str]],
                                           start_unix: int, end_unix: int,
                                           interval_seconds: int) -> Dict[str, List[Dict]]:
        """Attempt to recover multi-symbol data by querying symbols individually"""
        results_by_symbol = {}
        successful_symbols = []
        failed_symbols = []
        
        for symbol in symbols:
            try:
                s3_paths = s3_paths_by_symbol.get(symbol, [])
                if not s3_paths:
                    continue
                    
                # Use the regular aggregated query for individual symbol
                symbol_data = await self.query_ohlcv_aggregated(
                    s3_paths, symbol, start_unix, end_unix, interval_seconds
                )
                
                if symbol_data:
                    results_by_symbol[symbol] = symbol_data
                    successful_symbols.append(symbol)
                    
            except Exception as symbol_error:
                failed_symbols.append({"symbol": symbol, "error": str(symbol_error)})
                continue
        
        # Log recovery results
        logger.info(
            f"Multi-symbol recovery completed",
            extra={
                "symbols_requested": len(symbols),
                "successful_symbols": len(successful_symbols),
                "failed_symbols": len(failed_symbols),
                "total_records": sum(len(data) for data in results_by_symbol.values()),
                "recovery_rate_percent": round((len(successful_symbols) / len(symbols)) * 100, 1)
            }
        )
        
        return results_by_symbol