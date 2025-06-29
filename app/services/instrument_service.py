"""Instrument service for managing instrument metadata and data range validation"""
import json
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import date, datetime, timezone
from app.models_ohlcv import InstrumentMetadata, DataRange
from app.minio_client import minio_client
from app.core.config import settings
from app.repositories.market_data_repository import MarketDataRepository
from app.infrastructure.duckdb_adapter import duckdb_adapter

logger = logging.getLogger(__name__)

class InstrumentService:
    """Service for managing instrument metadata and data range validation"""
    
    def __init__(self, minio_client_instance=None, repository: MarketDataRepository = None):
        self.minio_client = minio_client_instance or minio_client
        self.repository = repository or MarketDataRepository(duckdb_adapter.conn)
        self._instruments_data: Optional[Dict[str, Any]] = None
        self._load_instruments()
    
    def _load_instruments(self):
        """Load instruments metadata from MinIO bucket"""
        try:
            # Get the HTTPResponse object from MinIO
            response = self.minio_client.get_object(settings.minio_bucket, "metadata/instruments.json")
            # Read the content from the HTTPResponse and decode it
            content = response.read().decode('utf-8')
            # Parse the JSON content
            self._instruments_data = json.loads(content)
            
            # Log success with proper context
            instrument_count = len([k for k in self._instruments_data.keys() if not k.startswith('_')])
            logger.info(
                f"Successfully loaded instruments metadata from MinIO", 
                extra={
                    "bucket": settings.minio_bucket,
                    "instrument_count": instrument_count,
                    "metadata_keys": [k for k in self._instruments_data.keys() if k.startswith('_')]
                }
            )
        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to parse instruments.json: Invalid JSON format", 
                extra={"bucket": settings.minio_bucket, "error": str(e)},
                exc_info=True
            )
            self._instruments_data = {}
        except Exception as e:
            logger.warning(
                f"Failed to load instruments from MinIO: {e}. Will fall back to scanning actual data when needed.",
                extra={"bucket": settings.minio_bucket},
                exc_info=True
            )
            self._instruments_data = {}
    
    def reload_instruments(self):
        """Reload instruments data from MinIO"""
        self._load_instruments()
    
    async def _scan_actual_data_range(self, symbol: str, source_resolution: str = "1Y") -> Optional[Tuple[str, str]]:
        """Scan actual parquet data to find earliest and latest dates available
        
        This is used as a fallback when instruments.json is not available
        """
        try:
            # Get all available dates/years for this symbol
            available_dates = await self.repository.get_available_dates(symbol, source_resolution)
            if not available_dates:
                return None
            
            # Build S3 paths for first and last available periods to scan actual data
            first_date = available_dates[0]
            last_date = available_dates[-1]
            
            if source_resolution == "1Y":
                # Build paths for yearly data: ohlcv/1Y/symbol=BTC/year=2017/BTC_2017.parquet
                first_path = f"s3://{settings.minio_bucket}/ohlcv/{source_resolution}/symbol={symbol}/year={first_date}/{symbol}_{first_date}.parquet"
                last_path = f"s3://{settings.minio_bucket}/ohlcv/{source_resolution}/symbol={symbol}/year={last_date}/{symbol}_{last_date}.parquet"
            else:
                # Build paths for daily data: ohlcv/1m/symbol=DAX/date=2013-10-01/DAX_2013-10-01.parquet
                first_path = f"s3://{settings.minio_bucket}/ohlcv/{source_resolution}/symbol={symbol}/date={first_date}/{symbol}_{first_date}.parquet"
                last_path = f"s3://{settings.minio_bucket}/ohlcv/{source_resolution}/symbol={symbol}/date={last_date}/{symbol}_{last_date}.parquet"
            
            # Query actual data to get min/max timestamps
            earliest_query = f"""
                SELECT MIN(unix_time) as min_timestamp 
                FROM read_parquet('{first_path}')
                WHERE symbol = '{symbol}'
            """
            
            latest_query = f"""
                SELECT MAX(unix_time) as max_timestamp 
                FROM read_parquet('{last_path}')
                WHERE symbol = '{symbol}'
            """
            
            # Execute queries
            earliest_result = self.repository.conn.execute(earliest_query).fetchone()
            latest_result = self.repository.conn.execute(latest_query).fetchone()
            
            if earliest_result and latest_result and earliest_result[0] and latest_result[0]:
                # Convert unix timestamps to date strings
                earliest_date = datetime.fromtimestamp(earliest_result[0], tz=timezone.utc).date().isoformat()
                latest_date = datetime.fromtimestamp(latest_result[0], tz=timezone.utc).date().isoformat()
                
                logger.info(f"Scanned actual data for {symbol}: {earliest_date} to {latest_date}")
                return (earliest_date, latest_date)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to scan actual data range for {symbol}: {e}")
            return None
    
    def get_instrument_metadata(self, symbol: str) -> Optional[InstrumentMetadata]:
        """Get metadata for a specific instrument"""
        if not self._instruments_data or symbol not in self._instruments_data:
            return None
            
        data = self._instruments_data[symbol]
        
        # Skip metadata entries (those starting with _)
        if symbol.startswith('_'):
            return None
            
        # Convert to InstrumentMetadata model
        data_range = None
        if 'dataRange' in data:
            data_range_dict = data['dataRange']
            data_range = DataRange(
                earliest=data_range_dict.get('earliest'),
                latest=data_range_dict.get('latest'),
                sources=data_range_dict.get('sources', {})
            )
        
        return InstrumentMetadata(
            symbol=symbol,
            exchange=data.get('exchange', ''),
            market=data.get('market', ''),
            name=data.get('name', ''),
            shortName=data.get('shortName', symbol),
            ticker=data.get('ticker', symbol),
            type=data.get('type', ''),
            currency=data.get('currency', ''),
            description=data.get('description', ''),
            sector=data.get('sector', ''),
            country=data.get('country', ''),
            dataRange=data_range
        )
    
    async def get_data_range(self, symbol: str, source_resolution: str = "1Y") -> Optional[Tuple[str, str]]:
        """Get data range for a symbol and source resolution
        
        First tries instruments metadata, then falls back to scanning actual data
        
        Returns:
            Tuple of (earliest_date, latest_date) or None if not found
        """
        # Try instruments metadata first
        if self._instruments_data and symbol in self._instruments_data:
            data = self._instruments_data[symbol]
            if not symbol.startswith('_') and 'dataRange' in data:
                data_range_dict = data['dataRange']
                
                # Try to get range for specific source resolution first
                if 'sources' in data_range_dict and source_resolution in data_range_dict['sources']:
                    source_range = data_range_dict['sources'][source_resolution]
                    earliest = source_range.get('earliest')
                    latest = source_range.get('latest')
                    if earliest and latest:
                        return (earliest, latest)
                
                # Fall back to general range
                earliest = data_range_dict.get('earliest')
                latest = data_range_dict.get('latest')
                if earliest and latest:
                    return (earliest, latest)
        
        # Fall back to scanning actual data
        logger.info(f"No metadata found for {symbol}, scanning actual data in MinIO")
        return await self._scan_actual_data_range(symbol, source_resolution)
    
    async def bound_date_range(self, symbol: str, start_date: date, end_date: date, 
                        source_resolution: str = "1Y") -> Tuple[date, date]:
        """Bound requested date range to available data bounds
        
        Args:
            symbol: Trading symbol
            start_date: Requested start date
            end_date: Requested end date
            source_resolution: Source data resolution
            
        Returns:
            Tuple of (bounded_start_date, bounded_end_date)
        """
        data_range = await self.get_data_range(symbol, source_resolution)
        if not data_range:
            logger.warning(f"No data range information found for symbol {symbol}")
            return start_date, end_date  # Return original dates if no range info
            
        available_start, available_end = data_range
        available_start_date = date.fromisoformat(available_start)
        available_end_date = date.fromisoformat(available_end)
        
        # Bound the dates to available range
        bounded_start = max(start_date, available_start_date)  # Don't go before available start
        bounded_end = min(end_date, available_end_date)        # Don't go after available end
        
        return bounded_start, bounded_end
    
    async def get_available_symbols(self) -> list[str]:
        """Get list of available instrument symbols
        
        First tries instruments metadata, then falls back to scanning MinIO
        """
        # Try instruments metadata first
        if self._instruments_data:
            # Filter out metadata entries (those starting with _)
            metadata_symbols = [symbol for symbol in self._instruments_data.keys() if not symbol.startswith('_')]
            if metadata_symbols:
                return metadata_symbols
        
        # Fall back to scanning MinIO for actual symbols
        logger.info("No instruments metadata available, scanning MinIO for available symbols")
        try:
            # Default to 1Y source for symbol discovery
            symbols = await self.repository.get_symbols("1Y")
            return symbols
        except Exception as e:
            logger.error(f"Failed to get symbols from MinIO: {e}")
            return []
    
    async def get_instruments_metadata(self) -> Dict[str, InstrumentMetadata]:
        """Get metadata for all instruments"""
        result = {}
        symbols = await self.get_available_symbols()
        for symbol in symbols:
            metadata = self.get_instrument_metadata(symbol)
            if metadata:
                result[symbol] = metadata
        return result

# Service should be instantiated with dependency injection
# Example: instrument_service = InstrumentService(minio_client_instance) 