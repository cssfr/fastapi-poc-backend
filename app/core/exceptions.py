"""Custom exceptions for the backtesting application"""
from typing import Any, Optional, Dict, List
from decimal import Decimal

class BacktestingException(Exception):
    """Base exception for backtesting app"""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

class NotFoundError(BacktestingException):
    """Resource not found"""
    pass

class ValidationError(BacktestingException):
    """Validation error"""
    pass

class AuthenticationError(BacktestingException):
    """Authentication error"""
    pass

class AuthorizationError(BacktestingException):
    """Authorization error"""
    pass

class DatabaseError(BacktestingException):
    """Database operation error"""
    pass

class ExternalServiceError(BacktestingException):
    """External service error"""
    pass

class DomainException(BacktestingException):
    """Base class for domain-specific exceptions"""
    domain: str = "unknown"
    
    def __init__(self, message: str, details: dict = None):
        super().__init__(message=message, details=details)

# Backtest exceptions
class BacktestException(DomainException):
    domain = "backtest"

class BacktestNotFoundException(BacktestException):
    def __init__(self, backtest_id: str, user_id: str = None):
        super().__init__(
            message=f"Backtest {backtest_id} not found",
            details={
                "backtest_id": backtest_id,
                "user_id": user_id
            }
        )

class BacktestInvalidStateError(BacktestException):
    def __init__(self, backtest_id: str, current_state: str, expected_states: List[str], action: str):
        super().__init__(
            message=f"Cannot {action} backtest in {current_state} state",
            details={
                "backtest_id": backtest_id,
                "current_state": current_state,
                "expected_states": expected_states,
                "action": action
            }
        )

class InsufficientCapitalError(BacktestException):
    def __init__(self, required: Decimal, available: Decimal, backtest_id: str):
        super().__init__(
            message=f"Insufficient capital: required ${required}, available ${available}",
            details={
                "required": float(required),
                "available": float(available),
                "backtest_id": backtest_id
            }
        )

# Trade exceptions
class TradeException(DomainException):
    domain = "trade"

# Strategy exceptions  
class StrategyException(DomainException):
    domain = "strategy"

class StrategyNotFoundException(StrategyException):
    def __init__(self, strategy_id: str):
        super().__init__(
            message=f"Strategy {strategy_id} not found",
            details={"strategy_id": strategy_id}
        )

# Market Data exceptions
class MarketDataException(DomainException):
    domain = "market_data"

class SymbolNotFoundException(MarketDataException):
    def __init__(self, symbol: str, source_resolution: str):
        super().__init__(
            message=f"Symbol {symbol} not found in {source_resolution} data",
            details={"symbol": symbol, "source_resolution": source_resolution}
        )

class DataRangeException(MarketDataException):
    def __init__(self, symbol: str, requested_start: str, requested_end: str, 
                 available_start: str, available_end: str):
        super().__init__(
            message=f"Requested date range not available for {symbol}",
            details={
                "symbol": symbol,
                "requested_range": f"{requested_start} to {requested_end}",
                "available_range": f"{available_start} to {available_end}"
            }
        )

class InvalidTimeframeException(MarketDataException):
    def __init__(self, timeframe: str, available_timeframes: List[str]):
        super().__init__(
            message=f"Invalid timeframe: {timeframe}",
            details={
                "requested": timeframe,
                "available": available_timeframes
            }
        )

# Storage exceptions
class StorageException(DomainException):
    domain = "storage"

class BucketAccessException(StorageException):
    def __init__(self, bucket: str, action: str, error: str = None):
        super().__init__(
            message=f"Cannot {action} bucket {bucket}: {error or 'Access denied'}",
            details={"bucket": bucket, "action": action, "error": error}
        )

class ObjectNotFoundException(StorageException):
    def __init__(self, bucket: str, object_key: str):
        super().__init__(
            message=f"Object not found: {object_key} in bucket {bucket}",
            details={"bucket": bucket, "object_key": object_key}
        )

# Add these new exceptions

class OHLCVRequestTooLargeError(Exception):
    """Raised when OHLCV request exceeds size limits"""
    def __init__(self, timeframe: str, days_requested: int, max_limit: int, estimated_records: int = None):
        self.timeframe = timeframe
        self.days_requested = days_requested
        self.max_limit = max_limit  # Could be max_days or max_records depending on context
        self.estimated_records = estimated_records
        
        # Create contextual error message
        if estimated_records and estimated_records > 50000:
            message = (
                f"Request would return too many records (~{estimated_records:,}) for {timeframe} timeframe. "
                f"Maximum allowed: {max_limit:,} records. "
                f"Reduce date range or use a larger timeframe."
            )
        else:
            message = (
                f"Date range too large for {timeframe} timeframe. "
                f"Requested {days_requested} days, maximum allowed: {max_limit} days. "
                f"Use a larger timeframe for longer historical periods."
            )
        
        super().__init__(message)

class OHLCVResultTooLargeError(Exception):
    """Raised when OHLCV result exceeds record limits"""
    def __init__(self, record_count: int, max_records: int):
        self.record_count = record_count
        self.max_records = max_records
        super().__init__(
            f"Result too large: {record_count} records. Maximum: {max_records} records"
        ) 