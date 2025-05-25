"""Custom exceptions for the backtesting application"""
from typing import Any, Optional, Dict

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

class AuthorizationError(BacktestingException):
    """Authorization error"""
    pass

class DatabaseError(BacktestingException):
    """Database operation error"""
    pass

class ExternalServiceError(BacktestingException):
    """External service error"""
    pass