from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from app.core.exceptions import (
    BacktestingException, BacktestNotFoundException,
    ValidationError, AuthenticationError, DatabaseError,
    NotFoundError, InsufficientCapitalError,
    OHLCVRequestTooLargeError, OHLCVResultTooLargeError
)
import logging
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)

def create_error_response(request: Request, exc: Exception, status_code: int):
    request_id = str(uuid.uuid4())
    
    # Log the error with request context
    logger.error(
        f"Error handling request {request_id}",
        extra={
            "request_id": request_id,
            "path": request.scope["path"],
            "method": request.method,
            "error_type": type(exc).__name__,
            "error_message": str(exc)
        },
        exc_info=True
    )
    
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "message": str(exc),
                "type": type(exc).__name__,
            },
            "request_id": request_id,
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.scope["path"]
        }
    )

def register_exception_handlers(app: FastAPI):
    @app.exception_handler(BacktestNotFoundException)
    async def handle_backtest_not_found(request: Request, exc: BacktestNotFoundException):
        return create_error_response(request, exc, status.HTTP_404_NOT_FOUND)
    
    @app.exception_handler(ValidationError)
    async def handle_validation_error(request: Request, exc: ValidationError):
        return create_error_response(request, exc, status.HTTP_400_BAD_REQUEST)
    
    @app.exception_handler(AuthenticationError)
    async def handle_auth_error(request: Request, exc: AuthenticationError):
        return create_error_response(request, exc, status.HTTP_401_UNAUTHORIZED)
    
    @app.exception_handler(InsufficientCapitalError)
    async def handle_insufficient_capital(request: Request, exc: InsufficientCapitalError):
        return create_error_response(request, exc, status.HTTP_400_BAD_REQUEST)
    
    @app.exception_handler(DatabaseError)
    async def handle_database_error(request: Request, exc: DatabaseError):
        return create_error_response(request, exc, status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception):
        return create_error_response(request, exc, status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @app.exception_handler(OHLCVRequestTooLargeError)
    async def ohlcv_request_too_large_handler(request: Request, exc: OHLCVRequestTooLargeError):
        # Determine if this is a record count or day limit error
        is_record_limit = exc.estimated_records and exc.estimated_records > 50000
        
        return JSONResponse(
            status_code=400,
            content={
                "error": "Request Too Large",
                "message": str(exc),
                "details": {
                    "timeframe": exc.timeframe,
                    "days_requested": exc.days_requested,
                    "max_limit_allowed": exc.max_limit,
                    "estimated_records": exc.estimated_records,
                    "limit_type": "records" if is_record_limit else "days",
                    "suggestion": (
                        f"Reduce date range to stay under {exc.max_limit:,} records" if is_record_limit 
                        else f"Use a larger timeframe (e.g., '1h', '1d') for date ranges longer than {exc.max_limit} days"
                    )
                }
            }
        )
    
    @app.exception_handler(OHLCVResultTooLargeError)
    async def ohlcv_result_too_large_handler(request: Request, exc: OHLCVResultTooLargeError):
        return JSONResponse(
            status_code=400,
            content={
                "error": "Result Too Large", 
                "message": str(exc),
                "details": {
                    "record_count": exc.record_count,
                    "max_records_allowed": exc.max_records,
                    "suggestion": "Reduce date range or use a larger timeframe to get fewer data points"
                }
            }
        ) 