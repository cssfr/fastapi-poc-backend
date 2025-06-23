from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from .auth import verify_token
from .database import db
from .models import Item
from .middleware import LoggingMiddleware, ErrorHandlingMiddleware
from .logging_config import setup_logging
from .exceptions import NotFoundError, ValidationError, DatabaseError
from app.api.v1.router import api_router
from typing import List
import logging
import uuid
import os
import re

# Setup logging
setup_logging(
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    use_json=os.getenv("LOG_FORMAT", "json") == "json"
)

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting up FastAPI Backtesting API...")
    try:
        await db.connect()
        logger.info("Database connection established")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down FastAPI Backtesting API...")
    try:
        await db.disconnect()
        logger.info("Database connection closed")
    except Exception as e:
        logger.error(f"Error closing database connection: {e}")

# Create FastAPI app with metadata
app = FastAPI(
    title="Backtesting API",
    description="API for managing backtesting strategies and trades",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add middleware
app.add_middleware(ErrorHandlingMiddleware)
app.add_middleware(LoggingMiddleware)

# CORS configuration
origins = [
    "http://localhost:3000",
    "http://localhost:3001",
    "https://glowing-space-telegram-5gx544xgv6rg2jgq-3001.app.github.dev",
    "http://localhost:5173",  
    "http://localhost:5174",
    "https://f-stage.backtesting.theworkpc.com",
    "https://front-stage.backtesting.theworkpc.com",
    r"|https:\/\/.*\.front-stage\.backtesting\.theworkpc\.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex=r"https:\/\/(?:.*\.)?front-stage\.backtesting\.theworkpc\.com",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(api_router)

# Exception handlers
@app.exception_handler(NotFoundError)
async def not_found_exception_handler(request: Request, exc: NotFoundError):
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={
            "detail": exc.message,
            "type": "not_found",
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )

@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "detail": exc.message,
            "type": "validation_error",
            "errors": exc.details,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )

@app.exception_handler(DatabaseError)
async def database_exception_handler(request: Request, exc: DatabaseError):
    logger.error(f"Database error: {exc.message}", extra={"details": exc.details})
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Database operation failed",
            "type": "database_error",
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "status": "ok", 
        "message": "FastAPI Backtesting API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }

# Legacy endpoint for backward compatibility
@app.get("/items", response_model=List[Item])
async def read_items(request: Request, user_id: str = Depends(verify_token)):
    """Legacy endpoint - returns dummy data for backward compatibility"""
    logger.info(
        "Legacy items endpoint called",
        extra={
            "user_id": user_id,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    DUMMY_ITEMS = [
        {"id": 1, "name": "Sample Backtest", "owner": user_id},
        {"id": 2, "name": "Strategy Template", "owner": user_id},
    ]
    return [Item(**item) for item in DUMMY_ITEMS if item["owner"] == user_id]