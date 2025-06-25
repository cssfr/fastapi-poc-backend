from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from app.auth import verify_token
from app.database import db
from app.models import Item
from app.middleware import LoggingMiddleware
from app.logging_config import setup_logging
from app.api.v1.router import api_router
from app.api.exception_handlers import register_exception_handlers
from typing import List
import logging
import uuid
import os
import re
from app.core.config import settings

# Setup logging
setup_logging(
    log_level=settings.log_level,
    use_json=settings.log_format == "json"
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

# CORS configuration - MUST BE FIRST!
origins = [
    "http://localhost:3000",
    "http://localhost:3001",
    "https://glowing-space-telegram-5gx544xgv6rg2jgq-3001.app.github.dev",
    "http://localhost:5173",  
    "http://localhost:5174",
    "http://f-stage.backtesting.theworkpc.com",   # Add HTTP version
    "https://f-stage.backtesting.theworkpc.com",  # Keep HTTPS version
    "http://front-stage.backtesting.theworkpc.com",   # Add HTTP version
    "https://front-stage.backtesting.theworkpc.com",  # Keep HTTPS version
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex=r"https?:\/\/(?:.*\.)?front-stage\.backtesting\.theworkpc\.com",
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Add other middleware AFTER CORS
app.add_middleware(LoggingMiddleware)

# Include routers
app.include_router(api_router)

# Register exception handlers
register_exception_handlers(app)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "status": "ok", 
        "message": "FastAPI Backtesting API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/v1/health/"
    }

@app.get("/cors-test")
async def cors_test():
    """Simple endpoint to test CORS without authentication"""
    return {
        "status": "ok",
        "message": "CORS test successful - no authentication required",
        "timestamp": "2024-01-01T00:00:00Z"
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