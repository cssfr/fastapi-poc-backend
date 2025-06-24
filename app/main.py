from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from .auth import verify_token
from .database import db
from .models import Item
from .middleware import LoggingMiddleware, ErrorHandlingMiddleware
from .logging_config import setup_logging
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