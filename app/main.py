from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from .auth import verify_token, get_user_info, security
from .database import db
from .models import (
    BacktestCreate, BacktestResponse, BacktestUpdate,
    TradeCreate, TradeResponse,
    StrategyCreate, StrategyResponse, StrategyUpdate,
    UserResponse, Item
)
from .services import BacktestService, TradeService, StrategyService, UserService
from .middleware import LoggingMiddleware, ErrorHandlingMiddleware
from .logging_config import setup_logging
from .health import router as health_router
from .routers.storage import router as storage_router
from .routers.ohlcv import router as ohlcv_router
from .exceptions import NotFoundError, ValidationError, DatabaseError
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
# origins = [
#     "http://localhost:5173",
#     "https://react-stage.backtesting.theworkpc.com",
#     "https://front-stage.backtesting.theworkpc.com",
#     "https://gs0ow4kc8c880gwkgkk4wkg4.backtesting.theworkpc.com",
# ]

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # CORS REGEX configuration suggested by ChatGPT first
# regex_pattern = (
#     r"http:\/\/localhost:5173"
#     r"http:\/\/localhost:3000"
#     r"|https:\/\/react-stage\.backtesting\.theworkpc\.com"
#     r"|https:\/\/front-stage\.backtesting\.theworkpc\.com"
#     r"|https:\/\/.*\.front-stage\.backtesting\.theworkpc\.com"
# )

# app.add_middleware(
#     CORSMiddleware,
#     allow_origin_regex=regex_pattern,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # CORS REGEX configuration suggested by ChatGPT second
origins = [
    "http://localhost:3000",
    "http://localhost:3001",
    "https://glowing-space-telegram-5gx544xgv6rg2jgq-3001.app.github.dev",
    "http://localhost:5173",  
    "http://localhost:5174",    # optional
    "https://f-stage.backtesting.theworkpc.com",
    "https://front-stage.backtesting.theworkpc.com",
    r"|https:\/\/.*\.front-stage\.backtesting\.theworkpc\.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex=r"https:\/\/(?:.*\.)?front-stage\.backtesting\.theworkpc\.com",
    allow_credentials=True,       # for JWT/auth headers
    allow_methods=["*"],          # GET, POST, etc.
    allow_headers=["*"],          # Auth headers
)

# Include routers
app.include_router(health_router)
app.include_router(storage_router)
app.include_router(ohlcv_router)

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

# User endpoints
@app.get("/api/user", response_model=UserResponse)
async def get_current_user(request: Request, cred: HTTPAuthorizationCredentials = Depends(security)):
    """Get current user profile"""
    try:
        user_info = get_user_info(cred)
        user_id = user_info["user_id"]
        email = user_info["email"]
        
        logger.info(
            "Getting user profile",
            extra={
                "user_id": user_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        return await UserService.get_or_create_user(user_id, email)
    except Exception as e:
        logger.error(f"Error getting user profile: {e}")
        raise DatabaseError("Failed to retrieve user profile", {"error": str(e)})

# Backtest endpoints
@app.post("/api/backtests", response_model=BacktestResponse, status_code=status.HTTP_201_CREATED)
async def create_backtest(
    request: Request,
    backtest_data: BacktestCreate,
    user_id: str = Depends(verify_token)
):
    """Create a new backtest"""
    try:
        logger.info(
            "Creating new backtest",
            extra={
                "user_id": user_id,
                "backtest_name": backtest_data.name,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        return await BacktestService.create_backtest(user_id, backtest_data)
    except Exception as e:
        logger.error(f"Error creating backtest: {e}", exc_info=True)
        raise DatabaseError("Failed to create backtest")

@app.get("/api/backtests", response_model=List[BacktestResponse])
async def get_backtests(request: Request, user_id: str = Depends(verify_token)):
    """Get all backtests for the current user"""
    try:
        logger.info(
            "Fetching user backtests",
            extra={
                "user_id": user_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        return await BacktestService.get_user_backtests(user_id)
    except Exception as e:
        logger.error(f"Error fetching backtests: {e}", exc_info=True)
        raise DatabaseError("Failed to fetch backtests")

@app.get("/api/backtests/{backtest_id}", response_model=BacktestResponse)
async def get_backtest(request: Request, backtest_id: str, user_id: str = Depends(verify_token)):
    """Get a specific backtest"""
    try:
        try:
            uuid.UUID(backtest_id)
        except ValueError:
            raise ValidationError("Invalid backtest ID format", {"backtest_id": backtest_id})
        
        logger.info(
            "Fetching backtest",
            extra={
                "user_id": user_id,
                "backtest_id": backtest_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        backtest = await BacktestService.get_backtest_by_id(user_id, backtest_id)
        if not backtest:
            raise NotFoundError(f"Backtest not found", {"backtest_id": backtest_id})
        return backtest
    except (NotFoundError, ValidationError):
        raise
    except Exception as e:
        logger.error(f"Error fetching backtest: {e}", exc_info=True)
        raise DatabaseError("Failed to fetch backtest")

@app.put("/api/backtests/{backtest_id}", response_model=BacktestResponse)
async def update_backtest(
    request: Request,
    backtest_id: str,
    update_data: BacktestUpdate,
    user_id: str = Depends(verify_token)
):
    """Update a backtest"""
    try:
        try:
            uuid.UUID(backtest_id)
        except ValueError:
            raise ValidationError("Invalid backtest ID format", {"backtest_id": backtest_id})
        
        logger.info(
            "Updating backtest",
            extra={
                "user_id": user_id,
                "backtest_id": backtest_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        backtest = await BacktestService.update_backtest(user_id, backtest_id, update_data)
        if not backtest:
            raise NotFoundError("Backtest not found", {"backtest_id": backtest_id})
        return backtest
    except (NotFoundError, ValidationError):
        raise
    except Exception as e:
        logger.error(f"Error updating backtest: {e}", exc_info=True)
        raise DatabaseError("Failed to update backtest")

@app.delete("/api/backtests/{backtest_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_backtest(
    request: Request,
    backtest_id: str,
    user_id: str = Depends(verify_token)
):
    """Delete a backtest"""
    try:
        try:
            uuid.UUID(backtest_id)
        except ValueError:
            raise ValidationError("Invalid backtest ID format", {"backtest_id": backtest_id})
        
        logger.info(
            "Deleting backtest",
            extra={
                "user_id": user_id,
                "backtest_id": backtest_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        success = await BacktestService.delete_backtest(user_id, backtest_id)
        if not success:
            raise NotFoundError("Backtest not found", {"backtest_id": backtest_id})
    except (NotFoundError, ValidationError):
        raise
    except Exception as e:
        logger.error(f"Error deleting backtest: {e}", exc_info=True)
        raise DatabaseError("Failed to delete backtest")

# Trade endpoints
@app.post("/api/trades", response_model=TradeResponse, status_code=status.HTTP_201_CREATED)
async def create_trade(
    request: Request,
    trade_data: TradeCreate,
    user_id: str = Depends(verify_token)
):
    """Create a new trade"""
    try:
        logger.info(
            "Creating new trade",
            extra={
                "user_id": user_id,
                "backtest_id": str(trade_data.backtest_id),
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        trade = await TradeService.create_trade(user_id, trade_data)
        if not trade:
            raise NotFoundError("Backtest not found or access denied", {"backtest_id": str(trade_data.backtest_id)})
        return trade
    except NotFoundError:
        raise
    except Exception as e:
        logger.error(f"Error creating trade: {e}", exc_info=True)
        raise DatabaseError("Failed to create trade")

@app.get("/api/backtests/{backtest_id}/trades", response_model=List[TradeResponse])
async def get_backtest_trades(
    request: Request,
    backtest_id: str,
    user_id: str = Depends(verify_token)
):
    """Get all trades for a specific backtest"""
    try:
        try:
            uuid.UUID(backtest_id)
        except ValueError:
            raise ValidationError("Invalid backtest ID format", {"backtest_id": backtest_id})
        
        logger.info(
            "Fetching trades for backtest",
            extra={
                "user_id": user_id,
                "backtest_id": backtest_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        return await TradeService.get_backtest_trades(user_id, backtest_id)
    except ValidationError:
        raise
    except Exception as e:
        logger.error(f"Error fetching trades: {e}", exc_info=True)
        raise DatabaseError("Failed to fetch trades")

# Strategy endpoints
@app.post("/api/strategies", response_model=StrategyResponse, status_code=status.HTTP_201_CREATED)
async def create_strategy(
    request: Request,
    strategy_data: StrategyCreate,
    user_id: str = Depends(verify_token)
):
    """Create a new strategy"""
    try:
        logger.info(
            "Creating new strategy",
            extra={
                "user_id": user_id,
                "strategy_name": strategy_data.name,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        return await StrategyService.create_strategy(user_id, strategy_data)
    except Exception as e:
        logger.error(f"Error creating strategy: {e}", exc_info=True)
        raise DatabaseError("Failed to create strategy")

@app.get("/api/strategies", response_model=List[StrategyResponse])
async def get_strategies(
    request: Request,
    include_public: bool = True,
    user_id: str = Depends(verify_token)
):
    """Get all strategies for the current user (and public ones if specified)"""
    try:
        logger.info(
            "Fetching strategies",
            extra={
                "user_id": user_id,
                "include_public": include_public,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        return await StrategyService.get_user_strategies(user_id, include_public)
    except Exception as e:
        logger.error(f"Error fetching strategies: {e}", exc_info=True)
        raise DatabaseError("Failed to fetch strategies")

@app.get("/api/strategies/{strategy_id}", response_model=StrategyResponse)
async def get_strategy(
    request: Request,
    strategy_id: str,
    user_id: str = Depends(verify_token)
):
    """Get a specific strategy"""
    try:
        try:
            uuid.UUID(strategy_id)
        except ValueError:
            raise ValidationError("Invalid strategy ID format", {"strategy_id": strategy_id})
        
        logger.info(
            "Fetching strategy",
            extra={
                "user_id": user_id,
                "strategy_id": strategy_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        strategy = await StrategyService.get_strategy_by_id(user_id, strategy_id)
        if not strategy:
            raise NotFoundError("Strategy not found", {"strategy_id": strategy_id})
        return strategy
    except (NotFoundError, ValidationError):
        raise
    except Exception as e:
        logger.error(f"Error fetching strategy: {e}", exc_info=True)
        raise DatabaseError("Failed to fetch strategy")

@app.put("/api/strategies/{strategy_id}", response_model=StrategyResponse)
async def update_strategy(
    request: Request,
    strategy_id: str,
    update_data: StrategyUpdate,
    user_id: str = Depends(verify_token)
):
    """Update a strategy"""
    try:
        try:
            uuid.UUID(strategy_id)
        except ValueError:
            raise ValidationError("Invalid strategy ID format", {"strategy_id": strategy_id})
        
        logger.info(
            "Updating strategy",
            extra={
                "user_id": user_id,
                "strategy_id": strategy_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        strategy = await StrategyService.update_strategy(user_id, strategy_id, update_data)
        if not strategy:
            raise NotFoundError("Strategy not found", {"strategy_id": strategy_id})
        return strategy
    except (NotFoundError, ValidationError):
        raise
    except Exception as e:
        logger.error(f"Error updating strategy: {e}", exc_info=True)
        raise DatabaseError("Failed to update strategy")

@app.delete("/api/strategies/{strategy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_strategy(
    request: Request,
    strategy_id: str,
    user_id: str = Depends(verify_token)
):
    """Delete a strategy"""
    try:
        try:
            uuid.UUID(strategy_id)
        except ValueError:
            raise ValidationError("Invalid strategy ID format", {"strategy_id": strategy_id})
        
        logger.info(
            "Deleting strategy",
            extra={
                "user_id": user_id,
                "strategy_id": strategy_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        success = await StrategyService.delete_strategy(user_id, strategy_id)
        if not success:
            raise NotFoundError("Strategy not found", {"strategy_id": strategy_id})
    except (NotFoundError, ValidationError):
        raise
    except Exception as e:
        logger.error(f"Error deleting strategy: {e}", exc_info=True)
        raise DatabaseError("Failed to delete strategy")