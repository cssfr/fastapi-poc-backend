from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials
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
from typing import List
import logging
import uuid

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up...")
    await db.connect()
    yield
    # Shutdown
    logger.info("Shutting down...")
    await db.disconnect()

app = FastAPI(lifespan=lifespan)

# CORS configuration
origins = [
    "http://localhost:5173",                       # local dev
    "https://react-dev.backtesting.theworkpc.com",  # your deployed FE
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def health_check():
    return {"status": "ok", "message": "FastAPI Backtesting API"}

# Legacy endpoint for backward compatibility
@app.get("/items", response_model=List[Item])
async def read_items(user_id: str = Depends(verify_token)):
    """Legacy endpoint - returns dummy data for backward compatibility"""
    DUMMY_ITEMS = [
        {"id": 1, "name": "Sample Backtest", "owner": user_id},
        {"id": 2, "name": "Strategy Template", "owner": user_id},
    ]
    return [Item(**item) for item in DUMMY_ITEMS if item["owner"] == user_id]

# User endpoints
@app.get("/api/user", response_model=UserResponse)
async def get_current_user(cred: HTTPAuthorizationCredentials = Depends(security)):
    """Get current user profile"""
    # Extract real user info from JWT token
    user_info = get_user_info(cred)
    user_id = user_info["user_id"]
    email = user_info["email"]
    
    return await UserService.get_or_create_user(user_id, email)

# Backtest endpoints
@app.post("/api/backtests", response_model=BacktestResponse, status_code=status.HTTP_201_CREATED)
async def create_backtest(
    backtest_data: BacktestCreate,
    user_id: str = Depends(verify_token)
):
    """Create a new backtest"""
    try:
        return await BacktestService.create_backtest(user_id, backtest_data)
    except Exception as e:
        logger.error(f"Error creating backtest: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create backtest"
        )

@app.get("/api/backtests", response_model=List[BacktestResponse])
async def get_backtests(user_id: str = Depends(verify_token)):
    """Get all backtests for the current user"""
    try:
        return await BacktestService.get_user_backtests(user_id)
    except Exception as e:
        logger.error(f"Error fetching backtests: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch backtests"
        )

@app.get("/api/backtests/{backtest_id}", response_model=BacktestResponse)
async def get_backtest(
    backtest_id: str,
    user_id: str = Depends(verify_token)
):
    """Get a specific backtest"""
    try:
        backtest = await BacktestService.get_backtest_by_id(user_id, backtest_id)
        if not backtest:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Backtest not found"
            )
        return backtest
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid backtest ID"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching backtest: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch backtest"
        )

@app.put("/api/backtests/{backtest_id}", response_model=BacktestResponse)
async def update_backtest(
    backtest_id: str,
    update_data: BacktestUpdate,
    user_id: str = Depends(verify_token)
):
    """Update a backtest"""
    try:
        backtest = await BacktestService.update_backtest(user_id, backtest_id, update_data)
        if not backtest:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Backtest not found"
            )
        return backtest
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid backtest ID"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating backtest: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update backtest"
        )

@app.delete("/api/backtests/{backtest_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_backtest(
    backtest_id: str,
    user_id: str = Depends(verify_token)
):
    """Delete a backtest"""
    try:
        success = await BacktestService.delete_backtest(user_id, backtest_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Backtest not found"
            )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid backtest ID"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting backtest: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete backtest"
        )

# Trade endpoints
@app.post("/api/trades", response_model=TradeResponse, status_code=status.HTTP_201_CREATED)
async def create_trade(
    trade_data: TradeCreate,
    user_id: str = Depends(verify_token)
):
    """Create a new trade"""
    try:
        trade = await TradeService.create_trade(user_id, trade_data)
        if not trade:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Backtest not found or access denied"
            )
        return trade
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating trade: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create trade"
        )

@app.get("/api/backtests/{backtest_id}/trades", response_model=List[TradeResponse])
async def get_backtest_trades(
    backtest_id: str,
    user_id: str = Depends(verify_token)
):
    """Get all trades for a specific backtest"""
    try:
        return await TradeService.get_backtest_trades(user_id, backtest_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid backtest ID"
        )
    except Exception as e:
        logger.error(f"Error fetching trades: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch trades"
        )

# Strategy endpoints
@app.post("/api/strategies", response_model=StrategyResponse, status_code=status.HTTP_201_CREATED)
async def create_strategy(
    strategy_data: StrategyCreate,
    user_id: str = Depends(verify_token)
):
    """Create a new strategy"""
    try:
        return await StrategyService.create_strategy(user_id, strategy_data)
    except Exception as e:
        logger.error(f"Error creating strategy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create strategy"
        )

@app.get("/api/strategies", response_model=List[StrategyResponse])
async def get_strategies(
    include_public: bool = True,
    user_id: str = Depends(verify_token)
):
    """Get all strategies for the current user (and public ones if specified)"""
    try:
        return await StrategyService.get_user_strategies(user_id, include_public)
    except Exception as e:
        logger.error(f"Error fetching strategies: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch strategies"
        )

@app.get("/api/strategies/{strategy_id}", response_model=StrategyResponse)
async def get_strategy(
    strategy_id: str,
    user_id: str = Depends(verify_token)
):
    """Get a specific strategy"""
    try:
        strategy = await StrategyService.get_strategy_by_id(user_id, strategy_id)
        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Strategy not found"
            )
        return strategy
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid strategy ID"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching strategy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch strategy"
        )

@app.put("/api/strategies/{strategy_id}", response_model=StrategyResponse)
async def update_strategy(
    strategy_id: str,
    update_data: StrategyUpdate,
    user_id: str = Depends(verify_token)
):
    """Update a strategy"""
    try:
        strategy = await StrategyService.update_strategy(user_id, strategy_id, update_data)
        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Strategy not found"
            )
        return strategy
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid strategy ID"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating strategy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update strategy"
        )

@app.delete("/api/strategies/{strategy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_strategy(
    strategy_id: str,
    user_id: str = Depends(verify_token)
):
    """Delete a strategy"""
    try:
        success = await StrategyService.delete_strategy(user_id, strategy_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Strategy not found"
            )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid strategy ID"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting strategy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete strategy"
        )