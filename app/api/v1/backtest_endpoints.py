from fastapi import APIRouter, Depends, HTTPException, status, Request
from typing import List, Optional
from app.services.backtest_service import BacktestService
from app.models import BacktestCreate, BacktestResponse, BacktestUpdate
from app.auth import verify_token
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/backtests",
    tags=["backtests"],
    # Remove router-level auth to allow OPTIONS preflight requests
)

@router.post("", response_model=BacktestResponse, status_code=status.HTTP_201_CREATED)
async def create_backtest(
    request: Request,
    backtest_data: BacktestCreate,
    user_id: str = Depends(verify_token)
):
    """Create a new backtest"""
    logger.info(
        "Creating backtest",
        extra={
            "user_id": user_id,
            "backtest_name": backtest_data.name,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    service = BacktestService()
    backtest = await service.create_backtest(user_id, backtest_data)
    
    logger.info(
        f"Backtest created successfully: {backtest.id}",
        extra={
            "user_id": user_id,
            "backtest_id": str(backtest.id),
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    return backtest

@router.get("", response_model=List[BacktestResponse])
async def get_backtests(request: Request, user_id: str = Depends(verify_token)):
    """Get all backtests for the current user"""
    logger.info(
        "Fetching user backtests",
        extra={
            "user_id": user_id,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    service = BacktestService()
    backtests = await service.get_user_backtests(user_id)
    return backtests

@router.get("/{backtest_id}", response_model=BacktestResponse)
async def get_backtest(request: Request, backtest_id: str, user_id: str = Depends(verify_token)):
    """Get a specific backtest by ID"""
    logger.info(
        f"Fetching backtest: {backtest_id}",
        extra={
            "user_id": user_id,
            "backtest_id": backtest_id,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    service = BacktestService()
    backtest = await service.get_backtest_by_id(user_id, backtest_id)
    return backtest

@router.put("/{backtest_id}", response_model=BacktestResponse)
async def update_backtest(
    request: Request,
    backtest_id: str,
    update_data: BacktestUpdate,
    user_id: str = Depends(verify_token)
):
    """Update a backtest"""
    logger.info(
        f"Updating backtest: {backtest_id}",
        extra={
            "user_id": user_id,
            "backtest_id": backtest_id,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    service = BacktestService()
    backtest = await service.update_backtest(user_id, backtest_id, update_data)
    
    logger.info(
        f"Backtest updated successfully: {backtest_id}",
        extra={
            "user_id": user_id,
            "backtest_id": backtest_id,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    return backtest

@router.delete("/{backtest_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_backtest(
    request: Request,
    backtest_id: str,
    user_id: str = Depends(verify_token)
):
    """Delete a backtest"""
    logger.info(
        f"Deleting backtest: {backtest_id}",
        extra={
            "user_id": user_id,
            "backtest_id": backtest_id,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )
    
    service = BacktestService()
    await service.delete_backtest(user_id, backtest_id)
    
    logger.info(
        f"Backtest deleted successfully: {backtest_id}",
        extra={
            "user_id": user_id,
            "backtest_id": backtest_id,
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    ) 