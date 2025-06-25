from fastapi import APIRouter, Depends, HTTPException, status, Request
from typing import List
from app.services.trade_service import TradeService
from app.models import TradeCreate, TradeResponse
from app.auth import verify_token
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/trades",
    tags=["trades"],
    # Remove router-level auth to allow OPTIONS preflight requests
)

@router.post("", response_model=TradeResponse, status_code=status.HTTP_201_CREATED)
async def create_trade(
    request: Request,
    trade_data: TradeCreate,
    user_id: str = Depends(verify_token)
):
    """Create a new trade"""
    try:
        logger.info(
            "Creating trade",
            extra={
                "user_id": user_id,
                "backtest_id": str(trade_data.backtest_id),
                "trade_type": trade_data.trade_type.value,
                "symbol": trade_data.symbol,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        service = TradeService()
        trade = await service.create_trade(user_id, trade_data)
        
        if not trade:
            logger.warning(
                "Failed to create trade - backtest not found or unauthorized",
                extra={
                    "user_id": user_id,
                    "backtest_id": str(trade_data.backtest_id),
                    "request_id": getattr(request.state, "request_id", "unknown")
                }
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Backtest not found or unauthorized"
            )
        
        logger.info(
            f"Trade created successfully: {trade.id}",
            extra={
                "user_id": user_id,
                "trade_id": str(trade.id),
                "backtest_id": str(trade_data.backtest_id),
                "request_id": getattr(request.state, "request_id", "unknown")
            }
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

@router.get("/backtest/{backtest_id}", response_model=List[TradeResponse])
async def get_backtest_trades(
    request: Request,
    backtest_id: str,
    user_id: str = Depends(verify_token)
):
    """Get all trades for a specific backtest"""
    try:
        logger.info(
            f"Fetching trades for backtest: {backtest_id}",
            extra={
                "user_id": user_id,
                "backtest_id": backtest_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        service = TradeService()
        trades = await service.get_backtest_trades(user_id, backtest_id)
        
        logger.info(
            f"Found {len(trades)} trades for backtest {backtest_id}",
            extra={
                "user_id": user_id,
                "backtest_id": backtest_id,
                "trade_count": len(trades),
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        return trades
        
    except Exception as e:
        logger.error(f"Error fetching trades for backtest {backtest_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch trades"
        ) 