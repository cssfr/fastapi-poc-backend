from fastapi import APIRouter, Depends, HTTPException, status, Request
from typing import List
from app.services.strategy_service import StrategyService
from app.models import StrategyCreate, StrategyResponse, StrategyUpdate
from app.auth import verify_token
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/strategies",
    tags=["strategies"],
    # Remove router-level auth to allow OPTIONS preflight requests
)

@router.post("", response_model=StrategyResponse, status_code=status.HTTP_201_CREATED)
async def create_strategy(
    request: Request,
    strategy_data: StrategyCreate,
    user_id: str = Depends(verify_token)
):
    """Create a new strategy"""
    try:
        logger.info(
            "Creating strategy",
            extra={
                "user_id": user_id,
                "strategy_name": strategy_data.name,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        service = StrategyService()
        strategy = await service.create_strategy(user_id, strategy_data)
        
        logger.info(
            f"Strategy created successfully: {strategy.id}",
            extra={
                "user_id": user_id,
                "strategy_id": str(strategy.id),
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        return strategy
        
    except Exception as e:
        logger.error(f"Error creating strategy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create strategy"
        )

@router.get("", response_model=List[StrategyResponse])
async def get_strategies(
    request: Request,
    include_public: bool = True,
    user_id: str = Depends(verify_token)
):
    """Get all strategies for the current user"""
    try:
        logger.info(
            "Fetching user strategies",
            extra={
                "user_id": user_id,
                "include_public": include_public,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        service = StrategyService()
        strategies = await service.get_user_strategies(user_id, include_public)
        
        logger.info(
            f"Found {len(strategies)} strategies",
            extra={
                "user_id": user_id,
                "strategy_count": len(strategies),
                "include_public": include_public,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        return strategies
        
    except Exception as e:
        logger.error(f"Error fetching strategies: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch strategies"
        )

@router.get("/{strategy_id}", response_model=StrategyResponse)
async def get_strategy(
    request: Request,
    strategy_id: str,
    user_id: str = Depends(verify_token)
):
    """Get a specific strategy by ID"""
    try:
        logger.info(
            f"Fetching strategy: {strategy_id}",
            extra={
                "user_id": user_id,
                "strategy_id": strategy_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        service = StrategyService()
        strategy = await service.get_strategy_by_id(user_id, strategy_id)
        
        if not strategy:
            logger.warning(
                f"Strategy not found: {strategy_id}",
                extra={
                    "user_id": user_id,
                    "strategy_id": strategy_id,
                    "request_id": getattr(request.state, "request_id", "unknown")
                }
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Strategy not found"
            )
        
        return strategy
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching strategy {strategy_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch strategy"
        )

@router.put("/{strategy_id}", response_model=StrategyResponse)
async def update_strategy(
    request: Request,
    strategy_id: str,
    update_data: StrategyUpdate,
    user_id: str = Depends(verify_token)
):
    """Update a strategy"""
    try:
        logger.info(
            f"Updating strategy: {strategy_id}",
            extra={
                "user_id": user_id,
                "strategy_id": strategy_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        service = StrategyService()
        strategy = await service.update_strategy(user_id, strategy_id, update_data)
        
        if not strategy:
            logger.warning(
                f"Strategy not found for update: {strategy_id}",
                extra={
                    "user_id": user_id,
                    "strategy_id": strategy_id,
                    "request_id": getattr(request.state, "request_id", "unknown")
                }
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Strategy not found"
            )
        
        logger.info(
            f"Strategy updated successfully: {strategy_id}",
            extra={
                "user_id": user_id,
                "strategy_id": strategy_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        return strategy
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating strategy {strategy_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update strategy"
        )

@router.delete("/{strategy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_strategy(
    request: Request,
    strategy_id: str,
    user_id: str = Depends(verify_token)
):
    """Delete a strategy"""
    try:
        logger.info(
            f"Deleting strategy: {strategy_id}",
            extra={
                "user_id": user_id,
                "strategy_id": strategy_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        service = StrategyService()
        success = await service.delete_strategy(user_id, strategy_id)
        
        if not success:
            logger.warning(
                f"Strategy not found for deletion: {strategy_id}",
                extra={
                    "user_id": user_id,
                    "strategy_id": strategy_id,
                    "request_id": getattr(request.state, "request_id", "unknown")
                }
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Strategy not found"
            )
        
        logger.info(
            f"Strategy deleted successfully: {strategy_id}",
            extra={
                "user_id": user_id,
                "strategy_id": strategy_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting strategy {strategy_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete strategy"
        ) 