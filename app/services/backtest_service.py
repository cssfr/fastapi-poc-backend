from typing import List, Optional
from datetime import datetime
from decimal import Decimal
import uuid
import logging
from app.database import db
from app.models import (
    BacktestCreate, BacktestResponse, BacktestUpdate,
    BacktestStatus, BacktestMetrics
)
from app.repositories.backtest_repository import BacktestRepository

logger = logging.getLogger(__name__)

class BacktestService:
    def __init__(self, repository: BacktestRepository = None):
        self.repository = repository or BacktestRepository()
    async def create_backtest(self, user_id: str, data: BacktestCreate) -> BacktestResponse:
        """Create a new backtest with transaction support"""
        try:
            async with db.transaction() as conn:
                # First ensure user exists (business logic)
                user_exists = await self.repository.check_user_exists(conn, uuid.UUID(user_id))
                
                if not user_exists:
                    # Create user if not exists (this is transactional)
                    await self.repository.create_user_if_not_exists(
                        conn, 
                        uuid.UUID(user_id), 
                        f"user_{user_id}@placeholder.com"
                    )
                
                # Create backtest
                row = await self.repository.create(
                    conn,
                    uuid.UUID(user_id),
                    data.name,
                    data.strategy,
                    data.symbol,
                    data.start_date,
                    data.end_date,
                    data.initial_capital,
                    BacktestStatus.PENDING.value
                )
                
                if row:
                    return BacktestResponse(**row)
                raise Exception("Failed to create backtest")
                
        except Exception as e:
            logger.error(f"Error creating backtest: {e}")
            raise
    
    async def get_user_backtests(self, user_id: str) -> List[BacktestResponse]:
        """Get all backtests for a user"""
        rows = await self.repository.get_by_user(uuid.UUID(user_id))
        return [BacktestResponse(**row) for row in rows]
    
    async def get_backtest_by_id(self, user_id: str, backtest_id: str) -> Optional[BacktestResponse]:
        """Get a specific backtest by ID"""
        row = await self.repository.get_by_id(uuid.UUID(backtest_id), uuid.UUID(user_id))
        return BacktestResponse(**row) if row else None
    
    async def update_backtest(self, user_id: str, backtest_id: str, data: BacktestUpdate) -> Optional[BacktestResponse]:
        """Update a backtest with transaction support"""
        # Build dynamic update query (business logic)
        update_fields = []
        values = []
        param_count = 1
        
        for field, value in data.dict(exclude_unset=True).items():
            if value is not None:
                update_fields.append(f"{field} = ${param_count}")
                values.append(value)
                param_count += 1
        
        if not update_fields:
            # No fields to update
            return await self.get_backtest_by_id(user_id, backtest_id)
        
        # Add updated_at
        update_fields.append(f"updated_at = ${param_count}")
        values.append(datetime.utcnow())
        param_count += 1
        
        # Add WHERE clause parameters
        values.extend([uuid.UUID(backtest_id), uuid.UUID(user_id)])
        
        async with db.transaction() as conn:
            row = await self.repository.update(conn, uuid.UUID(backtest_id), uuid.UUID(user_id), update_fields, values)
            return BacktestResponse(**row) if row else None
    
    async def delete_backtest(self, user_id: str, backtest_id: str) -> bool:
        """Delete a backtest (trades are cascade deleted)"""
        result = await self.repository.delete(uuid.UUID(backtest_id), uuid.UUID(user_id))
        return result == "DELETE 1" 