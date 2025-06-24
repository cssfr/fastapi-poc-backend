from typing import List, Optional
from datetime import datetime
import uuid
import logging
from app.database import db
from app.models import StrategyCreate, StrategyResponse, StrategyUpdate
from app.repositories.strategy_repository import StrategyRepository
from app.core.exceptions import StrategyNotFoundException, StrategyException

logger = logging.getLogger(__name__)

class StrategyService:
    def __init__(self, repository: StrategyRepository = None):
        self.repository = repository or StrategyRepository()
    async def create_strategy(self, user_id: str, data: StrategyCreate) -> StrategyResponse:
        """Create a new strategy"""
        row = await self.repository.create(
            uuid.UUID(user_id),
            data.name,
            data.description,
            data.parameters,
            data.is_public
        )
        
        if row:
            return StrategyResponse(**row)
        raise StrategyException("Failed to create strategy")
    
    async def get_user_strategies(self, user_id: str, include_public: bool = True) -> List[StrategyResponse]:
        """Get strategies for a user (optionally including public ones)"""
        rows = await self.repository.get_by_user(uuid.UUID(user_id), include_public)
        return [StrategyResponse(**row) for row in rows]
    
    async def get_strategy_by_id(self, user_id: str, strategy_id: str) -> StrategyResponse:
        """Get a specific strategy by ID"""
        row = await self.repository.get_by_id(uuid.UUID(strategy_id), uuid.UUID(user_id))
        if not row:
            raise StrategyNotFoundException(strategy_id=strategy_id)
        return StrategyResponse(**row)
    
    async def update_strategy(self, user_id: str, strategy_id: str, data: StrategyUpdate) -> StrategyResponse:
        """Update a strategy (only by owner)"""
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
            return await self.get_strategy_by_id(user_id, strategy_id)
        
        # Add updated_at
        update_fields.append(f"updated_at = ${param_count}")
        values.append(datetime.utcnow())
        param_count += 1
        
        # Add WHERE clause parameters
        values.extend([uuid.UUID(strategy_id), uuid.UUID(user_id)])
        
        row = await self.repository.update(uuid.UUID(strategy_id), uuid.UUID(user_id), update_fields, values)
        if not row:
            raise StrategyNotFoundException(strategy_id=strategy_id)
        return StrategyResponse(**row)
    
    async def delete_strategy(self, user_id: str, strategy_id: str) -> bool:
        """Delete a strategy (only by owner)"""
        result = await self.repository.delete(uuid.UUID(strategy_id), uuid.UUID(user_id))
        success = result == "DELETE 1"
        if not success:
            raise StrategyNotFoundException(strategy_id=strategy_id)
        return True 