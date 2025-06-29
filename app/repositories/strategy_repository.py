from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime
from app.repositories.base_repository import BaseRepository

class StrategyRepository(BaseRepository):
    def _table_name(self) -> str:
        return "strategies"
    
    def _entity_class(self) -> type:
        return dict  # Returns raw database rows as dicts
    
    async def create(self, user_id: uuid.UUID, name: str, description: str, 
                    parameters: dict, is_public: bool) -> Optional[Dict]:
        """Create a new strategy"""
        query = """
            INSERT INTO strategies (
                user_id, name, description, parameters, is_public
            ) VALUES ($1, $2, $3, $4, $5)
            RETURNING *
        """
        
        return await self.db.fetch_one(
            query,
            user_id,
            name,
            description,
            parameters,
            is_public
        )
    
    async def get_by_user(self, user_id: uuid.UUID, include_public: bool = True) -> List[Dict]:
        """Get strategies for a user (optionally including public ones)"""
        if include_public:
            query = """
                SELECT * FROM strategies 
                WHERE user_id = $1 OR is_public = true
                ORDER BY created_at DESC
            """
        else:
            query = """
                SELECT * FROM strategies 
                WHERE user_id = $1
                ORDER BY created_at DESC
            """
        
        return await self.db.fetch_all(query, user_id)
    
    async def get_by_id(self, strategy_id: uuid.UUID, user_id: uuid.UUID) -> Optional[Dict]:
        """Get a specific strategy by ID"""
        query = """
            SELECT * FROM strategies 
            WHERE id = $1 AND (user_id = $2 OR is_public = true)
        """
        return await self.db.fetch_one(query, strategy_id, user_id)
    
    async def update(self, strategy_id: uuid.UUID, user_id: uuid.UUID, 
                    update_fields: List[str], values: List[Any]) -> Optional[Dict]:
        """Update a strategy (only by owner)"""
        query = f"""
            UPDATE strategies 
            SET {', '.join(update_fields)}
            WHERE id = ${len(values) - 1} AND user_id = ${len(values)}
            RETURNING *
        """
        
        return await self.db.fetch_one(query, *values)
    
    async def delete(self, strategy_id: uuid.UUID, user_id: uuid.UUID) -> str:
        """Delete a strategy (only by owner)"""
        query = """
            DELETE FROM strategies 
            WHERE id = $1 AND user_id = $2
        """
        return await self.db.execute(query, strategy_id, user_id) 