from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime
from app.repositories.base_repository import BaseRepository
from app.database import db

class BacktestRepository(BaseRepository):
    def _table_name(self) -> str:
        return "backtests"
    
    def _entity_class(self) -> type:
        return dict  # Returns raw database rows as dicts
    
    async def check_user_exists(self, conn, user_id: uuid.UUID) -> bool:
        """Check if user exists in the database"""
        return await db.fetch_val_in_transaction(
            conn,
            "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)",
            user_id
        )
    
    async def create_user_if_not_exists(self, conn, user_id: uuid.UUID, email: str) -> None:
        """Create user if not exists (for backtest creation)"""
        await db.execute_in_transaction(
            conn,
            "INSERT INTO users (id, email) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            user_id,
            email
        )
    
    async def create(self, conn, user_id: uuid.UUID, name: str, strategy: str, symbol: str,
                    start_date: datetime, end_date: datetime, initial_capital: float, status: str) -> Optional[Dict]:
        """Create a new backtest"""
        query = """
            INSERT INTO backtests (
                user_id, name, strategy, symbol, start_date, end_date, 
                initial_capital, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
        """
        
        return await db.fetch_one_in_transaction(
            conn,
            query,
            user_id,
            name,
            strategy,
            symbol,
            start_date,
            end_date,
            initial_capital,
            status
        )
    
    async def get_by_user(self, user_id: uuid.UUID) -> List[Dict]:
        """Get all backtests for a user"""
        query = """
            SELECT * FROM backtests 
            WHERE user_id = $1 
            ORDER BY created_at DESC
        """
        return await self.db.fetch_all(query, user_id)
    
    async def get_by_id(self, backtest_id: uuid.UUID, user_id: uuid.UUID) -> Optional[Dict]:
        """Get a specific backtest by ID"""
        query = """
            SELECT * FROM backtests 
            WHERE id = $1 AND user_id = $2
        """
        return await self.db.fetch_one(query, backtest_id, user_id)
    
    async def update(self, conn, backtest_id: uuid.UUID, user_id: uuid.UUID, 
                    update_fields: List[str], values: List[Any]) -> Optional[Dict]:
        """Update a backtest with dynamic fields"""
        query = f"""
            UPDATE backtests 
            SET {', '.join(update_fields)}
            WHERE id = ${len(values) - 1} AND user_id = ${len(values)}
            RETURNING *
        """
        
        return await db.fetch_one_in_transaction(conn, query, *values)
    
    async def delete(self, backtest_id: uuid.UUID, user_id: uuid.UUID) -> str:
        """Delete a backtest (trades are cascade deleted)"""
        query = """
            DELETE FROM backtests 
            WHERE id = $1 AND user_id = $2
        """
        return await self.db.execute(query, backtest_id, user_id) 