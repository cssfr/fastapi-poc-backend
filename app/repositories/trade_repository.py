from typing import List, Optional, Dict
import uuid
from app.repositories.base_repository import BaseRepository
from app.database import db

class TradeRepository(BaseRepository):
    def _table_name(self) -> str:
        return "trades"
    
    def _entity_class(self) -> type:
        return dict  # Returns raw database rows as dicts
    
    async def verify_backtest_belongs_to_user(self, conn, backtest_id: uuid.UUID, user_id: uuid.UUID) -> Optional[Dict]:
        """Verify backtest exists and belongs to user"""
        return await db.fetch_one_in_transaction(
            conn,
            "SELECT id FROM backtests WHERE id = $1 AND user_id = $2",
            backtest_id,
            user_id
        )
    
    async def create_with_backtest_update(self, conn, backtest_id: uuid.UUID, trade_type: str,
                                        symbol: str, quantity: float, price: float, timestamp) -> Optional[Dict]:
        """
        Create trade and update backtest total_trades count in a single transaction
        This handles the TWO queries requirement from migration instructions
        """
        # Insert trade
        trade_query = """
            INSERT INTO trades (
                backtest_id, trade_type, symbol, quantity, price, timestamp
            ) VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
        """
        
        trade_result = await db.fetch_one_in_transaction(
            conn,
            trade_query,
            backtest_id,
            trade_type,
            symbol,
            quantity,
            price,
            timestamp
        )
        
        # Update backtest total_trades count
        await db.execute_in_transaction(
            conn,
            """
            UPDATE backtests 
            SET total_trades = total_trades + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $1
            """,
            backtest_id
        )
        
        return trade_result
    
    async def get_by_backtest(self, backtest_id: uuid.UUID) -> List[Dict]:
        """Get all trades for a backtest"""
        query = """
            SELECT * FROM trades 
            WHERE backtest_id = $1 
            ORDER BY timestamp ASC
        """
        return await self.db.fetch_all(query, backtest_id) 