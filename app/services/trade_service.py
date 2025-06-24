from typing import List, Optional
from decimal import Decimal
from datetime import datetime
import uuid
import logging
from app.database import db
from app.models import TradeCreate, TradeResponse, TradeType
from app.services.backtest_service import BacktestService
from app.repositories.trade_repository import TradeRepository

logger = logging.getLogger(__name__)

class TradeService:
    def __init__(self, repository: TradeRepository = None, backtest_service: BacktestService = None):
        self.repository = repository or TradeRepository()
        self.backtest_service = backtest_service or BacktestService()
    async def create_trade(self, user_id: str, data: TradeCreate) -> Optional[TradeResponse]:
        """Create a new trade with transaction support"""
        try:
            async with db.transaction() as conn:
                # Verify backtest exists and belongs to user
                backtest = await self.repository.verify_backtest_belongs_to_user(
                    conn, data.backtest_id, uuid.UUID(user_id)
                )
                
                if not backtest:
                    return None
                
                # Create trade and update backtest total_trades count (special method for TWO queries)
                row = await self.repository.create_with_backtest_update(
                    conn,
                    data.backtest_id,
                    data.trade_type.value,
                    data.symbol,
                    data.quantity,
                    data.price,
                    data.timestamp
                )
                
                if row:
                    return TradeResponse(**row)
                raise Exception("Failed to create trade")
                
        except Exception as e:
            logger.error(f"Error creating trade: {e}")
            raise
    
    async def get_backtest_trades(self, user_id: str, backtest_id: str) -> List[TradeResponse]:
        """Get all trades for a backtest"""
        # First verify backtest belongs to user (business logic)
        backtest = await self.backtest_service.get_backtest_by_id(user_id, backtest_id)
        if not backtest:
            return []
        
        rows = await self.repository.get_by_backtest(uuid.UUID(backtest_id))
        return [TradeResponse(**row) for row in rows] 