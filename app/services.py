from typing import List, Optional
import uuid
from .database import db
from .models import (
    BacktestCreate, BacktestResponse, BacktestUpdate,
    TradeCreate, TradeResponse,
    StrategyCreate, StrategyResponse, StrategyUpdate,
    UserResponse
)
import logging

logger = logging.getLogger(__name__)

class BacktestService:
    @staticmethod
    async def create_backtest(user_id: str, backtest_data: BacktestCreate) -> BacktestResponse:
        """Create a new backtest"""
        query = """
            INSERT INTO backtests (user_id, name, strategy, symbol, start_date, end_date, initial_capital)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING *
        """
        row = await db.fetch_one(
            query,
            uuid.UUID(user_id),
            backtest_data.name,
            backtest_data.strategy,
            backtest_data.symbol,
            backtest_data.start_date,
            backtest_data.end_date,
            backtest_data.initial_capital
        )
        return BacktestResponse(**row)
    
    @staticmethod
    async def get_user_backtests(user_id: str) -> List[BacktestResponse]:
        """Get all backtests for a user"""
        query = """
            SELECT * FROM backtests 
            WHERE user_id = $1 
            ORDER BY created_at DESC
        """
        rows = await db.fetch_all(query, uuid.UUID(user_id))
        return [BacktestResponse(**row) for row in rows]
    
    @staticmethod
    async def get_backtest_by_id(user_id: str, backtest_id: str) -> Optional[BacktestResponse]:
        """Get a specific backtest by ID"""
        query = """
            SELECT * FROM backtests 
            WHERE id = $1 AND user_id = $2
        """
        row = await db.fetch_one(query, uuid.UUID(backtest_id), uuid.UUID(user_id))
        return BacktestResponse(**row) if row else None
    
    @staticmethod
    async def update_backtest(user_id: str, backtest_id: str, update_data: BacktestUpdate) -> Optional[BacktestResponse]:
        """Update a backtest"""
        # Build dynamic update query
        update_fields = []
        values = []
        param_count = 1
        
        for field, value in update_data.dict(exclude_unset=True).items():
            if value is not None:
                update_fields.append(f"{field} = ${param_count}")
                values.append(value)
                param_count += 1
        
        if not update_fields:
            return await BacktestService.get_backtest_by_id(user_id, backtest_id)
        
        update_fields.append(f"updated_at = NOW()")
        values.extend([uuid.UUID(backtest_id), uuid.UUID(user_id)])
        
        query = f"""
            UPDATE backtests 
            SET {', '.join(update_fields)}
            WHERE id = ${param_count} AND user_id = ${param_count + 1}
            RETURNING *
        """
        
        row = await db.fetch_one(query, *values)
        return BacktestResponse(**row) if row else None
    
    @staticmethod
    async def delete_backtest(user_id: str, backtest_id: str) -> bool:
        """Delete a backtest"""
        query = """
            DELETE FROM backtests 
            WHERE id = $1 AND user_id = $2
        """
        result = await db.execute(query, uuid.UUID(backtest_id), uuid.UUID(user_id))
        return result == "DELETE 1"

class TradeService:
    @staticmethod
    async def create_trade(user_id: str, trade_data: TradeCreate) -> Optional[TradeResponse]:
        """Create a new trade (only if backtest belongs to user)"""
        # First verify the backtest belongs to the user
        backtest_check = await db.fetch_one(
            "SELECT id FROM backtests WHERE id = $1 AND user_id = $2",
            trade_data.backtest_id, uuid.UUID(user_id)
        )
        if not backtest_check:
            return None
        
        query = """
            INSERT INTO trades (backtest_id, trade_type, symbol, quantity, price, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
        """
        row = await db.fetch_one(
            query,
            trade_data.backtest_id,
            trade_data.trade_type.value,
            trade_data.symbol,
            trade_data.quantity,
            trade_data.price,
            trade_data.timestamp
        )
        return TradeResponse(**row) if row else None
    
    @staticmethod
    async def get_backtest_trades(user_id: str, backtest_id: str) -> List[TradeResponse]:
        """Get all trades for a specific backtest"""
        query = """
            SELECT t.* FROM trades t
            JOIN backtests b ON t.backtest_id = b.id
            WHERE t.backtest_id = $1 AND b.user_id = $2
            ORDER BY t.timestamp ASC
        """
        rows = await db.fetch_all(query, uuid.UUID(backtest_id), uuid.UUID(user_id))
        return [TradeResponse(**row) for row in rows]

class StrategyService:
    @staticmethod
    async def create_strategy(user_id: str, strategy_data: StrategyCreate) -> StrategyResponse:
        """Create a new strategy"""
        import json
        query = """
            INSERT INTO strategies (user_id, name, description, parameters, is_public)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING *
        """
        row = await db.fetch_one(
            query,
            uuid.UUID(user_id),
            strategy_data.name,
            strategy_data.description,
            json.dumps(strategy_data.parameters),  # Convert dict to JSON string
            strategy_data.is_public
        )
        # Parse JSON string back to dict for parameters in response
        if row and isinstance(row['parameters'], str):
            row['parameters'] = json.loads(row['parameters'])
        return StrategyResponse(**row)
    
    @staticmethod
    async def get_user_strategies(user_id: str, include_public: bool = True) -> List[StrategyResponse]:
        """Get strategies for a user (including public ones if specified)"""
        import json
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
        
        rows = await db.fetch_all(query, uuid.UUID(user_id))
        strategies = []
        for row in rows:
            # Parse JSON string back to dict for parameters
            if isinstance(row['parameters'], str):
                row['parameters'] = json.loads(row['parameters'])
            strategies.append(StrategyResponse(**row))
        return strategies
    
    @staticmethod
    async def get_strategy_by_id(user_id: str, strategy_id: str) -> Optional[StrategyResponse]:
        """Get a specific strategy by ID"""
        import json
        query = """
            SELECT * FROM strategies 
            WHERE id = $1 AND (user_id = $2 OR is_public = true)
        """
        row = await db.fetch_one(query, uuid.UUID(strategy_id), uuid.UUID(user_id))
        if row:
            # Parse JSON string back to dict for parameters
            if isinstance(row['parameters'], str):
                row['parameters'] = json.loads(row['parameters'])
            return StrategyResponse(**row)
        return None
    
    @staticmethod
    async def update_strategy(user_id: str, strategy_id: str, update_data: StrategyUpdate) -> Optional[StrategyResponse]:
        """Update a strategy (only if owned by user)"""
        import json
        # Build dynamic update query
        update_fields = []
        values = []
        param_count = 1
        
        for field, value in update_data.dict(exclude_unset=True).items():
            if value is not None:
                if field == 'parameters':
                    # Convert parameters dict to JSON string
                    update_fields.append(f"{field} = ${param_count}")
                    values.append(json.dumps(value))
                else:
                    update_fields.append(f"{field} = ${param_count}")
                    values.append(value)
                param_count += 1
        
        if not update_fields:
            return await StrategyService.get_strategy_by_id(user_id, strategy_id)
        
        update_fields.append(f"updated_at = NOW()")
        values.extend([uuid.UUID(strategy_id), uuid.UUID(user_id)])
        
        query = f"""
            UPDATE strategies 
            SET {', '.join(update_fields)}
            WHERE id = ${param_count} AND user_id = ${param_count + 1}
            RETURNING *
        """
        
        row = await db.fetch_one(query, *values)
        if row:
            # Parse JSON string back to dict for parameters
            if isinstance(row['parameters'], str):
                row['parameters'] = json.loads(row['parameters'])
            return StrategyResponse(**row)
        return None
    
    @staticmethod
    async def delete_strategy(user_id: str, strategy_id: str) -> bool:
        """Delete a strategy (only if owned by user)"""
        query = """
            DELETE FROM strategies 
            WHERE id = $1 AND user_id = $2
        """
        result = await db.execute(query, uuid.UUID(strategy_id), uuid.UUID(user_id))
        return result == "DELETE 1"

class UserService:
    @staticmethod
    async def get_or_create_user(user_id: str, email: str) -> UserResponse:
        """Get user or create if doesn't exist"""
        # First try to get existing user
        query = "SELECT * FROM users WHERE id = $1"
        row = await db.fetch_one(query, uuid.UUID(user_id))
        
        if row:
            return UserResponse(**row)
        
        # Create new user if doesn't exist
        query = """
            INSERT INTO users (id, email)
            VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE SET 
                email = EXCLUDED.email,
                updated_at = NOW()
            RETURNING *
        """
        row = await db.fetch_one(query, uuid.UUID(user_id), email)
        return UserResponse(**row)