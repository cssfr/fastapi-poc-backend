"""Service layer for business logic"""
from typing import List, Optional
from datetime import datetime
import uuid
from .database import db
from .models import (
    BacktestCreate, BacktestResponse, BacktestUpdate, BacktestStatus,
    TradeCreate, TradeResponse, TradeType,
    StrategyCreate, StrategyResponse, StrategyUpdate,
    UserResponse
)
import logging

logger = logging.getLogger(__name__)

class BacktestService:
    @staticmethod
    async def create_backtest(user_id: str, data: BacktestCreate) -> BacktestResponse:
        """Create a new backtest with transaction support"""
        try:
            async with db.transaction() as conn:
                # First ensure user exists
                user_exists = await db.fetch_val_in_transaction(
                    conn,
                    "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)",
                    uuid.UUID(user_id)
                )
                
                if not user_exists:
                    # Create user if not exists (this is transactional)
                    await db.execute_in_transaction(
                        conn,
                        "INSERT INTO users (id, email) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
                        uuid.UUID(user_id),
                        f"user_{user_id}@placeholder.com"  # Placeholder email
                    )
                
                # Create backtest
                query = """
                    INSERT INTO backtests (
                        user_id, name, strategy, symbol, start_date, end_date, 
                        initial_capital, status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    RETURNING *
                """
                
                row = await db.fetch_one_in_transaction(
                    conn,
                    query,
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
    async def update_backtest(user_id: str, backtest_id: str, data: BacktestUpdate) -> Optional[BacktestResponse]:
        """Update a backtest with transaction support"""
        # Build dynamic update query
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
            return await BacktestService.get_backtest_by_id(user_id, backtest_id)
        
        # Add updated_at
        update_fields.append(f"updated_at = ${param_count}")
        values.append(datetime.utcnow())
        param_count += 1
        
        # Add WHERE clause parameters
        values.extend([uuid.UUID(backtest_id), uuid.UUID(user_id)])
        
        query = f"""
            UPDATE backtests 
            SET {', '.join(update_fields)}
            WHERE id = ${param_count} AND user_id = ${param_count + 1}
            RETURNING *
        """
        
        async with db.transaction() as conn:
            row = await db.fetch_one_in_transaction(conn, query, *values)
            return BacktestResponse(**row) if row else None
    
    @staticmethod
    async def delete_backtest(user_id: str, backtest_id: str) -> bool:
        """Delete a backtest (trades are cascade deleted)"""
        query = """
            DELETE FROM backtests 
            WHERE id = $1 AND user_id = $2
        """
        result = await db.execute(query, uuid.UUID(backtest_id), uuid.UUID(user_id))
        return result == "DELETE 1"

class TradeService:
    @staticmethod
    async def create_trade(user_id: str, data: TradeCreate) -> Optional[TradeResponse]:
        """Create a new trade with transaction support"""
        try:
            async with db.transaction() as conn:
                # Verify backtest exists and belongs to user
                backtest = await db.fetch_one_in_transaction(
                    conn,
                    "SELECT id FROM backtests WHERE id = $1 AND user_id = $2",
                    data.backtest_id,
                    uuid.UUID(user_id)
                )
                
                if not backtest:
                    return None
                
                # Create trade
                query = """
                    INSERT INTO trades (
                        backtest_id, trade_type, symbol, quantity, price, timestamp
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING *
                """
                
                row = await db.fetch_one_in_transaction(
                    conn,
                    query,
                    data.backtest_id,
                    data.trade_type.value,
                    data.symbol,
                    data.quantity,
                    data.price,
                    data.timestamp
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
                    data.backtest_id
                )
                
                if row:
                    return TradeResponse(**row)
                raise Exception("Failed to create trade")
                
        except Exception as e:
            logger.error(f"Error creating trade: {e}")
            raise
    
    @staticmethod
    async def get_backtest_trades(user_id: str, backtest_id: str) -> List[TradeResponse]:
        """Get all trades for a backtest"""
        # First verify backtest belongs to user
        backtest = await BacktestService.get_backtest_by_id(user_id, backtest_id)
        if not backtest:
            return []
        
        query = """
            SELECT * FROM trades 
            WHERE backtest_id = $1 
            ORDER BY timestamp ASC
        """
        rows = await db.fetch_all(query, uuid.UUID(backtest_id))
        return [TradeResponse(**row) for row in rows]

class StrategyService:
    @staticmethod
    async def create_strategy(user_id: str, data: StrategyCreate) -> StrategyResponse:
        """Create a new strategy"""
        query = """
            INSERT INTO strategies (
                user_id, name, description, parameters, is_public
            ) VALUES ($1, $2, $3, $4, $5)
            RETURNING *
        """
        
        row = await db.fetch_one(
            query,
            uuid.UUID(user_id),
            data.name,
            data.description,
            data.parameters,
            data.is_public
        )
        
        if row:
            return StrategyResponse(**row)
        raise Exception("Failed to create strategy")
    
    @staticmethod
    async def get_user_strategies(user_id: str, include_public: bool = True) -> List[StrategyResponse]:
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
        
        rows = await db.fetch_all(query, uuid.UUID(user_id))
        return [StrategyResponse(**row) for row in rows]
    
    @staticmethod
    async def get_strategy_by_id(user_id: str, strategy_id: str) -> Optional[StrategyResponse]:
        """Get a specific strategy by ID"""
        query = """
            SELECT * FROM strategies 
            WHERE id = $1 AND (user_id = $2 OR is_public = true)
        """
        row = await db.fetch_one(query, uuid.UUID(strategy_id), uuid.UUID(user_id))
        return StrategyResponse(**row) if row else None
    
    @staticmethod
    async def update_strategy(user_id: str, strategy_id: str, data: StrategyUpdate) -> Optional[StrategyResponse]:
        """Update a strategy (only by owner)"""
        # Build dynamic update query
        update_fields = []
        values = []
        param_count = 1
        
        for field, value in data.dict(exclude_unset=True).items():
            if value is not None:
                update_fields.append(f"{field} = ${param_count}")
                values.append(value)
                param_count += 1
        
        if not update_fields:
            return await StrategyService.get_strategy_by_id(user_id, strategy_id)
        
        # Add updated_at
        update_fields.append(f"updated_at = ${param_count}")
        values.append(datetime.utcnow())
        param_count += 1
        
        # Add WHERE clause parameters
        values.extend([uuid.UUID(strategy_id), uuid.UUID(user_id)])
        
        query = f"""
            UPDATE strategies 
            SET {', '.join(update_fields)}
            WHERE id = ${param_count} AND user_id = ${param_count + 1}
            RETURNING *
        """
        
        row = await db.fetch_one(query, *values)
        return StrategyResponse(**row) if row else None
    
    @staticmethod
    async def delete_strategy(user_id: str, strategy_id: str) -> bool:
        """Delete a strategy (only by owner)"""
        query = """
            DELETE FROM strategies 
            WHERE id = $1 AND user_id = $2
        """
        result = await db.execute(query, uuid.UUID(strategy_id), uuid.UUID(user_id))
        return result == "DELETE 1"

class UserService:
    @staticmethod
    async def get_or_create_user(user_id: str, email: str) -> UserResponse:
        """Get or create a user with transaction support"""
        async with db.transaction() as conn:
            # Try to get existing user
            user = await db.fetch_one_in_transaction(
                conn,
                "SELECT * FROM users WHERE id = $1",
                uuid.UUID(user_id)
            )
            
            if user:
                # Update email if different
                if user['email'] != email:
                    user = await db.fetch_one_in_transaction(
                        conn,
                        "UPDATE users SET email = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 RETURNING *",
                        email,
                        uuid.UUID(user_id)
                    )
                return UserResponse(**user)
            
            # Create new user
            user = await db.fetch_one_in_transaction(
                conn,
                "INSERT INTO users (id, email) VALUES ($1, $2) RETURNING *",
                uuid.UUID(user_id),
                email
            )
            
            if user:
                return UserResponse(**user)
            raise Exception("Failed to create user")