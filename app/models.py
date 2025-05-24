from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
import uuid

class BacktestStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class TradeType(str, Enum):
    BUY = "buy"
    SELL = "sell"

# Request/Response Models
class BacktestCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    strategy: str = Field(..., min_length=1)
    symbol: str = Field(..., min_length=1, max_length=20)
    start_date: date
    end_date: date
    initial_capital: Decimal = Field(default=Decimal("10000.00"), gt=0)

class BacktestResponse(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    name: str
    strategy: str
    symbol: str
    start_date: date
    end_date: date
    initial_capital: Decimal
    final_value: Optional[Decimal] = None
    total_return: Optional[Decimal] = None
    max_drawdown: Optional[Decimal] = None
    sharpe_ratio: Optional[Decimal] = None
    win_rate: Optional[Decimal] = None
    total_trades: int = 0
    status: BacktestStatus
    created_at: datetime
    updated_at: datetime

class BacktestUpdate(BaseModel):
    name: Optional[str] = None
    final_value: Optional[Decimal] = None
    total_return: Optional[Decimal] = None
    max_drawdown: Optional[Decimal] = None
    sharpe_ratio: Optional[Decimal] = None
    win_rate: Optional[Decimal] = None
    total_trades: Optional[int] = None
    status: Optional[BacktestStatus] = None

class TradeCreate(BaseModel):
    backtest_id: uuid.UUID
    trade_type: TradeType
    symbol: str = Field(..., min_length=1, max_length=20)
    quantity: Decimal = Field(..., gt=0)
    price: Decimal = Field(..., gt=0)
    timestamp: datetime

class TradeResponse(BaseModel):
    id: uuid.UUID
    backtest_id: uuid.UUID
    trade_type: TradeType
    symbol: str
    quantity: Decimal
    price: Decimal
    timestamp: datetime
    created_at: datetime

class StrategyCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    is_public: bool = False

class StrategyResponse(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    name: str
    description: Optional[str] = None
    parameters: Dict[str, Any]
    is_public: bool
    created_at: datetime
    updated_at: datetime

class StrategyUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    is_public: Optional[bool] = None

class UserResponse(BaseModel):
    id: uuid.UUID
    email: str
    created_at: datetime
    updated_at: datetime

# Legacy Item model for backward compatibility
class Item(BaseModel):
    id: int
    name: str