from pydantic import BaseModel, Field, validator
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
    initial_capital: Decimal = Field(default=Decimal("10000.00"), gt=0, le=Decimal("1000000000"))
    
    @validator('symbol')
    def normalize_symbol(cls, v):
        """Normalize symbol to uppercase and strip whitespace"""
        return v.upper().strip()
    
    @validator('end_date')
    def validate_dates(cls, v, values):
        """Ensure end_date is after start_date and not in future"""
        if 'start_date' in values and v <= values['start_date']:
            raise ValueError('end_date must be after start_date')
        if v > date.today():
            raise ValueError('end_date cannot be in the future')
        return v
    
    @validator('initial_capital')
    def validate_capital(cls, v):
        """Ensure capital is a reasonable amount"""
        if v <= 0:
            raise ValueError('initial_capital must be positive')
        if v > Decimal("1000000000"):  # 1 billion max
            raise ValueError('initial_capital exceeds maximum allowed')
        return v

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
    
    class Config:
        # Handle Decimal serialization
        json_encoders = {
            Decimal: lambda v: float(v)
        }

class BacktestUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    final_value: Optional[Decimal] = Field(None, gt=0)
    total_return: Optional[Decimal] = None
    max_drawdown: Optional[Decimal] = Field(None, ge=0, le=1)  # 0-100% as decimal
    sharpe_ratio: Optional[Decimal] = None
    win_rate: Optional[Decimal] = Field(None, ge=0, le=1)  # 0-100% as decimal
    total_trades: Optional[int] = Field(None, ge=0)
    status: Optional[BacktestStatus] = None

class TradeCreate(BaseModel):
    backtest_id: uuid.UUID
    trade_type: TradeType
    symbol: str = Field(..., min_length=1, max_length=20)
    quantity: Decimal = Field(..., gt=0, le=Decimal("1000000"))
    price: Decimal = Field(..., gt=0, le=Decimal("1000000"))
    timestamp: datetime
    
    @validator('symbol')
    def normalize_symbol(cls, v):
        """Normalize symbol to uppercase and strip whitespace"""
        return v.upper().strip()
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Ensure timestamp is not in the future"""
        if v > datetime.now():
            raise ValueError('timestamp cannot be in the future')
        return v
    
    @validator('quantity', 'price')
    def validate_positive_decimal(cls, v):
        """Ensure positive values"""
        if v <= 0:
            raise ValueError('must be positive')
        return v

class TradeResponse(BaseModel):
    id: uuid.UUID
    backtest_id: uuid.UUID
    trade_type: TradeType
    symbol: str
    quantity: Decimal
    price: Decimal
    timestamp: datetime
    created_at: datetime
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v)
        }

class StrategyCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    is_public: bool = False
    
    @validator('parameters')
    def validate_parameters(cls, v):
        """Ensure parameters is JSON-serializable"""
        import json
        try:
            # Test if it can be serialized
            json.dumps(v)
        except (TypeError, ValueError):
            raise ValueError('parameters must be JSON-serializable')
        return v

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
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    parameters: Optional[Dict[str, Any]] = None
    is_public: Optional[bool] = None
    
    @validator('parameters')
    def validate_parameters(cls, v):
        """Ensure parameters is JSON-serializable if provided"""
        if v is not None:
            import json
            try:
                json.dumps(v)
            except (TypeError, ValueError):
                raise ValueError('parameters must be JSON-serializable')
        return v

class UserResponse(BaseModel):
    id: uuid.UUID
    email: str
    created_at: datetime
    updated_at: datetime

# Legacy Item model for backward compatibility
class Item(BaseModel):
    id: int
    name: str