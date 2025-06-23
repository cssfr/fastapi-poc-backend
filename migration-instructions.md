# FastAPI Backtesting Application Migration Instructions

## Overview
This document contains step-by-step instructions for migrating the FastAPI backtesting application from a monolithic structure to a clean, layered architecture. Each instruction is explicit and designed for AI coding assistants like Claude Code or Cursor AI.

## Complete Endpoint Inventory
The application has 30 total endpoints across:
- **main.py**: 17 endpoints (user, backtests, trades, strategies)
- **health.py**: 3 endpoints (health checks)
- **routers/storage.py**: 3 endpoints (MinIO storage)
- **routers/ohlcv.py**: 7 endpoints (market data)

## Migration Timeline
1. **Phase 1**: Extract routes from main.py to router files (1-2 days) - ALL 30 endpoints
2. **Phase 2**: Implement centralized configuration (1 day)
3. **Phase 3**: Split services.py into separate files (1 day)
4. **Phase 4**: Introduce repository pattern (3-5 days) - Including market data
5. **Phase 5**: Improve error handling and exceptions (2 days) - All domains
6. **Phase 6**: Market data architecture optimization (3-4 days) - Performance focus

## Phase 1: Extract Routes from main.py to Router Files

### Task 1.1: Create Router Directory Structure
```bash
# Execute these commands in the project root
mkdir -p app/api/v1
touch app/api/__init__.py
touch app/api/v1/__init__.py
```

### Task 1.2: Create Backtest Router
Create file: `app/api/v1/backtest_endpoints.py`

```python
# Instructions for AI Assistant:
# 1. Open app/main.py
# 2. Find all endpoints with path starting with "/api/backtests"
# 3. Copy these endpoints to this new file:
#    - POST /api/backtests (around line 195)
#    - GET /api/backtests (around line 222)
#    - GET /api/backtests/{backtest_id} (around line 238)
#    - PUT /api/backtests/{backtest_id} (around line 263)
#    - DELETE /api/backtests/{backtest_id} (around line 294)
# 4. Add these imports at the top:
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Optional
from app.services import BacktestService
from app.models import BacktestCreate, BacktestResponse, BacktestUpdate
from app.auth import verify_token
import logging

logger = logging.getLogger(__name__)

# 5. Create router:
router = APIRouter(
    prefix="/api/v1/backtests",
    tags=["backtests"],
    dependencies=[Depends(verify_token)]
)

# 6. For each endpoint:
#    - Remove "/api/backtests" from the path (it's in the prefix)
#    - Keep all the logic exactly the same
#    - Keep the same parameter names and types
```

### Task 1.3: Create Trade Router
Create file: `app/api/v1/trade_endpoints.py`

```python
# Instructions:
# 1. Copy these endpoints from main.py:
#    - POST /api/trades (around line 318)
#    - GET /api/backtests/{backtest_id}/trades (around line 342)
# 2. For the second endpoint, change the route:
#    FROM: @app.get("/api/backtests/{backtest_id}/trades")
#    TO: @router.get("/backtest/{backtest_id}")
# 3. Create router with prefix="/api/v1/trades"
```

### Task 1.4: Create Strategy Router
Create file: `app/api/v1/strategy_endpoints.py`

```python
# Instructions:
# 1. Copy all endpoints with "/api/strategies" from main.py
# 2. There should be 5 endpoints (POST, GET all, GET one, PUT, DELETE)
# 3. Remove "/api/strategies" from paths
# 4. Use prefix="/api/v1/strategies" in router
```

### Task 1.5: Create User Router
Create file: `app/api/v1/user_endpoints.py`

```python
# Instructions:
# 1. Copy GET /api/user endpoint from main.py (around line 162)
# 2. Change route from "/api/user" to just "/"
# 3. Use prefix="/api/v1/users" in router
```

### Task 1.6: Create Main V1 Router
Create file: `app/api/v1/router.py`

```python
from fastapi import APIRouter
from app.api.v1 import (
    backtest_endpoints,
    trade_endpoints,
    strategy_endpoints,
    user_endpoints
)

api_router = APIRouter()

# Include all routers
api_router.include_router(backtest_endpoints.router)
api_router.include_router(trade_endpoints.router)
api_router.include_router(strategy_endpoints.router)
api_router.include_router(user_endpoints.router)
```

### Task 1.7: Update main.py
```python
# Instructions:
# 1. Delete all endpoint definitions (@app.get, @app.post, etc.) EXCEPT:
#    - The root endpoint "/"
#    - The legacy "/items" endpoint
# 2. Delete these imports (no longer needed):
#    - BacktestCreate, BacktestResponse, BacktestUpdate
#    - TradeCreate, TradeResponse
#    - StrategyCreate, StrategyResponse, StrategyUpdate
#    - UserResponse, Item
#    - BacktestService, TradeService, StrategyService, UserService
# 3. Add this import:
from app.api.v1.router import api_router
# 4. After the line with app.include_router(ohlcv_router), add:
app.include_router(api_router)
```

### Task 1.8: Migrate Health Router
Create file: `app/api/v1/health_endpoints.py`

```python
# Instructions:
# 1. Copy entire content from app/health.py
# 2. Update imports - NO MORE RELATIVE IMPORTS:
#    FROM: from .database import db
#    TO: from app.database import db
# 3. Update router to include prefix:
#    FROM: router = APIRouter(tags=["health"])
#    TO: router = APIRouter(prefix="/api/v1/health", tags=["health"])
# 4. Remove "/health" from individual routes since it's in prefix
```

### Task 1.9: Migrate Storage Router
Create file: `app/api/v1/storage_endpoints.py`

```python
# Instructions:
# 1. Copy entire content from app/routers/storage.py
# 2. Fix ALL imports (they will break!):
#    FROM: from ..minio_client import minio_service
#    TO: from app.minio_client import minio_service
#    FROM: from ..auth import verify_token
#    TO: from app.auth import verify_token
# 3. Update router prefix:
#    FROM: prefix="/api/storage"
#    TO: prefix="/api/v1/storage"
```

### Task 1.10: Migrate OHLCV Router (CRITICAL - WILL BREAK)
Create file: `app/api/v1/ohlcv_endpoints.py`

```python
# IMPORTANT: This WILL break due to import changes!
# 1. Copy entire content from app/routers/ohlcv.py
# 2. Fix ALL relative imports:
#    FROM: from ..auth import verify_token
#    TO: from app.auth import verify_token
#    FROM: from ..models_ohlcv import OHLCVRequest, OHLCVResponse
#    TO: from app.models_ohlcv import OHLCVRequest, OHLCVResponse
#    FROM: from ..duckdb_service import duckdb_service
#    TO: from app.duckdb_service import duckdb_service
#    FROM: from ..minio_client import minio_service
#    TO: from app.minio_client import minio_service
# 3. Update router prefix:
#    FROM: prefix="/api/ohlcv"
#    TO: prefix="/api/v1/ohlcv"
```

### Task 1.11: Update Main V1 Router to Include ALL Routers
Update file: `app/api/v1/router.py`

```python
from fastapi import APIRouter
from app.api.v1 import (
    backtest_endpoints,
    trade_endpoints,
    strategy_endpoints,
    user_endpoints,
    health_endpoints,
    storage_endpoints,
    ohlcv_endpoints
)

api_router = APIRouter()

# Include all routers (order matters for documentation)
api_router.include_router(health_endpoints.router)
api_router.include_router(ohlcv_endpoints.router)
api_router.include_router(storage_endpoints.router)
api_router.include_router(backtest_endpoints.router)
api_router.include_router(trade_endpoints.router)
api_router.include_router(strategy_endpoints.router)
api_router.include_router(user_endpoints.router)
```

### Task 1.12: Clean Up main.py Completely
```python
# Instructions:
# 1. Remove these imports:
#    - from .health import router as health_router
#    - from .routers.storage import router as storage_router
#    - from .routers.ohlcv import router as ohlcv_router
# 2. Remove these lines:
#    - app.include_router(health_router)
#    - app.include_router(storage_router)
#    - app.include_router(ohlcv_router)
# 3. Should only have:
#    - from app.api.v1.router import api_router
#    - app.include_router(api_router)
```

### Task 1.13: Delete Old Router Files
```bash
# After verifying all endpoints work:
rm -rf app/routers/
rm app/health.py
```

### Verification Steps for Phase 1
```bash
# Run these tests to ensure everything works:
# 1. Start the application
# 2. Check that all endpoints still work:
curl http://localhost:8000/api/v1/backtests
curl http://localhost:8000/api/v1/strategies
curl http://localhost:8000/api/v1/users
curl http://localhost:8000/api/v1/health
curl http://localhost:8000/api/v1/storage/status
curl http://localhost:8000/api/v1/ohlcv/symbols
# 3. Verify main.py is now under 200 lines
wc -l app/main.py
# 4. Verify old files are deleted:
#    - app/routers/ directory (completely removed)
#    - app/health.py (moved to api/v1/)
```

## Phase 2: Implement Centralized Configuration

### Task 2.1: Install Dependencies
```bash
# Add to requirements.txt:
pydantic-settings>=2.0.0
```

### Task 2.2: Create Configuration Module
Create file: `app/core/config.py`

```python
# Instructions:
# 1. Create this exact structure:
from pydantic_settings import BaseSettings
from typing import Optional
from functools import lru_cache

class Settings(BaseSettings):
    # Application settings
    app_name: str = "Backtesting API"
    version: str = "1.0.0"
    environment: str = "development"
    log_level: str = "INFO"
    log_format: str = "json"
    
    # Database settings (required)
    database_url: str
    db_pool_min: int = 1
    db_pool_max: int = 5
    
    # Authentication (required)
    supabase_url: str
    supabase_jwt_secret: str
    
    # Storage (optional)
    minio_endpoint: Optional[str] = None
    minio_access_key: Optional[str] = None
    minio_secret_key: Optional[str] = None
    minio_secure: bool = False
    minio_bucket: str = "dukascopy-node"
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        # Add env prefix if needed
        # env_prefix = "APP_"

@lru_cache()
def get_settings():
    return Settings()

# Global instance
settings = get_settings()
```

### Task 2.3: Update auth.py
```python
# Instructions:
# 1. Open app/auth.py
# 2. Find these lines (around 12-16):
SUPABASE_URL = os.getenv("SUPABASE_URL")
JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")

if not JWT_SECRET or not SUPABASE_URL:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_JWT_SECRET")

# 3. Replace with:
from app.core.config import settings

SUPABASE_URL = settings.supabase_url
JWT_SECRET = settings.supabase_jwt_secret

# 4. Remove the import: import os
```

### Task 2.4: Update database.py
```python
# Instructions:
# 1. At the top, replace:
DATABASE_URL = os.getenv("DATABASE_URL")
# With:
from app.core.config import settings
DATABASE_URL = settings.database_url

# 2. In the connect() method, find:
max_size=5,
# Replace with:
min_size=settings.db_pool_min,
max_size=settings.db_pool_max,

# 3. Remove: import os
```

### Task 2.5: Update minio_client.py
```python
# Instructions:
# 1. Replace these lines (12-16):
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "dukascopy-node")

# 2. With:
from app.core.config import settings

MINIO_ENDPOINT = settings.minio_endpoint or "localhost:9000"
MINIO_ACCESS_KEY = settings.minio_access_key
MINIO_SECRET_KEY = settings.minio_secret_key
MINIO_SECURE = settings.minio_secure
MINIO_BUCKET = settings.minio_bucket

# 3. Remove: import os
```

### Task 2.6: Update main.py Logging
```python
# Instructions:
# 1. Find setup_logging call (around line 30):
setup_logging(
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    use_json=os.getenv("LOG_FORMAT", "json") == "json"
)

# 2. Replace with:
from app.core.config import settings

setup_logging(
    log_level=settings.log_level,
    use_json=settings.log_format == "json"
)
```

### Task 2.7: Create .env.example
Create file: `.env.example`

```bash
# Application
APP_NAME="Backtesting API"
VERSION="1.0.0"
ENVIRONMENT="development"
LOG_LEVEL="INFO"
LOG_FORMAT="json"

# Database (required)
DATABASE_URL="postgresql://user:password@localhost:5432/backtesting"
DB_POOL_MIN=1
DB_POOL_MAX=5

# Authentication (required)
SUPABASE_URL="https://your-project.supabase.co"
SUPABASE_JWT_SECRET="your-secret-key"

# Storage (optional)
MINIO_ENDPOINT="localhost:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
MINIO_SECURE=false
MINIO_BUCKET="dukascopy-node"
```

### Phase 2 Verification
```bash
# Check for os.getenv usage
grep -r "os.getenv" app/ --include="*.py"  # Should return nothing

# Test missing config
unset DATABASE_URL
python -m app.main  # Should fail with clear error message

# Verify settings work
python -c "from app.core.config import settings; print(settings.database_url)"
```

## Phase 3: Split services.py into Separate Files

### Task 3.1: Create Services Directory
```bash
mkdir -p app/services
touch app/services/__init__.py
```

### Task 3.2: Extract BacktestService
Create file: `app/services/backtest_service.py`

```python
# Instructions:
# 1. Open app/services.py
# 2. Copy the entire BacktestService class (lines 18-125 approximately)
# 3. Copy these imports from the top of services.py:
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

logger = logging.getLogger(__name__)

# 4. Paste the BacktestService class
# 5. Do NOT copy other service classes
```

### Task 3.3: Extract TradeService
Create file: `app/services/trade_service.py`

```python
# Instructions:
# 1. Copy TradeService class from services.py (lines 127-185 approximately)
# 2. Add these imports:
from typing import List, Optional
from decimal import Decimal
from datetime import datetime
import logging
from app.database import db
from app.models import TradeCreate, TradeResponse, TradeType
from app.services.backtest_service import BacktestService

logger = logging.getLogger(__name__)

# 3. Note: TradeService uses BacktestService, so we import it
```

### Task 3.4: Extract StrategyService
Create file: `app/services/strategy_service.py`

```python
# Instructions:
# 1. Copy StrategyService class from services.py (lines 187-291 approximately)
# 2. Add required imports for models and database
```

### Task 3.5: Extract UserService
Create file: `app/services/user_service.py`

```python
# Instructions:
# 1. Copy UserService class from services.py (lines 293-326 approximately)
# 2. Add required imports
```

### Task 3.6: Create services __init__.py
Update file: `app/services/__init__.py`

```python
from app.services.backtest_service import BacktestService
from app.services.trade_service import TradeService
from app.services.strategy_service import StrategyService
from app.services.user_service import UserService

__all__ = [
    "BacktestService",
    "TradeService", 
    "StrategyService",
    "UserService"
]
```

### Task 3.7: Update All Imports
```python
# Instructions:
# 1. Search for all files importing from app.services
# 2. Update imports:
#    OLD: from app.services import BacktestService
#    NEW: from app.services.backtest_service import BacktestService
# 3. Files to check:
#    - app/api/v1/backtest_endpoints.py
#    - app/api/v1/trade_endpoints.py
#    - app/api/v1/strategy_endpoints.py
#    - app/api/v1/user_endpoints.py
#    - Any test files
```

### Task 3.8: Delete Old services.py
```bash
# Only after verifying everything works:
rm app/services.py
```

### Phase 3 Verification
```bash
# Verify services.py is gone
ls app/services.py  # Should not exist

# Check imports
grep -r "from app.services import" app/  # Should return nothing

# Verify each service works
python -c "from app.services.backtest_service import BacktestService"
```

## Phase 4: Introduce Repository Pattern

### Task 4.1: Create Repository Structure
```bash
mkdir -p app/repositories
touch app/repositories/__init__.py
```

### Task 4.2: Create Base Repository
Create file: `app/repositories/base_repository.py`

```python
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional, List, Dict, Any
from app.database import db
import uuid

T = TypeVar('T')

class BaseRepository(ABC, Generic[T]):
    def __init__(self, database=None):
        self.db = database or db
    
    @abstractmethod
    def _table_name(self) -> str:
        """Return the table name for this repository"""
        pass
    
    @abstractmethod
    def _entity_class(self) -> type:
        """Return the entity class for this repository"""
        pass
    
    async def execute_in_transaction(self, queries: List[tuple]):
        """Execute multiple queries in a transaction"""
        async with self.db.transaction():
            results = []
            for query, *params in queries:
                result = await self.db.fetch_one(query, *params)
                results.append(result)
            return results
```

### Task 4.3: Create BacktestRepository
Create file: `app/repositories/backtest_repository.py`

```python
# Instructions:
# 1. Open app/services/backtest_service.py
# 2. Find all SQL queries (look for db.fetch_one, db.fetch_all, db.execute)
# 3. For each method in BacktestService that has SQL:
#    - Create a corresponding method in BacktestRepository
#    - Move ONLY the SQL query and database call
#    - Keep all business logic in the service
# 
# Example transformation:
# FROM service:
# async def create_backtest(self, user_id: str, data: BacktestCreate):
#     backtest_id = str(uuid.uuid4())
#     # ... validation logic stays in service ...
#     result = await db.fetch_one("""
#         INSERT INTO backtests (...) VALUES (...) RETURNING *
#     """, backtest_id, user_id, ...)
#     return BacktestResponse(**result)
#
# TO repository:
# async def create(self, backtest_id: str, user_id: str, data: dict) -> Optional[Dict]:
#     return await self.db.fetch_one("""
#         INSERT INTO backtests (...) VALUES (...) RETURNING *
#     """, backtest_id, user_id, ...)
```

### Task 4.4: Create TradeRepository
Create file: `app/repositories/trade_repository.py`

```python
# Special instructions for TradeRepository:
# 1. The create_trade method has TWO queries (INSERT trade, UPDATE backtest)
# 2. These must be in a transaction, so create:
# async def create_with_backtest_update(self, trade_data: dict, backtest_id: str):
#     async with self.db.transaction():
#         # INSERT trade
#         # UPDATE backtest metrics
#         return trade_result
```

### Task 4.5: Create StrategyRepository
Create file: `app/repositories/strategy_repository.py`

```python
# Instructions:
# Extract all queries from StrategyService similar to above.
# Methods:
# - create(user_id: str, data: StrategyCreate) -> Optional[Dict]
# - get_by_user(user_id: str, include_public: bool) -> List[Dict]
# - get_by_id(user_id: str, strategy_id: str) -> Optional[Dict]
# - update(user_id: str, strategy_id: str, data: Dict) -> Optional[Dict]
# - delete(user_id: str, strategy_id: str) -> bool
```

### Task 4.6: Create UserRepository
Create file: `app/repositories/user_repository.py`

```python
# Instructions:
# Extract from UserService:
# 1. SELECT user query (lines 298-302)
# 2. UPDATE user query (lines 306-311)
# 3. INSERT user query (lines 315-320)
#
# Methods:
# - get_or_create(user_id: str, email: str) -> Dict
```

### Task 4.7: Update Services to Use Repositories
Example for `app/services/backtest_service.py`:

```python
# Instructions:
# 1. Add import: from app.repositories.backtest_repository import BacktestRepository
# 2. Add __init__ method:
#    def __init__(self, repository: BacktestRepository = None):
#        self.repository = repository or BacktestRepository()
# 3. Replace all db.fetch_one calls with self.repository methods
# 4. Remove direct SQL queries
# 5. Keep business logic (validation, calculations)
```

### Task 4.8: Create MarketDataRepository
Create file: `app/repositories/market_data_repository.py`

```python
# Instructions:
# 1. This is for DuckDB/Parquet operations from duckdb_service.py
# 2. Extract query building logic:

from typing import List, Dict, Optional
from datetime import date
import logging

logger = logging.getLogger(__name__)

class MarketDataRepository:
    def __init__(self, duckdb_conn):
        self.conn = duckdb_conn
    
    async def get_symbols(self, source_resolution: str) -> List[str]:
        """Move symbol query logic from duckdb_service.get_available_symbols"""
        # Extract from duckdb_service.py lines ~260-280
        pass
    
    async def get_available_dates(self, symbol: str, source_resolution: str) -> List[str]:
        """Move date range query from duckdb_service.get_available_dates"""
        # Extract from duckdb_service.py lines ~285-310
        pass
    
    async def query_ohlcv_raw(self, s3_paths: List[str], symbol: str,
                             start_unix: int, end_unix: int) -> List[Dict]:
        """Raw 1-minute data query"""
        # Extract from _build_raw_query method
        pass
    
    async def query_ohlcv_aggregated(self, s3_paths: List[str], symbol: str,
                                    start_unix: int, end_unix: int,
                                    interval_seconds: int) -> List[Dict]:
        """Aggregated data query"""
        # Extract from _build_aggregated_query method
        pass
```

### Task 4.9: Create StorageRepository  
Create file: `app/repositories/storage_repository.py`

```python
# Instructions:
# 1. Extract MinIO operations from minio_client.py
# 2. Move data access operations here:

from typing import List, Dict, Optional
from minio import Minio
import logging

logger = logging.getLogger(__name__)

class StorageRepository:
    def __init__(self, minio_client: Minio):
        self.client = minio_client
    
    async def list_buckets(self) -> List[str]:
        """List all available buckets"""
        # Move from MinIOService.list_buckets
        pass
    
    async def list_objects(self, bucket: str, prefix: str = "") -> List[Dict]:
        """List objects in bucket with prefix"""
        # Move from MinIOService.list_objects
        pass
    
    async def check_object_exists(self, bucket: str, object_name: str) -> bool:
        """Check if object exists"""
        # Move from MinIOService.check_object_exists
        pass
    
    async def get_presigned_url(self, bucket: str, object_name: str, 
                               expires: int = 3600) -> str:
        """Generate presigned URL for object access"""
        # Move from MinIOService.get_object_url
        pass
```

### Phase 4 Verification
```bash
# Check for SQL in services
grep -r "INSERT INTO\|SELECT.*FROM\|UPDATE.*SET\|DELETE FROM" app/services/  # Should return nothing

# Verify repositories exist
ls app/repositories/*_repository.py

# Test repository isolation
python -c "from app.repositories.backtest_repository import BacktestRepository"
```

## Phase 5: Improve Error Handling and Exceptions

### Task 5.1: Enhance Core Exceptions
Update file: `app/core/exceptions.py`

```python
# Add to existing exceptions:

class DomainException(BacktestingException):
    """Base class for domain-specific exceptions"""
    domain: str = "unknown"
    
    def __init__(self, message: str, details: dict = None):
        super().__init__(message=message, details=details)

# Backtest exceptions
class BacktestException(DomainException):
    domain = "backtest"

class BacktestNotFoundException(BacktestException):
    def __init__(self, backtest_id: str, user_id: str = None):
        super().__init__(
            message=f"Backtest {backtest_id} not found",
            details={
                "backtest_id": backtest_id,
                "user_id": user_id
            }
        )

class BacktestInvalidStateError(BacktestException):
    def __init__(self, backtest_id: str, current_state: str, expected_states: List[str], action: str):
        super().__init__(
            message=f"Cannot {action} backtest in {current_state} state",
            details={
                "backtest_id": backtest_id,
                "current_state": current_state,
                "expected_states": expected_states,
                "action": action
            }
        )

class InsufficientCapitalError(BacktestException):
    def __init__(self, required: Decimal, available: Decimal, backtest_id: str):
        super().__init__(
            message=f"Insufficient capital: required ${required}, available ${available}",
            details={
                "required": float(required),
                "available": float(available),
                "backtest_id": backtest_id
            }
        )

# Trade exceptions
class TradeException(DomainException):
    domain = "trade"

# Strategy exceptions  
class StrategyException(DomainException):
    domain = "strategy"

class StrategyNotFoundException(StrategyException):
    def __init__(self, strategy_id: str):
        super().__init__(
            message=f"Strategy {strategy_id} not found",
            details={"strategy_id": strategy_id}
        )

# Market Data exceptions
class MarketDataException(DomainException):
    domain = "market_data"

class SymbolNotFoundException(MarketDataException):
    def __init__(self, symbol: str, source_resolution: str):
        super().__init__(
            message=f"Symbol {symbol} not found in {source_resolution} data",
            details={"symbol": symbol, "source_resolution": source_resolution}
        )

class DataRangeException(MarketDataException):
    def __init__(self, symbol: str, requested_start: str, requested_end: str, 
                 available_start: str, available_end: str):
        super().__init__(
            message=f"Requested date range not available for {symbol}",
            details={
                "symbol": symbol,
                "requested_range": f"{requested_start} to {requested_end}",
                "available_range": f"{available_start} to {available_end}"
            }
        )

class InvalidTimeframeException(MarketDataException):
    def __init__(self, timeframe: str, available_timeframes: List[str]):
        super().__init__(
            message=f"Invalid timeframe: {timeframe}",
            details={
                "requested": timeframe,
                "available": available_timeframes
            }
        )

# Storage exceptions
class StorageException(DomainException):
    domain = "storage"

class BucketAccessException(StorageException):
    def __init__(self, bucket: str, action: str, error: str = None):
        super().__init__(
            message=f"Cannot {action} bucket {bucket}: {error or 'Access denied'}",
            details={"bucket": bucket, "action": action, "error": error}
        )

class ObjectNotFoundException(StorageException):
    def __init__(self, bucket: str, object_key: str):
        super().__init__(
            message=f"Object not found: {object_key} in bucket {bucket}",
            details={"bucket": bucket, "object_key": object_key}
        )
```

### Task 5.2: Create Exception Handlers
Create file: `app/api/exception_handlers.py`

```python
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from app.core.exceptions import (
    BacktestingException, BacktestNotFoundException,
    ValidationError, AuthenticationError, DatabaseError,
    NotFoundError, InsufficientCapitalError
)
import logging
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)

def create_error_response(request: Request, exc: Exception, status_code: int):
    request_id = str(uuid.uuid4())
    
    # Log the error with request context
    logger.error(
        f"Error handling request {request_id}",
        extra={
            "request_id": request_id,
            "path": request.url.path,
            "method": request.method,
            "error_type": type(exc).__name__,
            "error_message": str(exc)
        },
        exc_info=True
    )
    
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "message": str(exc),
                "type": type(exc).__name__,
            },
            "request_id": request_id,
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
    )

def register_exception_handlers(app: FastAPI):
    @app.exception_handler(BacktestNotFoundException)
    async def handle_backtest_not_found(request: Request, exc: BacktestNotFoundException):
        return create_error_response(request, exc, status.HTTP_404_NOT_FOUND)
    
    @app.exception_handler(ValidationError)
    async def handle_validation_error(request: Request, exc: ValidationError):
        return create_error_response(request, exc, status.HTTP_400_BAD_REQUEST)
    
    @app.exception_handler(AuthenticationError)
    async def handle_auth_error(request: Request, exc: AuthenticationError):
        return create_error_response(request, exc, status.HTTP_401_UNAUTHORIZED)
    
    @app.exception_handler(InsufficientCapitalError)
    async def handle_insufficient_capital(request: Request, exc: InsufficientCapitalError):
        return create_error_response(request, exc, status.HTTP_400_BAD_REQUEST)
    
    @app.exception_handler(DatabaseError)
    async def handle_database_error(request: Request, exc: DatabaseError):
        return create_error_response(request, exc, status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception):
        return create_error_response(request, exc, status.HTTP_500_INTERNAL_SERVER_ERROR)
```

### Task 5.3: Update main.py Exception Handling
```python
# Instructions:
# 1. Remove all exception handlers from main.py (lines 135-159)
# 2. Import and register the new handlers:
from app.api.exception_handlers import register_exception_handlers

# 3. After creating the app, add:
register_exception_handlers(app)
```

### Task 5.4: Update Service Error Handling
```python
# Instructions for each service:
# 1. Import domain-specific exceptions
# 2. Replace generic error handling with specific exceptions
# 
# Example for backtest_service.py:
# OLD:
if not backtest:
    return None
    
# NEW:
if not backtest:
    raise BacktestNotFoundException(backtest_id=backtest_id, user_id=user_id)

# OLD:
if backtest['status'] != 'created':
    raise ValueError("Invalid backtest status")
    
# NEW:
if backtest['status'] != BacktestStatus.CREATED:
    raise BacktestInvalidStateError(
        backtest_id=backtest_id,
        current_state=backtest['status'],
        expected_states=[BacktestStatus.CREATED],
        action="update"
    )
```

### Task 5.5: Update Endpoint Error Handling
```python
# Instructions for each endpoint file:
# 1. Remove try/except blocks that just re-raise or log
# 2. Let domain exceptions bubble up to handlers
# 3. Only catch exceptions where you can add value
# 
# Example:
# REMOVE:
try:
    backtest = await service.get_backtest_by_id(user_id, backtest_id)
    if not backtest:
        raise HTTPException(status_code=404, detail="Backtest not found")
    return backtest
except Exception as e:
    logger.error(f"Error: {e}")
    raise HTTPException(status_code=500, detail="Internal error")

# REPLACE WITH:
backtest = await service.get_backtest_by_id(user_id, backtest_id)
return backtest
# The BacktestNotFoundException will be handled by exception handler
```

### Phase 5 Verification
```bash
# Test error responses format
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/v1/backtests/invalid-uuid
# Should return structured error with request_id

# Check for consistent error handling
grep -r "raise HTTPException" app/api/  # Should be minimal

# Verify specific exceptions
grep -r "BacktestNotFoundException\|InsufficientCapitalError" app/services/
```

## Phase 6: Market Data Architecture (3-4 days)

### Overview
The OHLCV/market data system is complex enough to deserve its own migration phase. This phase addresses:
- DuckDB integration and query optimization
- Time-series data handling patterns
- Performance optimization for large datasets
- Caching strategies for immutable market data

### Task 6.1: Split DuckDB Service
```python
# Current: duckdb_service.py (450 lines) does everything
# Split into:

# 1. app/infrastructure/duckdb_adapter.py
# - Connection management
# - Low-level query execution
# - Connection pooling (if supported)

# 2. app/services/market_data_service.py  
# - Business logic for OHLCV data
# - Timeframe aggregations
# - Data validation

# 3. Continue using market_data_repository.py from Phase 4
# - SQL query building
# - Parquet file access patterns
# - Query optimization
```

### Task 6.2: Create Market Data Models
Create file: `app/models/market_data.py`

```python
from pydantic import BaseModel, validator
from datetime import datetime
from typing import List, Optional
from decimal import Decimal

class Candle(BaseModel):
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    
    class Config:
        json_encoders = {
            Decimal: float
        }

class MarketDataQuery(BaseModel):
    symbol: str
    start_date: datetime
    end_date: datetime
    timeframe: str
    
    @validator('timeframe')
    def validate_timeframe(cls, v):
        valid = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M']
        if v not in valid:
            raise ValueError(f"Invalid timeframe. Must be one of: {valid}")
        return v
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        if 'start_date' in values and v < values['start_date']:
            raise ValueError("End date must be after start date")
        return v
```

### Task 6.3: Implement Caching Layer
```python
# Create app/infrastructure/cache.py
# Instructions:
# 1. Use Redis or in-memory cache for market data
# 2. Cache key pattern: f"ohlcv:{symbol}:{timeframe}:{date_hash}"
# 3. TTL: Infinite for historical data, 1 minute for current day

from typing import Optional, Any
import hashlib
import json

class MarketDataCache:
    def __init__(self, redis_client=None):
        self.redis = redis_client
        
    def _generate_key(self, symbol: str, timeframe: str, 
                     start: int, end: int) -> str:
        """Generate cache key for market data query"""
        date_hash = hashlib.md5(f"{start}:{end}".encode()).hexdigest()[:8]
        return f"ohlcv:{symbol}:{timeframe}:{date_hash}"
    
    async def get(self, key: str) -> Optional[Any]:
        if not self.redis:
            return None
        # Implementation
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        if not self.redis:
            return
        # Implementation
```

### Task 6.4: Optimize Query Patterns
```python
# In market_data_repository.py:
# 1. Implement query batching for multiple symbols
# 2. Use DuckDB's parallel query execution
# 3. Optimize Parquet file reading with projections

async def get_multi_symbol_data(self, symbols: List[str], 
                               start: datetime, end: datetime,
                               timeframe: str) -> Dict[str, List[Candle]]:
    """
    Optimized query for multiple symbols in one DuckDB query
    Uses UNION ALL for parallel execution
    """
    # Build optimized query
    pass
```

### Task 6.5: Add Performance Monitoring
```python
# Add metrics collection for:
# - Query execution time
# - Cache hit/miss rates  
# - Data volume processed
# - S3/MinIO access patterns

from time import time
import logging

class PerformanceMonitor:
    def __init__(self):
        self.metrics = {}
    
    async def track_query(self, query_type: str, symbol: str):
        start_time = time()
        # ... execute query ...
        duration = time() - start_time
        
        logger.info(f"Query performance", extra={
            "query_type": query_type,
            "symbol": symbol,
            "duration_ms": duration * 1000,
            "cache_hit": cache_hit
        })
```

### Phase 6 Verification
```bash
# Performance tests
python -m pytest tests/performance/test_market_data.py -v

# Check query optimization
# Should see significant improvement in multi-symbol queries

# Verify caching works
# Second identical request should be < 10ms

# Monitor DuckDB connections
# Should reuse connections, not create new ones per request
```

## Verification Checklist After Each Phase

### After Phase 1:
- [ ] main.py is under 200 lines
- [ ] All 30 endpoints still work with same URLs
- [ ] No duplicate code between files
- [ ] All imports resolved correctly
- [ ] Old routers/ directory deleted
- [ ] health.py deleted (moved to v1)
- [ ] All routers under app/api/v1/

### After Phase 2:
- [ ] No os.getenv() calls outside config.py
- [ ] Application starts with missing env vars shows clear error
- [ ] Settings accessible throughout app
- [ ] All services use settings object

### After Phase 3:
- [ ] services.py deleted
- [ ] Each service in its own file
- [ ] All imports updated
- [ ] No circular dependencies
- [ ] Consider splitting duckdb_service.py next

### After Phase 4:
- [ ] All SQL queries in repository layer
- [ ] Services contain only business logic
- [ ] Repositories are testable with mock DB
- [ ] DuckDB queries in MarketDataRepository
- [ ] MinIO operations in StorageRepository

### After Phase 5:
- [ ] Consistent error responses across API
- [ ] Domain-specific error messages
- [ ] Proper HTTP status codes
- [ ] Error tracking/logging improved
- [ ] Market data errors handled properly

### After Phase 6:
- [ ] DuckDB service split into adapter/service/repository
- [ ] Market data queries optimized
- [ ] Caching implemented for historical data
- [ ] Performance metrics collected
- [ ] Multi-symbol queries batched

## Common Issues and Solutions

### Import Errors After Reorganization
```python
# If you get: ImportError: cannot import name 'BacktestService'
# Check:
# 1. __init__.py files exist in all directories
# 2. Imports updated to new structure
# 3. No circular imports between services
```

### Database Connection Issues
```python
# If services can't connect to database:
# 1. Ensure repositories are properly initialized
# 2. Check that self.db is set in repository __init__
# 3. Verify database.py exports 'db' instance
```

### Configuration Not Loading
```python
# If settings are None or default:
# 1. Check .env file exists and has values
# 2. Verify environment variable names match Settings class
# 3. Ensure settings = get_settings() is called at module level
```

### Endpoint 404 Errors
```python
# If endpoints return 404 after migration:
# 1. Check router prefix doesn't duplicate path
# 2. Verify router is included in main.py
# 3. Check endpoint path (should not include prefix)
```

### OHLCV Import Errors
```python
# Most common issue after migration:
# ImportError: attempted relative import beyond top-level package
# Solution: Change ALL relative imports to absolute imports
# FROM: from ..auth import verify_token
# TO: from app.auth import verify_token
```