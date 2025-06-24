# Post-Migration Cleanup Plan

## Overview
The migration has been implemented but several critical issues need to be resolved. The main problems are database connection initialization, import path conflicts, and service dependency injection issues.

## Critical Issues Identified

### 1. Database Connection Issues
**Problem**: "Database pool not initialized" error in user endpoints
**Root Cause**: Database connection not properly initialized in the new repository/service architecture

### 2. Import Path Conflicts  
**Problem**: Relative imports still exist, circular imports possible

### 3. Router Configuration Issues
**Problem**: Health endpoint returning 307 redirect instead of 200
**Root Cause**: Path conflicts or duplicate route definitions

### 4. Service Dependency Injection Problems
**Problem**: Services not properly initialized with repositories

## Detailed Cleanup Tasks

### Task 1: Fix Database Connection Architecture
**Priority**: CRITICAL

#### 1.1 Fix Database Property Access
- **File**: `app/database.py`
- **Issue**: Properties like `self.pool` vs `self._pool` inconsistency
- **Fix**: Standardize property access and ensure pool is properly exposed

#### 1.2 Fix Repository Database Initialization
- **Files**: All `app/repositories/*_repository.py`
- **Issue**: Repositories might be calling database methods before connection is established
- **Fix**: Ensure repositories get active database connection, not just database instance

#### 1.3 Fix Service Database Dependencies
- **Files**: All `app/services/*_service.py`
- **Issue**: Services instantiating repositories without proper database connection
- **Fix**: Pass database connection to repository constructors

### Task 2: Clean Up Import Paths
**Priority**: HIGH

#### 2.1 Audit All Import Statements
- **Search Pattern**: `from \.\.|from \.`
- **Files**: All Python files in `app/`
- **Action**: Convert all relative imports to absolute imports

#### 2.2 Fix Circular Import Issues
- **Check**: Service -> Repository -> Service imports
- **Files**: `app/services/trade_service.py` (imports BacktestService)
- **Action**: Consider dependency injection instead of direct imports

### Task 3: Fix Router and Endpoint Issues
**Priority**: HIGH

#### 3.1 Fix Health Endpoint Redirect Issue
- **File**: `app/api/v1/health_endpoints.py`
- **Issue**: Paths might conflict with existing routes
- **Check**: Remove "/health" prefix since router already has prefix

#### 3.2 Audit All Endpoint Paths
- **Files**: All `app/api/v1/*_endpoints.py`
- **Check**: Ensure no path conflicts between router prefix and endpoint paths
- **Fix**: Remove prefixes from individual routes that are already in router prefix

#### 3.3 Verify Router Inclusion Order
- **File**: `app/api/v1/router.py`
- **Check**: Router inclusion order might affect path resolution
- **Fix**: Ensure specific routes come before generic ones

### Task 4: Comprehensive Legacy File Cleanup
**Priority**: HIGH

#### 4.1 Complete Legacy File Inventory
**Root Directory Files to Audit**:
- `app/services.py` - Delete (split into app/services/)
- `app/health.py` - Delete (moved to api/v1/health_endpoints.py)
- `app/fast_metadata_service.py` - Review if still used by OHLCV endpoints
- `app/duckdb_service.py` - Review vs new infrastructure/duckdb_adapter.py

**Directory Structures to Check**:
- `app/routers/` - Should be completely deleted
- Any old `app/models_*.py` files that might be duplicated
- Old configuration or utility files that might be obsolete

#### 4.2 Legacy Import Pattern Search (Windows/Cursor)
**In Cursor Terminal**:
```cmd
# Find old import patterns  
findstr /s /c:"from app.services import" app\
findstr /s /c:"from .services" app\
findstr /s /c:"from app.health" app\
findstr /s /c:"from .health" app\
findstr /s /c:"from app.routers" app\
```

#### 4.3 Unused File Detection (Windows)
**Check for References**:
```cmd
# For each suspected legacy file, check if it's imported anywhere
findstr /s /c:"fast_metadata_service" app\
findstr /s /c:"duckdb_service" app\
findstr /s /c:"services.py" app\
```

#### 4.4 Clean Up Dead Imports in Main Files
- **Files**: `app/main.py`
- **Action**: Remove imports like:
  - `from .health import router as health_router`
  - `from .routers.storage import router as storage_router`
  - `from .routers.ohlcv import router as ohlcv_router`
  - `from .services import BacktestService` (should use specific imports)

#### 4.5 Verify __init__.py Files
- **Files**: `app/services/__init__.py`, `app/repositories/__init__.py`, `app/api/__init__.py`
- **Check**: Ensure exports match actual files
- **Fix**: Remove exports of deleted services/modules

### Task 5: Middleware Architecture Cleanup
**Priority**: MEDIUM

#### 5.1 Remove Redundant ErrorHandlingMiddleware
**Files**: `app/main.py`, `app/middleware.py`
**Action**: Delete ErrorHandlingMiddleware completely - it conflicts with exception_handlers.py
**Reason**: Exception handlers provide better error context and domain-specific error handling

#### 5.2 Keep LoggingMiddleware for Coolify Integration
**Files**: `app/middleware.py`
**Action**: Retain LoggingMiddleware for structured request/response logging
**Reason**: Provides detailed request tracking, duration metrics, and request IDs for Coolify log analysis

#### 5.3 Update Middleware Registration
**File**: `app/main.py`
**Remove**:
```python
app.add_middleware(ErrorHandlingMiddleware)
```
**Keep**:
```python
app.add_middleware(LoggingMiddleware)
# CORS middleware
register_exception_handlers(app)  # Handle all errors here
```

#### 5.4 Clean Up Middleware File
**File**: `app/middleware.py`
**Action**: Delete ErrorHandlingMiddleware class entirely
**Keep**: LoggingMiddleware for structured logging to Coolify console

### Task 6: Fix Configuration and Environment Issues
**Priority**: MEDIUM

#### 6.1 Verify Configuration Loading
- **File**: `app/core/config.py`
- **Check**: Settings properly loaded and accessible
- **Test**: Print settings values during startup

#### 6.2 Fix Environment Variable Migration
- **Files**: All files that previously used `os.getenv()`
- **Check**: Ensure all converted to use `settings` object
- **Verify**: No remaining `os.getenv()` calls except in config.py

### Task 7: Fix Missing Service Dependencies
**Priority**: HIGH

#### 7.1 Fix Repository Instantiation in Services
**Pattern Issue**:
```python
# Current (broken):
class BacktestService:
    def __init__(self, repository: BacktestRepository = None):
        self.repository = repository or BacktestRepository()  # Creates new instance

# Should be:
class BacktestService:
    def __init__(self, repository: BacktestRepository = None):
        self.repository = repository or BacktestRepository(db)  # Pass database
```

#### 7.2 Fix Service Instantiation in Endpoints
**Files**: All `app/api/v1/*_endpoints.py`
**Issue**: Services instantiated as `Service()` without dependencies
**Fix**: Either use dependency injection or ensure services get proper dependencies

### Task 8: Fix Market Data Service Issues
**Priority**: MEDIUM

#### 8.1 Check DuckDB Service Migration
- **Files**: `app/services/market_data_service.py`, `app/infrastructure/duckdb_adapter.py`
- **Issue**: DuckDB service was split but might have broken dependencies
- **Check**: Ensure market data service properly instantiates DuckDB adapter

#### 8.2 Verify Cache Infrastructure
- **Files**: `app/infrastructure/cache.py`
- **Issue**: Cache might not be properly initialized
- **Check**: Ensure cache is accessible to market data service

### Task 9: Test and Verify Fixes
**Priority**: CRITICAL

#### 9.1 Local Development Verification (Windows/Cursor)
```python
# Test imports work without errors
from app.database import db
from app.services.user_service import UserService  
from app.repositories.user_repository import UserRepository
print("Import tests passed")
```

#### 9.2 Pre-Deployment Checks
- Import validation passes without errors
- No relative import warnings in IDE
- Settings load without missing environment variable errors
- All service classes instantiate (even without DB connection)

#### 9.3 Post-Deployment Testing (Coolify Staging)
- Check deployment logs for startup errors
- Verify database migrations run successfully  
- Test endpoints via Coolify staging URL
- Monitor structured logs for database connection errors

#### 9.4 Coolify Deployment Monitoring
**Critical Environment Variables for Coolify**:
- `DATABASE_URL` - Must be set correctly
- `SUPABASE_URL`, `SUPABASE_JWT_SECRET` - Required for auth
- `MINIO_*` variables - Optional but needed for storage features

**Deployment Log Patterns to Watch**:
- ✅ "Database connection pool created successfully"
- ✅ "FastAPI Backtesting API..." startup message
- ❌ "Database pool not initialized" 
- ❌ "ImportError" or "ModuleNotFoundError"
- ❌ "Missing required environment variable"

**Rollback Strategy**:
- Keep previous working commit hash ready
- If deployment fails, immediately rollback via Coolify interface
- Test rollback functionality before starting cleanup

## Implementation Order

### Phase 1: Critical Database & Legacy Cleanup (Day 1)
1. **Legacy File Deletion** (Task 4.1): Delete services.py, health.py, routers/ directory
2. **Import Path Cleanup** (Task 2): Convert relative imports, remove dead imports  
3. **Database Connection** (Task 1): Fix database.py property access and repository initialization
4. **Test Imports** (Task 9.1): Verify all imports work in Cursor

### Phase 2: Service Dependencies & Architecture (Day 1)
1. **Repository Dependencies** (Task 1.2-1.3): Fix repository database initialization
2. **Service Dependencies** (Task 7): Fix service instantiation and repository injection
3. **Configuration** (Task 6): Verify settings loading and environment variables
4. **Test Services** (Task 9.2): Verify service classes instantiate without errors

### Phase 3: Routing & Middleware (Day 2)
1. **Router Conflicts** (Task 3): Fix health endpoint redirects and path conflicts
2. **Middleware Cleanup** (Task 5): Remove ErrorHandlingMiddleware, keep LoggingMiddleware
3. **Deploy & Test** (Task 9.3): Deploy to Coolify, test all endpoints

### Phase 4: Final Integration & Market Data (Day 2)
1. **Market Data Services** (Task 8): Verify OHLCV integration with new infrastructure
2. **Final Cleanup** (Task 4.2-4.5): Remove remaining dead code and verify __init__.py files
3. **Full Integration Testing** (Task 9.3): Complete API testing and performance verification

## Verification Checklist

### Legacy Cleanup
- [ ] app/services.py deleted
- [ ] app/health.py deleted  
- [ ] app/routers/ directory deleted
- [ ] No imports reference deleted files
- [ ] All imports use absolute paths (app.services.specific_service)
- [ ] __init__.py files updated for new structure

### Middleware Architecture
- [ ] No double error handling (middleware + exception handlers)
- [ ] Request logging works without duplication
- [ ] Error responses consistent across all endpoints
- [ ] Middleware order doesn't conflict with exception handling

### Database Layer
- [ ] Database connection initializes successfully
- [ ] Repositories can execute queries
- [ ] Services can call repository methods
- [ ] No "Database pool not initialized" errors

### API Layer (Post-Deployment on Coolify)
- [ ] All endpoints return appropriate status codes (not 307/500)
- [ ] User endpoints work without database errors  
- [ ] Health endpoints return 200 via staging URL
- [ ] Storage endpoints accessible
- [ ] OHLCV endpoints continue working
- [ ] Structured logs show no import or initialization errors

### Development Environment (Windows/Cursor)
- [ ] No import errors in IDE
- [ ] Service classes can be instantiated in Python console
- [ ] Cursor doesn't show relative import warnings
- [ ] Settings object loads without environment errors locally

### Code Quality
- [ ] No relative imports remain
- [ ] No circular import warnings
- [ ] No dead/unused imports
- [ ] All services instantiate without errors

### Integration
- [ ] Full API test suite passes
- [ ] Error handling works correctly
- [ ] Logging produces structured output
- [ ] Performance acceptable

## Common Patterns to Fix

### Pattern 1: Database Access
```python
# Broken:
class SomeRepository:
    def __init__(self):
        self.db = db  # May not be connected yet

# Fixed:
class SomeRepository:
    def __init__(self, database=None):
        self.db = database or db
        if not self.db._pool:
            raise RuntimeError("Database not connected")
```

### Pattern 2: Service Dependencies
```python
# Broken:
@router.get("/")
async def get_data():
    service = SomeService()  # No dependencies

# Fixed:
@router.get("/")  
async def get_data():
    repository = SomeRepository(db)
    service = SomeService(repository)
```

### Pattern 3: Import Paths
```python
# Broken:
from ..services import SomeService
from .other_module import something

# Fixed:
from app.services.some_service import SomeService  
from app.other_module import something
```