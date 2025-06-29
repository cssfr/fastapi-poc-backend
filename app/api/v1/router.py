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