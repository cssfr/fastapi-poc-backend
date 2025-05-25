"""Health check endpoints"""
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from app.database import db
import time
import os
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])

@router.get("/health", status_code=status.HTTP_200_OK)
async def health_check() -> Dict[str, Any]:
    """Basic health check endpoint - Used by Coolify"""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "service": "backtesting-api",
        "version": os.getenv("APP_VERSION", "1.0.0"),
        "environment": os.getenv("COOLIFY_ENVIRONMENT", "production")
    }

@router.get("/health/ready")
async def readiness_check() -> JSONResponse:
    """
    Readiness check including database connectivity
    Returns 200 if service is ready to accept requests
    Returns 503 if service is not ready
    """
    checks = {
        "database": False,
        "timestamp": time.time()
    }
    
    # Overall status
    ready = True
    errors = []
    
    # Check database connection
    try:
        # Simple query to check database connectivity
        result = await db.fetch_val("SELECT 1")
        if result == 1:
            checks["database"] = True
            
            # Check if migrations are up to date
            try:
                alembic_version = await db.fetch_val(
                    "SELECT version_num FROM alembic_version LIMIT 1"
                )
                checks["migrations"] = {
                    "current": alembic_version,
                    "status": "up-to-date" if alembic_version else "not-initialized"
                }
            except:
                checks["migrations"] = {"status": "table-missing"}
        else:
            ready = False
            errors.append("Database query returned unexpected result")
    except Exception as e:
        ready = False
        checks["database"] = False
        errors.append(f"Database connection failed: {str(e)}")
        logger.error(f"Database health check failed: {e}")
    
    # Check if required environment variables are set
    required_env_vars = ["SUPABASE_URL", "SUPABASE_JWT_SECRET", "DATABASE_URL"]
    checks["environment"] = True
    
    for var in required_env_vars:
        if not os.getenv(var):
            ready = False
            checks["environment"] = False
            errors.append(f"Missing required environment variable: {var}")
    
    # Prepare response
    response_data = {
        "status": "ready" if ready else "not ready",
        "checks": checks,
        "timestamp": time.time()
    }
    
    if errors:
        response_data["errors"] = errors
    
    # Return appropriate status code
    status_code = status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE
    
    return JSONResponse(
        status_code=status_code,
        content=response_data
    )

@router.get("/health/live")
async def liveness_check() -> Dict[str, Any]:
    """
    Liveness check - simple check to see if the service is running
    Used by Kubernetes/container orchestrators to know when to restart
    """
    return {
        "status": "alive",
        "timestamp": time.time()
    }