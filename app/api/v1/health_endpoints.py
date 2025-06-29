from fastapi import APIRouter, HTTPException, status, Request
from typing import Dict, Any
import asyncio
import logging
import os
from datetime import datetime

from app.database import db
from app.services.storage_service import StorageService
from app.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["health"]
)

@router.get("/health")
async def health_check(request: Request) -> Dict[str, Any]:
    """Basic health check endpoint"""
    logger.info(
        "Health check requested",
        extra={"request_id": getattr(request.state, "request_id", "unknown")}
    )
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": settings.version,
        "environment": settings.environment
    }

@router.get("/health/detailed")
async def detailed_health_check(request: Request) -> Dict[str, Any]:
    """Detailed health check with dependency status"""
    logger.info(
        "Detailed health check requested",
        extra={"request_id": getattr(request.state, "request_id", "unknown")}
    )
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": settings.version,
        "environment": settings.environment,
        "dependencies": {}
    }
    
    # Check database
    try:
        await db.fetch_val("SELECT 1")
        health_status["dependencies"]["database"] = {
            "status": "healthy",
            "message": "Connected"
        }
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        health_status["dependencies"]["database"] = {
            "status": "unhealthy",
            "message": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check MinIO storage
    try:
        service = StorageService()
        bucket_status = await service.get_bucket_status()
        health_status["dependencies"]["storage"] = {
            "status": "healthy",
            "message": "Connected",
            "details": bucket_status
        }
    except Exception as e:
        logger.error(f"Storage health check failed: {e}")
        health_status["dependencies"]["storage"] = {
            "status": "unhealthy",
            "message": str(e)
        }
        health_status["status"] = "degraded"
    
    return health_status

@router.get("/health/ready")
async def readiness_check(request: Request) -> Dict[str, Any]:
    """Readiness check for Kubernetes/deployment systems"""
    logger.info(
        "Readiness check requested",
        extra={"request_id": getattr(request.state, "request_id", "unknown")}
    )
    
    # Check critical dependencies
    checks = []
    
    # Required environment variables - check settings instead of os.getenv
    missing_vars = []
    
    if not settings.database_url:
        missing_vars.append("DATABASE_URL")
    if not settings.supabase_url:
        missing_vars.append("SUPABASE_URL")
    if not settings.supabase_jwt_secret:
        missing_vars.append("SUPABASE_JWT_SECRET")
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Missing required environment variables: {', '.join(missing_vars)}"
        )
    
    # Database connectivity
    try:
        await asyncio.wait_for(db.fetch_val("SELECT 1"), timeout=5.0)
        checks.append({"service": "database", "status": "ready"})
    except asyncio.TimeoutError:
        logger.error("Database readiness check timed out")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection timeout"
        )
    except Exception as e:
        logger.error(f"Database readiness check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not ready"
        )
    
    return {
        "status": "ready",
        "timestamp": datetime.utcnow().isoformat(),
        "checks": checks
    } 