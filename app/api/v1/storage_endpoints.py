from fastapi import APIRouter, HTTPException, status, Request, Depends
from typing import Dict, Any
import logging

from app.services.storage_service import StorageService
from app.auth import verify_token
from app.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/storage",
    tags=["storage"]
)

@router.get("/status")
async def get_storage_status(request: Request, user_id: str = Depends(verify_token)) -> Dict[str, Any]:
    """Get MinIO storage status"""
    try:
        logger.info(
            "Storage status check requested",
            extra={"request_id": getattr(request.state, "request_id", "unknown")}
        )
        
        service = StorageService()
        status_info = await service.get_bucket_status()
        
        response = {
            "status": "healthy",
            "configured_bucket": status_info["configured_bucket"],
            "minio_status": status_info,
            "message": "Storage service is operational"
        }
        
        logger.info(
            f"Storage status retrieved: {response}",
            extra={
                "bucket": status_info["configured_bucket"],
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Storage status check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Storage service unavailable: {str(e)}"
        )

@router.get("/buckets")
async def list_buckets(request: Request, user_id: str = Depends(verify_token)):
    """List available buckets"""
    try:
        logger.info(
            "Bucket list requested",
            extra={"request_id": getattr(request.state, "request_id", "unknown")}
        )
        
        service = StorageService()
        buckets = await service.get_bucket_list()
        
        logger.info(
            f"Found {len(buckets)} buckets",
            extra={
                "bucket_count": len(buckets),
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        return {"buckets": buckets}
        
    except Exception as e:
        logger.error(f"Failed to list buckets: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list buckets: {str(e)}"
        )

@router.get("/health")
async def storage_health_check(request: Request, user_id: str = Depends(verify_token)):
    """Storage-specific health check"""
    try:
        logger.info(
            "Storage health check requested",
            extra={"request_id": getattr(request.state, "request_id", "unknown")}
        )
        
        service = StorageService()
        health_info = await service.check_storage_health()
        
        return health_info
        
    except Exception as e:
        logger.error(f"Storage health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage service unhealthy"
        ) 