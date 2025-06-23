from fastapi import APIRouter, HTTPException, status, Request
from typing import Dict, Any
import logging
import os

from app.minio_client import minio_service
from app.auth import verify_token

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/storage",
    tags=["storage"]
)

@router.get("/status")
async def get_storage_status(request: Request) -> Dict[str, Any]:
    """Get MinIO storage status"""
    try:
        logger.info(
            "Storage status check requested",
            extra={"request_id": getattr(request.state, "request_id", "unknown")}
        )
        
        status_info = await minio_service.get_bucket_status()
        
        # Add additional bucket info
        configured_bucket = os.getenv("MINIO_BUCKET", "dukascopy-node")
        
        response = {
            "status": "healthy",
            "configured_bucket": configured_bucket,
            "minio_status": status_info,
            "message": "Storage service is operational"
        }
        
        logger.info(
            f"Storage status retrieved: {response}",
            extra={
                "bucket": configured_bucket,
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
async def list_buckets(request: Request):
    """List available buckets"""
    try:
        logger.info(
            "Bucket list requested",
            extra={"request_id": getattr(request.state, "request_id", "unknown")}
        )
        
        buckets = await minio_service.list_buckets()
        
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
async def storage_health_check(request: Request):
    """Storage-specific health check"""
    try:
        logger.info(
            "Storage health check requested",
            extra={"request_id": getattr(request.state, "request_id", "unknown")}
        )
        
        # Test basic connectivity
        await minio_service.get_bucket_status()
        
        return {
            "status": "healthy",
            "service": "minio",
            "message": "Storage service is responding"
        }
        
    except Exception as e:
        logger.error(f"Storage health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage service unhealthy"
        ) 