"""Storage endpoints for MinIO operations"""
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List
from ..auth import verify_token
from ..minio_client import minio_service
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/storage",
    tags=["storage"]
)

@router.get("/status")
async def get_storage_status(user_id: str = Depends(verify_token)):
    """Check if MinIO storage is configured and available"""
    return {
        "available": minio_service.is_available(),
        "configured": minio_service.is_available()
    }

@router.get("/buckets", response_model=List[str])
async def list_buckets(user_id: str = Depends(verify_token)):
    """List all available storage buckets"""
    if not minio_service.is_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage service not available"
        )
    
    try:
        buckets = await minio_service.list_buckets()
        return buckets
    except Exception as e:
        logger.error(f"Failed to list buckets: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list storage buckets"
        )

@router.get("/objects")
async def list_objects(
    bucket: str = "dukascopy-node",
    prefix: str = "",
    user_id: str = Depends(verify_token)
):
    """List objects in a bucket with optional prefix filter"""
    if not minio_service.is_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage service not available"
        )
    
    try:
        objects = await minio_service.list_objects(bucket, prefix)
        return {
            "bucket": bucket,
            "prefix": prefix,
            "objects": objects,
            "count": len(objects)
        }
    except Exception as e:
        logger.error(f"Failed to list objects: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list objects"
        )