from typing import Dict, Any, List
import logging
from app.repositories.storage_repository import StorageRepository
from app.minio_client import minio_client
from app.core.config import settings

logger = logging.getLogger(__name__)

class StorageService:
    """Business logic for storage operations"""
    
    def __init__(self, repository: StorageRepository = None):
        self.repository = repository or StorageRepository(minio_client)
    
    async def get_bucket_status(self) -> Dict[str, Any]:
        """Get comprehensive bucket status information"""
        if not minio_client:
            raise RuntimeError("MinIO service not available")
        
        try:
            # Get all available buckets
            buckets = await self.repository.list_buckets()
            configured_bucket = settings.minio_bucket
            
            status_info = {
                "configured_bucket": configured_bucket,
                "bucket_exists": configured_bucket in buckets,
                "total_buckets": len(buckets),
                "available_buckets": buckets,
                "minio_available": True
            }
            
            logger.info(f"Storage status: {status_info}")
            return status_info
            
        except Exception as e:
            logger.error(f"Failed to get bucket status: {e}")
            raise
    
    async def get_bucket_list(self) -> List[str]:
        """Get list of available buckets"""
        return await self.repository.list_buckets()
    
    async def check_storage_health(self) -> Dict[str, Any]:
        """Basic storage connectivity check"""
        try:
            # Test basic connectivity by listing buckets
            await self.repository.list_buckets()
            
            return {
                "status": "healthy",
                "service": "minio",
                "message": "Storage service is responding"
            }
        except Exception as e:
            logger.error(f"Storage health check failed: {e}")
            raise 