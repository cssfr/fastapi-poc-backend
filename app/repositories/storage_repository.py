from typing import List, Dict, Optional
from minio import Minio
from minio.error import S3Error
import logging

logger = logging.getLogger(__name__)

class StorageRepository:
    def __init__(self, minio_client: Minio):
        self.client = minio_client
    
    async def list_buckets(self) -> List[str]:
        """List all available buckets"""
        if not self.client:
            raise RuntimeError("MinIO client not configured")
        
        try:
            buckets = self.client.list_buckets()
            return [bucket.name for bucket in buckets]
        except S3Error as e:
            logger.error(f"Failed to list buckets: {e}")
            raise
    
    async def list_objects(self, bucket: str, prefix: str = "") -> List[Dict]:
        """List objects in bucket with prefix"""
        if not self.client:
            raise RuntimeError("MinIO client not configured")
        
        try:
            objects = []
            for obj in self.client.list_objects(bucket, prefix=prefix, recursive=True):
                objects.append({
                    "name": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                    "etag": obj.etag
                })
            return objects
        except S3Error as e:
            logger.error(f"Failed to list objects: {e}")
            raise
    
    async def check_object_exists(self, bucket: str, object_name: str) -> bool:
        """Check if object exists"""
        if not self.client:
            raise RuntimeError("MinIO client not configured")
        
        try:
            self.client.stat_object(bucket, object_name)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            logger.error(f"Failed to check object existence: {e}")
            raise
    
    async def get_presigned_url(self, bucket: str, object_name: str, 
                               expires: int = 3600) -> str:
        """Generate presigned URL for object access"""
        if not self.client:
            raise RuntimeError("MinIO client not configured")
        
        try:
            # Generate presigned URL valid for specified time
            url = self.client.presigned_get_object(
                bucket,
                object_name,
                expires=expires
            )
            return url
        except S3Error as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            raise 