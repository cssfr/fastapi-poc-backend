"""MinIO client for accessing object storage"""
import os
from minio import Minio
from minio.error import S3Error
import logging
from typing import List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# MinIO configuration from environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "dukascopy-node")

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    logger.warning("MinIO credentials not configured. MinIO features will be disabled.")
    minio_client = None
else:
    try:
        # Initialize MinIO client
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        logger.info(f"MinIO client initialized for endpoint: {MINIO_ENDPOINT}")
    except Exception as e:
        logger.error(f"Failed to initialize MinIO client: {e}")
        minio_client = None

class MinIOService:
    """Service for interacting with MinIO storage"""
    
    @staticmethod
    def is_available() -> bool:
        """Check if MinIO is configured and available"""
        return minio_client is not None
    
    @staticmethod
    async def list_buckets() -> List[str]:
        """List all available buckets"""
        if not minio_client:
            raise RuntimeError("MinIO client not configured")
        
        try:
            buckets = minio_client.list_buckets()
            return [bucket.name for bucket in buckets]
        except S3Error as e:
            logger.error(f"Failed to list buckets: {e}")
            raise
    
    @staticmethod
    async def list_objects(bucket_name: str = MINIO_BUCKET, prefix: str = "") -> List[dict]:
        """List objects in a bucket with optional prefix"""
        if not minio_client:
            raise RuntimeError("MinIO client not configured")
        
        try:
            objects = []
            for obj in minio_client.list_objects(bucket_name, prefix=prefix, recursive=True):
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
    
    @staticmethod
    async def get_object_url(object_name: str, bucket_name: str = MINIO_BUCKET) -> str:
        """Get a presigned URL for an object (valid for 1 hour)"""
        if not minio_client:
            raise RuntimeError("MinIO client not configured")
        
        try:
            # Generate presigned URL valid for 1 hour
            url = minio_client.presigned_get_object(
                bucket_name,
                object_name,
                expires=3600
            )
            return url
        except S3Error as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            raise
    
    @staticmethod
    async def check_object_exists(object_name: str, bucket_name: str = MINIO_BUCKET) -> bool:
        """Check if an object exists in the bucket"""
        if not minio_client:
            raise RuntimeError("MinIO client not configured")
        
        try:
            minio_client.stat_object(bucket_name, object_name)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            logger.error(f"Failed to check object existence: {e}")
            raise
    
    @staticmethod
    def get_object_stream(object_name: str, bucket_name: str = MINIO_BUCKET):
        """Get an object as a stream (for DuckDB to read)"""
        if not minio_client:
            raise RuntimeError("MinIO client not configured")
        
        try:
            response = minio_client.get_object(bucket_name, object_name)
            return response
        except S3Error as e:
            logger.error(f"Failed to get object stream: {e}")
            raise
        
# Export service instance
minio_service = MinIOService()