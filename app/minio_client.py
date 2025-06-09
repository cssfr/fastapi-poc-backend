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
    
    @staticmethod
    async def get_storage_structure_info(source_resolution: str = "1m") -> dict:
        """Get information about the storage structure for a given resolution"""
        if not minio_client:
            raise RuntimeError("MinIO client not configured")
        
        try:
            prefix = f"ohlcv/{source_resolution}/"
            objects = await MinIOService.list_objects(MINIO_BUCKET, prefix=prefix)
            
            symbols = set()
            date_ranges = {}
            total_files = 0
            total_size = 0
            
            for obj in objects:
                total_files += 1
                total_size += obj["size"]
                
                # Parse object path based on structure
                parts = obj["name"].split("/")
                
                if source_resolution == "1Y":
                    # Parse: ohlcv/1Y/symbol=BTC/year=2017/BTC_2017.parquet
                    if len(parts) >= 4 and parts[2].startswith("symbol=") and parts[3].startswith("year="):
                        symbol = parts[2].replace("symbol=", "")
                        year = parts[3].replace("year=", "")
                        symbols.add(symbol)
                        
                        if symbol not in date_ranges:
                            date_ranges[symbol] = {"min": year, "max": year}
                        else:
                            if year < date_ranges[symbol]["min"]:
                                date_ranges[symbol]["min"] = year
                            if year > date_ranges[symbol]["max"]:
                                date_ranges[symbol]["max"] = year
                else:
                    # Parse: ohlcv/1m/symbol=DAX/date=2013-10-01/DAX_2013-10-01.parquet
                    if len(parts) >= 4 and parts[2].startswith("symbol=") and parts[3].startswith("date="):
                        symbol = parts[2].replace("symbol=", "")
                        date_str = parts[3].replace("date=", "")
                        symbols.add(symbol)
                        
                        if symbol not in date_ranges:
                            date_ranges[symbol] = {"min": date_str, "max": date_str}
                        else:
                            if date_str < date_ranges[symbol]["min"]:
                                date_ranges[symbol]["min"] = date_str
                            if date_str > date_ranges[symbol]["max"]:
                                date_ranges[symbol]["max"] = date_str
            
            return {
                "source_resolution": source_resolution,
                "total_files": total_files,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "symbol_count": len(symbols),
                "symbols": sorted(list(symbols)),
                "date_ranges": date_ranges
            }
            
        except S3Error as e:
            logger.error(f"Failed to get storage structure info: {e}")
            raise
    
    @staticmethod
    async def compare_storage_structures() -> dict:
        """Compare storage structures between 1m and 1Y"""
        try:
            info_1m = await MinIOService.get_storage_structure_info("1m")
            info_1y = await MinIOService.get_storage_structure_info("1Y")
            
            # Calculate efficiency metrics
            comparison = {
                "1m": info_1m,
                "1Y": info_1y,
                "comparison": {
                    "file_count_reduction": info_1m["total_files"] - info_1y["total_files"],
                    "file_count_reduction_percent": round(
                        ((info_1m["total_files"] - info_1y["total_files"]) / info_1m["total_files"]) * 100, 2
                    ) if info_1m["total_files"] > 0 else 0,
                    "size_difference_mb": round(info_1y["total_size_mb"] - info_1m["total_size_mb"], 2),
                    "common_symbols": sorted(list(set(info_1m["symbols"]) & set(info_1y["symbols"]))),
                    "only_in_1m": sorted(list(set(info_1m["symbols"]) - set(info_1y["symbols"]))),
                    "only_in_1Y": sorted(list(set(info_1y["symbols"]) - set(info_1m["symbols"])))
                }
            }
            
            return comparison
            
        except Exception as e:
            logger.error(f"Failed to compare storage structures: {e}")
            raise

# Export service instance
minio_service = MinIOService()