"""MinIO client for accessing object storage"""
import os
from minio import Minio
from minio.error import S3Error
import logging
from typing import List, Optional
from datetime import datetime
import json
from io import BytesIO
from app.core.config import settings

logger = logging.getLogger(__name__)

# MinIO configuration from environment
MINIO_ENDPOINT = settings.minio_endpoint or "localhost:9000"
MINIO_ACCESS_KEY = settings.minio_access_key
MINIO_SECRET_KEY = settings.minio_secret_key
MINIO_SECURE = settings.minio_secure
MINIO_BUCKET = settings.minio_bucket

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
    
    # list_buckets() method moved to StorageRepository - use StorageService instead
    
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
    
    # get_object_url() method moved to StorageRepository - use StorageService instead
    
    # check_object_exists() method moved to StorageRepository - use StorageService instead
    
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

    @staticmethod
    async def upload_json_object(object_name: str, data: dict, bucket_name: str = MINIO_BUCKET) -> bool:
        """Upload JSON data as an object to MinIO"""
        if not minio_client:
            raise RuntimeError("MinIO client not configured")
        
        try:
            # Convert to JSON bytes
            json_content = json.dumps(data, indent=2)
            json_bytes = json_content.encode('utf-8')
            
            # Upload to MinIO
            minio_client.put_object(
                bucket_name,
                object_name,
                BytesIO(json_bytes),
                length=len(json_bytes),
                content_type='application/json'
            )
            
            logger.info(f"Successfully uploaded JSON object: {object_name}")
            return True
            
        except S3Error as e:
            logger.error(f"Failed to upload JSON object: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading JSON object: {e}")
            return False

# Export service instance
minio_service = MinIOService()