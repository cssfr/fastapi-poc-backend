"""Fast MinIO-based instrument metadata service with caching"""
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import asyncio
from .minio_client import minio_service, MINIO_BUCKET
from minio.error import S3Error

logger = logging.getLogger(__name__)

class FastMetadataService:
    """Fast metadata service with class-level caching"""
    
    # Class-level cache (shared across all instances)
    _metadata_cache: Dict[str, Dict[str, Any]] = {}
    _cache_timestamp: Optional[float] = None
    _cache_duration = 300  # 5 minutes (much shorter than old service)
    
    @classmethod
    async def get_all_metadata(cls) -> Dict[str, Dict[str, Any]]:
        """Get all instrument metadata with fast caching"""
        current_time = datetime.utcnow().timestamp()
        
        # Check cache validity
        if (cls._cache_timestamp and 
            current_time - cls._cache_timestamp < cls._cache_duration and
            cls._metadata_cache):
            logger.info(f"Returning cached metadata with {len(cls._metadata_cache)} instruments")
            return cls._metadata_cache
        
        # Refresh cache - using exact pattern from old service
        try:
            from .minio_client import minio_service, MINIO_BUCKET
            
            logger.info("Loading metadata from MinIO...")
            
            # Check if MinIO service is available
            if not minio_service.is_available():
                raise RuntimeError("MinIO service not available")
            
            # Check if metadata file exists
            exists = await minio_service.check_object_exists("metadata/instruments.json", MINIO_BUCKET)
            if not exists:
                logger.warning("Metadata file not found in MinIO. Using empty metadata.")
                metadata = {}
            else:
                # Get the metadata file - EXACTLY like old service
                response = minio_service.get_object_stream("metadata/instruments.json", MINIO_BUCKET)
                content = response.read().decode('utf-8')
                response.close()
                
                metadata = json.loads(content)
                logger.info(f"Successfully loaded metadata with {len(metadata)} instruments from MinIO")
            
            # Update cache
            cls._metadata_cache = metadata
            cls._cache_timestamp = current_time
            return metadata
            
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.warning("Metadata file not found in MinIO")
                metadata = {}
            else:
                logger.error(f"S3 error loading metadata: {e}")
                metadata = {}
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in metadata file: {e}")
            metadata = {}
        except Exception as e:
            logger.error(f"Failed to load metadata: {e}")
            metadata = {}
        
        # If we got here, something failed - check if we have cached data
        if cls._metadata_cache:
            logger.warning("Using cached metadata due to load failure")
            return cls._metadata_cache
        
        logger.warning("No metadata found in MinIO")
        return metadata
    
    @classmethod
    def get_cache_info(cls) -> Dict[str, Any]:
        """Get cache information"""
        return {
            "cached": bool(cls._metadata_cache),
            "cache_age": int(datetime.utcnow().timestamp() - cls._cache_timestamp) if cls._cache_timestamp else None,
            "cache_duration": cls._cache_duration,
            "instrument_count": len(cls._metadata_cache),
            "last_updated": datetime.fromtimestamp(cls._cache_timestamp).isoformat() if cls._cache_timestamp else None
        }
    
    @classmethod
    async def refresh_cache(cls) -> Dict[str, Any]:
        """Force refresh cache"""
        cls._cache_timestamp = None
        cls._metadata_cache = {}
        await cls.get_all_metadata()
        return {"success": True, "message": "Cache refreshed"}
    
    @classmethod
    async def upload_metadata(cls, metadata: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Upload metadata to MinIO"""
        try:
            from .minio_client import minio_service, MINIO_BUCKET
            
            # Check if MinIO service is available
            if not minio_service.is_available():
                raise RuntimeError("MinIO service not available")
            
            # Add timestamp
            metadata["_updated"] = datetime.utcnow().isoformat() + "Z"
            
            # Use the upload method (matching old service pattern)
            success = await minio_service.upload_json_object("metadata/instruments.json", metadata)
            if not success:
                raise RuntimeError("Failed to upload to MinIO")
            
            # Clear cache to force refresh on next request
            cls._metadata_cache = {}
            cls._cache_timestamp = None
            
            logger.info(f"Successfully uploaded metadata for {len(metadata)} instruments to MinIO")
            return {"success": True, "message": f"Uploaded metadata for {len(metadata)} instruments"}
            
        except Exception as e:
            logger.error(f"Failed to upload metadata to MinIO: {e}")
            raise

# Global service instance
fast_metadata_service = FastMetadataService()