"""MinIO-based instrument metadata service"""
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import asyncio
from .minio_client import minio_service, MINIO_BUCKET
from minio.error import S3Error

logger = logging.getLogger(__name__)

class InstrumentMetadataService:
    """Service for managing instrument metadata stored in MinIO"""
    
    def __init__(self, metadata_path: str = "metadata/instruments.json"):
        self.metadata_path = metadata_path
        self._metadata_cache = {}
        self._cache_timestamp = None
        self.cache_duration = 3600  # 1 hour cache
        self._default_metadata = {
            "exchange": "UNKNOWN",
            "market": "UNKNOWN",
            "name": None,  # Will use symbol if None
            "shortName": None,  # Will use symbol if None
            "ticker": None,  # Will use symbol if None
            "type": "UNKNOWN",
            "currency": "USD",
            "description": None,  # Will generate if None
            "sector": "UNKNOWN",
            "country": "UNKNOWN"
        }
    
    async def _fetch_metadata_from_minio(self) -> Dict[str, Any]:
        """Fetch instrument metadata from MinIO"""
        if not minio_service.is_available():
            raise RuntimeError("MinIO service not available")
        
        try:
            # Check if metadata file exists
            exists = await minio_service.check_object_exists(self.metadata_path, MINIO_BUCKET)
            if not exists:
                logger.warning(f"Metadata file {self.metadata_path} not found in MinIO. Using empty metadata.")
                return {}
            
            # Get the metadata file
            response = minio_service.get_object_stream(self.metadata_path, MINIO_BUCKET)
            content = response.read().decode('utf-8')
            response.close()
            
            metadata = json.loads(content)
            logger.info(f"Loaded instrument metadata for {len(metadata)} instruments from MinIO")
            return metadata
            
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.warning(f"Metadata file {self.metadata_path} not found in MinIO")
                return {}
            logger.error(f"Failed to fetch metadata from MinIO: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in metadata file: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching metadata: {e}")
            raise
    
    async def get_metadata(self, symbol: str) -> Dict[str, Any]:
        """Get metadata for a specific instrument symbol"""
        all_metadata = await self.get_all_metadata()
        
        if symbol in all_metadata:
            return all_metadata[symbol]
        
        # Return default metadata with symbol-specific values
        default = self._default_metadata.copy()
        default.update({
            "name": default["name"] or symbol,
            "shortName": default["shortName"] or symbol,
            "ticker": default["ticker"] or symbol,
            "description": default["description"] or f"Trading instrument: {symbol}"
        })
        
        logger.info(f"No metadata found for {symbol}, using defaults")
        return default
    
    async def get_all_metadata(self) -> Dict[str, Dict[str, Any]]:
        """Get all instrument metadata with caching"""
        current_time = datetime.utcnow().timestamp()
        
        # Check cache validity
        if (self._cache_timestamp and 
            current_time - self._cache_timestamp < self.cache_duration and
            self._metadata_cache):
            return self._metadata_cache
        
        # Refresh cache
        try:
            metadata = await self._fetch_metadata_from_minio()
            self._metadata_cache = metadata
            self._cache_timestamp = current_time
            logger.info(f"Refreshed metadata cache with {len(metadata)} instruments")
            return metadata
        except Exception as e:
            # If we have cached data and fetch fails, use cached data
            if self._metadata_cache:
                logger.warning(f"Failed to refresh metadata, using cached data: {e}")
                return self._metadata_cache
            
            # No cached data and fetch failed, return empty
            logger.error(f"No cached metadata and fetch failed: {e}")
            return {}
    
    async def refresh_cache(self) -> bool:
        """Force refresh of metadata cache"""
        try:
            self._cache_timestamp = None
            await self.get_all_metadata()
            return True
        except Exception as e:
            logger.error(f"Failed to refresh metadata cache: {e}")
            return False
    
    async def upload_metadata(self, metadata: Dict[str, Dict[str, Any]]) -> bool:
        """Upload instrument metadata to MinIO"""
        if not minio_service.is_available():
            raise RuntimeError("MinIO service not available")
        
        try:
            # Upload using the service method
            success = await minio_service.upload_json_object(self.metadata_path, metadata)
            if not success:
                raise RuntimeError("Failed to upload to MinIO")
            
            # Clear cache to force refresh
            self._cache_timestamp = None
            self._metadata_cache = {}
            
            logger.info(f"Successfully uploaded metadata for {len(metadata)} instruments to MinIO")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload metadata to MinIO: {e}")
            raise
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about the current cache state"""
        return {
            "cached": bool(self._metadata_cache),
            "cache_age": int(datetime.utcnow().timestamp() - self._cache_timestamp) if self._cache_timestamp else None,
            "cache_duration": self.cache_duration,
            "instrument_count": len(self._metadata_cache),
            "last_updated": datetime.fromtimestamp(self._cache_timestamp).isoformat() if self._cache_timestamp else None
        }

# Global service instance
instrument_metadata_service = InstrumentMetadataService()