"""Fast MinIO-based instrument metadata service with caching"""
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from .minio_client import minio_service, MINIO_BUCKET
from minio.error import S3Error

logger = logging.getLogger(__name__)

class FastMetadataService:
    """Fast metadata service with class-level caching"""
    
    # Class-level cache (shared across all instances)
    _metadata_cache: Dict[str, Dict[str, Any]] = {}
    _cache_timestamp: Optional[float] = None
    _cache_duration = 300  # 5 minutes
    
    async def get_all_metadata(self) -> Dict[str, Dict[str, Any]]:
        """Get all instrument metadata with fast caching"""
        current_time = datetime.utcnow().timestamp()
        
        # Check cache validity (using class variables for shared cache)
        if (self.__class__._cache_timestamp and 
            current_time - self.__class__._cache_timestamp < self.__class__._cache_duration and
            self.__class__._metadata_cache):
            logger.info(f"Returning cached metadata with {len(self.__class__._metadata_cache)} instruments")
            return self.__class__._metadata_cache
        
        # Refresh cache - COPY EXACT PATTERN FROM OLD SERVICE
        if not minio_service.is_available():
            logger.error("MinIO service not available")
            return self.__class__._metadata_cache if self.__class__._metadata_cache else {}
        
        try:
            # Check if metadata file exists
            exists = await minio_service.check_object_exists("metadata/instruments.json", MINIO_BUCKET)
            if not exists:
                logger.warning("Metadata file metadata/instruments.json not found in MinIO. Using empty metadata.")
                metadata = {}
            else:
                # Get the metadata file - EXACT COPY FROM OLD SERVICE
                response = minio_service.get_object_stream("metadata/instruments.json", MINIO_BUCKET)
                content = response.read().decode('utf-8')
                response.close()
                
                metadata = json.loads(content)
                logger.info(f"Loaded instrument metadata for {len(metadata)} instruments from MinIO")
            
            # Update cache (class variables for shared cache)
            self.__class__._metadata_cache = metadata
            self.__class__._cache_timestamp = current_time
            return metadata
            
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.warning("Metadata file metadata/instruments.json not found in MinIO")
                return {}
            logger.error(f"Failed to fetch metadata from MinIO: {e}")
            return self.__class__._metadata_cache if self.__class__._metadata_cache else {}
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in metadata file: {e}")
            return self.__class__._metadata_cache if self.__class__._metadata_cache else {}
        except Exception as e:
            logger.error(f"Unexpected error fetching metadata: {e}")
            return self.__class__._metadata_cache if self.__class__._metadata_cache else {}
    
    async def get_instruments_with_data(self, source_resolution: str = "1Y") -> List[str]:
        """Get list of instrument symbols that have data - compatible with old service API"""
        metadata = await self.get_all_metadata()
        
        symbols = []
        for symbol, data in metadata.items():
            if symbol.startswith("_"):  # Skip metadata fields like _schema_version
                continue
            symbols.append(symbol)  # Return just the symbol name, not the full data
        
        logger.info(f"Returning {len(symbols)} instrument symbols")
        return symbols

    async def get_metadata(self, symbol: str) -> Dict[str, Any]:
        """Get metadata for a specific instrument symbol - compatible with old service API"""
        all_metadata = await self.get_all_metadata()
        
        if symbol in all_metadata:
            return all_metadata[symbol]
        
        # Return default metadata with symbol-specific values
        default = {
            "exchange": "UNKNOWN",
            "market": "UNKNOWN", 
            "name": symbol,
            "shortName": symbol,
            "ticker": symbol,
            "type": "UNKNOWN",
            "currency": "USD",
            "description": f"Trading instrument: {symbol}",
            "sector": "UNKNOWN",
            "country": "UNKNOWN"
        }
        
        logger.info(f"No metadata found for {symbol}, using defaults")
        return default
        """Get cache information"""
        return {
            "cached": bool(cls._metadata_cache),
            "cache_age": int(datetime.utcnow().timestamp() - cls._cache_timestamp) if cls._cache_timestamp else None,
            "cache_duration": cls._cache_duration,
            "instrument_count": len(cls._metadata_cache),
            "last_updated": datetime.fromtimestamp(cls._cache_timestamp).isoformat() if cls._cache_timestamp else None
        }
    
    async def refresh_cache(self) -> Dict[str, Any]:
        """Force refresh cache"""
        self.__class__._cache_timestamp = None
        self.__class__._metadata_cache = {}
        await self.get_all_metadata()
        return {"success": True, "message": "Cache refreshed"}
    
    async def upload_metadata(self, metadata: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Upload metadata to MinIO"""
        if not minio_service.is_available():
            raise RuntimeError("MinIO service not available")
        
        try:
            # Upload using the service method
            success = await minio_service.upload_json_object("metadata/instruments.json", metadata)
            if not success:
                raise RuntimeError("Failed to upload to MinIO")
            
            # Clear cache to force refresh
            self.__class__._cache_timestamp = None
            self.__class__._metadata_cache = {}
            
            logger.info(f"Successfully uploaded metadata for {len(metadata)} instruments to MinIO")
            return {"success": True, "message": f"Uploaded metadata for {len(metadata)} instruments"}
            
        except Exception as e:
            logger.error(f"Failed to upload metadata to MinIO: {e}")
            raise

# Global service instance
fast_metadata_service = FastMetadataService()