"""
Fast Instrument Metadata Service - No File Scanning
Reads data boundaries directly from metadata instead of scanning files
"""
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)

class FastInstrumentMetadataService:
    """Fast metadata service that reads boundaries from metadata file"""
    
    def __init__(self):
        self._cache = {}
        self._cache_timestamp = None
        self._cache_duration = 3600  # 1 hour cache
        
    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid"""
        if not self._cache_timestamp:
            return False
        
        age = datetime.utcnow().timestamp() - self._cache_timestamp
        return age < self._cache_duration
    
    async def get_all_metadata(self) -> Dict[str, Any]:
        """Get all metadata with caching"""
        if self._is_cache_valid() and self._cache:
            logger.debug("Returning cached metadata")
            return self._cache
        
        try:
            # Import here to avoid circular imports
            from .minio_client import minio_service
            
            logger.info("Loading metadata from MinIO...")
            # Try different method names based on common MinIO service patterns
            metadata = None
            
            # Option 1: Try get_object_as_json (common pattern)
            if hasattr(minio_service, 'get_object_as_json'):
                metadata = await minio_service.get_object_as_json("metadata/instruments.json")
            # Option 2: Try get_json (another common pattern)
            elif hasattr(minio_service, 'get_json'):
                metadata = await minio_service.get_json("metadata/instruments.json")
            # Option 3: Try download_json (another pattern)
            elif hasattr(minio_service, 'download_json'):
                metadata = await minio_service.download_json("metadata/instruments.json")
            # Option 4: Try generic get_object and parse JSON
            elif hasattr(minio_service, 'get_object'):
                content = await minio_service.get_object("metadata/instruments.json")
                if content:
                    import json
                    metadata = json.loads(content)
            # Option 5: Try download_object
            elif hasattr(minio_service, 'download_object'):
                content = await minio_service.download_object("metadata/instruments.json")
                if content:
                    import json
                    metadata = json.loads(content)
            else:
                logger.error("No suitable method found on MinIO service")
                logger.info(f"Available methods: {[method for method in dir(minio_service) if not method.startswith('_')]}")
                metadata = {}
            
            if metadata:
                self._cache = metadata
                self._cache_timestamp = datetime.utcnow().timestamp()
                logger.info(f"Loaded metadata for {len([k for k in metadata.keys() if not k.startswith('_')])} instruments")
            else:
                logger.warning("No metadata found in MinIO")
                metadata = {}
                
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to load metadata: {e}")
            # Return cached data if available, empty dict otherwise
            return self._cache if self._cache else {}
    
    async def get_metadata(self, symbol: str) -> Dict[str, Any]:
        """Get metadata for a specific symbol"""
        all_metadata = await self.get_all_metadata()
        
        if symbol in all_metadata:
            return all_metadata[symbol]
        
        # Return default metadata if not found
        logger.warning(f"No metadata found for symbol {symbol}, using defaults")
        return {
            "exchange": "UNKNOWN",
            "market": "UNKNOWN", 
            "name": symbol,
            "shortName": symbol,
            "ticker": symbol,
            "type": "UNKNOWN",
            "currency": "USD",
            "description": f"Trading instrument: {symbol}",
            "sector": "UNKNOWN",
            "country": "UNKNOWN",
            "dataRange": None  # No boundary data available
        }
    
    async def get_instruments_with_data(self, source_resolution: str = "1Y") -> List[str]:
        """Get list of instruments that have data available"""
        all_metadata = await self.get_all_metadata()
        
        instruments = []
        for symbol, metadata in all_metadata.items():
            if symbol.startswith("_"):
                continue  # Skip schema fields
                
            # Check if instrument has data range information
            data_range = metadata.get("dataRange")
            if data_range:
                # Check if the specific source has data
                sources = data_range.get("sources", {})
                if source_resolution in sources:
                    instruments.append(symbol)
                elif source_resolution == "both" and sources:
                    instruments.append(symbol)
                elif not sources and data_range.get("earliest"):
                    # Fallback: has general data range
                    instruments.append(symbol)
        
        return sorted(instruments)
    
    async def get_data_boundaries(self, symbol: str, source_resolution: str = "1Y") -> Dict[str, Optional[str]]:
        """Get data boundaries for a specific symbol"""
        metadata = await self.get_metadata(symbol)
        data_range = metadata.get("dataRange")
        
        if not data_range:
            return {"earliest": None, "latest": None}
        
        # Try to get source-specific boundaries first
        sources = data_range.get("sources", {})
        if source_resolution in sources:
            source_data = sources[source_resolution]
            return {
                "earliest": source_data.get("earliest"),
                "latest": source_data.get("latest")
            }
        
        # Fallback to general boundaries
        return {
            "earliest": data_range.get("earliest"),
            "latest": data_range.get("latest")
        }
    
    async def upload_metadata(self, metadata: Dict[str, Any]) -> bool:
        """Upload metadata to MinIO"""
        try:
            # Import here to avoid circular imports
            from .minio_client import minio_service
            
            # Add timestamp
            metadata["_updated"] = datetime.utcnow().isoformat() + "Z"
            
            # Try different upload method names
            success = False
            if hasattr(minio_service, 'upload_json_object'):
                success = await minio_service.upload_json_object("metadata/instruments.json", metadata)
            elif hasattr(minio_service, 'upload_json'):
                success = await minio_service.upload_json("metadata/instruments.json", metadata)
            elif hasattr(minio_service, 'put_json'):
                success = await minio_service.put_json("metadata/instruments.json", metadata)
            elif hasattr(minio_service, 'put_object'):
                import json
                content = json.dumps(metadata, indent=2)
                success = await minio_service.put_object("metadata/instruments.json", content)
            elif hasattr(minio_service, 'upload_object'):
                import json
                content = json.dumps(metadata, indent=2)
                success = await minio_service.upload_object("metadata/instruments.json", content)
            else:
                logger.error("No suitable upload method found on MinIO service")
                success = False
            
            if success:
                # Clear cache to force reload
                self._cache = {}
                self._cache_timestamp = None
                logger.info("Metadata uploaded and cache cleared")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to upload metadata: {e}")
            return False
    
    async def refresh_cache(self) -> bool:
        """Force refresh of cache"""
        try:
            self._cache_timestamp = None
            metadata = await self.get_all_metadata()
            return bool(metadata)
        except Exception as e:
            logger.error(f"Failed to refresh cache: {e}")
            return False
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about current cache state"""
        if not self._cache_timestamp:
            return {"cached": False}
        
        age = int(datetime.utcnow().timestamp() - self._cache_timestamp)
        instrument_count = len([k for k in self._cache.keys() if not k.startswith("_")]) if self._cache else 0
        
        return {
            "cached": True,
            "cache_age": age,
            "cache_duration": self._cache_duration,
            "instrument_count": instrument_count,
            "last_updated": self._cache.get("_updated"),
            "boundaries_updated": self._cache.get("_data_boundaries_updated"),
            "boundary_source": self._cache.get("_boundary_update_source", "unknown")
        }

# Create singleton instance
fast_metadata_service = FastInstrumentMetadataService()