# Create app/infrastructure/cache.py
# Instructions:
# 1. Use Redis or in-memory cache for market data
# 2. Cache key pattern: f"ohlcv:{symbol}:{timeframe}:{date_hash}"
# 3. TTL: Infinite for historical data, 1 minute for current day

from typing import Optional, Any
import hashlib
import json
import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)

class MarketDataCache:
    def __init__(self, redis_client=None):
        self.redis = redis_client
        self._memory_cache = {}  # Fallback in-memory cache
        
    def _generate_key(self, symbol: str, timeframe: str, 
                     start: int, end: int) -> str:
        """Generate cache key for market data query"""
        date_hash = hashlib.md5(f"{start}:{end}".encode()).hexdigest()[:8]
        return f"ohlcv:{symbol}:{timeframe}:{date_hash}"
    
    def _is_current_day(self, end_timestamp: int) -> bool:
        """Check if the query includes current day data"""
        end_date = date.fromtimestamp(end_timestamp)
        today = date.today()
        return end_date >= today
    
    def _get_ttl(self, end_timestamp: int) -> Optional[int]:
        """Get TTL for cache entry based on whether it includes current day"""
        if self._is_current_day(end_timestamp):
            return 60  # 1 minute for current day data
        else:
            return None  # Infinite TTL for historical data
    
    async def get(self, key: str) -> Optional[Any]:
        """Get cached data"""
        if self.redis:
            try:
                cached_data = await self.redis.get(key)
                if cached_data:
                    return json.loads(cached_data)
            except Exception as e:
                logger.warning(f"Redis cache get failed: {e}")
                
        # Fallback to memory cache
        cached_entry = self._memory_cache.get(key)
        if cached_entry:
            # Memory cache stores (value, expiration_timestamp) tuples
            value, expiration_timestamp = cached_entry
            # TODO: Implement TTL expiration check here in the future
            return value
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set cached data"""
        serialized_value = json.dumps(value, default=str)
        
        if self.redis:
            try:
                if ttl:
                    await self.redis.setex(key, ttl, serialized_value)
                else:
                    await self.redis.set(key, serialized_value)
                return
            except Exception as e:
                logger.warning(f"Redis cache set failed: {e}")
        
        # Fallback to memory cache (with basic TTL simulation)
        expiration_timestamp = datetime.now().timestamp() + ttl if ttl else None
        self._memory_cache[key] = (value, expiration_timestamp)
        
        # For memory cache, we don't implement TTL expiration
        # In production, you'd want to use a proper cache library
        logger.debug(f"Stored in memory cache: {key}")
    
    async def get_market_data(self, symbol: str, timeframe: str, 
                            start_timestamp: int, end_timestamp: int) -> Optional[Any]:
        """Get market data from cache with automatic key generation"""
        key = self._generate_key(symbol, timeframe, start_timestamp, end_timestamp)
        return await self.get(key)
    
    async def set_market_data(self, symbol: str, timeframe: str, 
                            start_timestamp: int, end_timestamp: int, data: Any):
        """Set market data in cache with automatic key generation and TTL"""
        key = self._generate_key(symbol, timeframe, start_timestamp, end_timestamp)
        ttl = self._get_ttl(end_timestamp)
        await self.set(key, data, ttl)
        
        cache_type = "historical" if ttl is None else "current_day"
        logger.info(f"Cached market data for {symbol} {timeframe} ({cache_type}): {key}")
    
    def get_cache_stats(self) -> dict:
        """Get basic cache statistics"""
        return {
            "redis_available": self.redis is not None,
            "memory_cache_size": len(self._memory_cache),
            "memory_cache_keys": list(self._memory_cache.keys())[:10]  # Show first 10 keys
        }

# Global cache instance
market_data_cache = MarketDataCache() 