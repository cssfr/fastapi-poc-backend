# Add metrics collection for:
# - Query execution time
# - Cache hit/miss rates  
# - Data volume processed
# - S3/MinIO access patterns

from time import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    def __init__(self):
        self.metrics = {}
        self.query_stats = {}
        self.cache_stats = {"hits": 0, "misses": 0}
        
    async def track_query(self, query_type: str, symbol: str, start_time: float = None) -> Dict[str, Any]:
        """Track query performance metrics"""
        if start_time is None:
            start_time = time()
            
        return {
            "query_type": query_type,
            "symbol": symbol,
            "start_time": start_time,
            "monitor": self
        }
    
    async def complete_query(self, tracking_info: Dict[str, Any], 
                           record_count: int = 0, 
                           cache_hit: bool = False,
                           data_size_bytes: int = 0) -> Dict[str, Any]:
        """Complete query tracking and log metrics"""
        end_time = time()
        duration = end_time - tracking_info["start_time"]
        
        query_type = tracking_info["query_type"]
        symbol = tracking_info["symbol"]
        
        # Update cache stats
        if cache_hit:
            self.cache_stats["hits"] += 1
        else:
            self.cache_stats["misses"] += 1
        
        # Update query stats
        if query_type not in self.query_stats:
            self.query_stats[query_type] = {
                "total_queries": 0,
                "total_duration": 0,
                "total_records": 0,
                "total_data_bytes": 0
            }
        
        stats = self.query_stats[query_type]
        stats["total_queries"] += 1
        stats["total_duration"] += duration
        stats["total_records"] += record_count
        stats["total_data_bytes"] += data_size_bytes
        
        # Calculate averages
        avg_duration = stats["total_duration"] / stats["total_queries"]
        avg_records = stats["total_records"] / stats["total_queries"]
        
        # Log detailed metrics
        logger.info(f"Query performance", extra={
            "query_type": query_type,
            "symbol": symbol,
            "duration_ms": round(duration * 1000, 2),
            "record_count": record_count,
            "cache_hit": cache_hit,
            "data_size_mb": round(data_size_bytes / (1024 * 1024), 2) if data_size_bytes else 0,
            "avg_duration_ms": round(avg_duration * 1000, 2),
            "avg_records": round(avg_records, 0)
        })
        
        return {
            "duration_seconds": duration,
            "record_count": record_count,
            "cache_hit": cache_hit,
            "data_size_bytes": data_size_bytes
        }
    
    def get_cache_hit_rate(self) -> float:
        """Calculate cache hit rate percentage"""
        total = self.cache_stats["hits"] + self.cache_stats["misses"]
        if total == 0:
            return 0.0
        return (self.cache_stats["hits"] / total) * 100
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        summary = {
            "cache_stats": {
                "hits": self.cache_stats["hits"],
                "misses": self.cache_stats["misses"],
                "hit_rate_percent": round(self.get_cache_hit_rate(), 2)
            },
            "query_types": {}
        }
        
        # Add query type summaries
        for query_type, stats in self.query_stats.items():
            if stats["total_queries"] > 0:
                summary["query_types"][query_type] = {
                    "total_queries": stats["total_queries"],
                    "avg_duration_ms": round((stats["total_duration"] / stats["total_queries"]) * 1000, 2),
                    "avg_records": round(stats["total_records"] / stats["total_queries"], 0),
                    "total_data_mb": round(stats["total_data_bytes"] / (1024 * 1024), 2),
                    "avg_data_mb": round((stats["total_data_bytes"] / stats["total_queries"]) / (1024 * 1024), 2)
                }
        
        return summary
    
    def reset_metrics(self):
        """Reset all performance metrics"""
        self.metrics = {}
        self.query_stats = {}
        self.cache_stats = {"hits": 0, "misses": 0}
        logger.info("Performance metrics reset")
    
    async def track_s3_access(self, operation: str, bucket: str, key_pattern: str, 
                            duration: float, success: bool = True):
        """Track S3/MinIO access patterns"""
        if "s3_access" not in self.metrics:
            self.metrics["s3_access"] = []
        
        access_info = {
            "timestamp": datetime.utcnow().isoformat(),
            "operation": operation,
            "bucket": bucket,
            "key_pattern": key_pattern,
            "duration_ms": round(duration * 1000, 2),
            "success": success
        }
        
        self.metrics["s3_access"].append(access_info)
        
        # Keep only last 100 entries to prevent memory growth
        if len(self.metrics["s3_access"]) > 100:
            self.metrics["s3_access"] = self.metrics["s3_access"][-100:]
        
        logger.debug(f"S3 access tracked: {operation} on {bucket}/{key_pattern} - {duration*1000:.2f}ms")

# Global performance monitor instance
performance_monitor = PerformanceMonitor() 