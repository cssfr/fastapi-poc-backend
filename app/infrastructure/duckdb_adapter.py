"""DuckDB adapter for connection management and low-level query execution"""
import duckdb
import logging
from typing import List, Dict, Any, Optional
from app.minio_client import MinIOService, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

logger = logging.getLogger(__name__)

class DuckDBAdapter:
    """Low-level DuckDB adapter for connection management and query execution"""
    
    def __init__(self):
        self._conn = None
        self._is_configured = False
        
    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get DuckDB connection, creating it if necessary"""
        if self._conn is None:
            self._conn = duckdb.connect(':memory:', read_only=False)
            if MinIOService.is_available():
                self._configure_s3_settings()
            logger.info("DuckDB adapter initialized with S3 configuration")
        return self._conn
    
    def _configure_s3_settings(self):
        """Configure DuckDB S3 settings for MinIO"""
        if self._is_configured:
            return
            
        try:
            # Install and load httpfs extension for S3 access
            self._conn.execute("INSTALL httpfs;")
            self._conn.execute("LOAD httpfs;")
            
            # Configure S3 settings for MinIO
            self._conn.execute(f"SET s3_region='us-east-1';")
            self._conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
            self._conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
            self._conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
            self._conn.execute("SET s3_use_ssl=false;")
            self._conn.execute("SET s3_url_style='path';")
            
            self._is_configured = True
            logger.info("DuckDB S3 configuration completed")
        except Exception as e:
            logger.error(f"Failed to configure DuckDB S3 settings: {e}")
            raise
    
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries"""
        try:
            logger.debug(f"Executing DuckDB query: {query}")
            
            # Execute query
            result = self.conn.execute(query).fetchall()
            
            # Get column names
            columns = [desc[0] for desc in self.conn.description]
            
            # Convert to list of dicts
            return [dict(zip(columns, row)) for row in result]
            
        except Exception as e:
            logger.error(f"Failed to execute DuckDB query: {e}")
            raise
    
    async def execute_raw(self, query: str):
        """Execute a raw query and return the raw result"""
        try:
            return self.conn.execute(query)
        except Exception as e:
            logger.error(f"Failed to execute raw DuckDB query: {e}")
            raise
    
    def close(self):
        """Close DuckDB connection"""
        if self._conn:
            self._conn.close()
            self._conn = None
            self._is_configured = False
            logger.info("DuckDB connection closed")

# Global adapter instance
duckdb_adapter = DuckDBAdapter() 