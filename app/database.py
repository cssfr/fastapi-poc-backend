import os
import asyncpg
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
import logging
import urllib.parse

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL environment variable")

class Database:
    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Initialize database connection pool"""
        try:
            # Parse DATABASE_URL to check if SSL is needed
            parsed = urllib.parse.urlparse(DATABASE_URL)
            
            # Determine SSL mode
            ssl_mode = None
            if parsed.hostname and not parsed.hostname.endswith('.local'):
                ssl_mode = 'prefer'  # Try SSL first, fall back if not available
            
            self._pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=1,
                max_size=5,  # Reduced for Supabase
                max_queries=50000,
                max_inactive_connection_lifetime=300,
                command_timeout=60,
                ssl=ssl_mode
            )
            logger.info("Database connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create database connection pool: {e}")
            raise
    
    async def disconnect(self):
        """Close database connection pool"""
        if self._pool:
            await self._pool.close()
            logger.info("Database connection pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a database connection from the pool"""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        
        async with self._pool.acquire() as connection:
            yield connection
    
    @asynccontextmanager
    async def transaction(self):
        """Create a database transaction context"""
        async with self.get_connection() as conn:
            async with conn.transaction():
                yield conn
    
    async def fetch_one(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Execute a query and fetch one row"""
        async with self.get_connection() as conn:
            row = await conn.fetchrow(query, *args)
            return dict(row) if row else None
    
    async def fetch_all(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a query and fetch all rows"""
        async with self.get_connection() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]
    
    async def execute(self, query: str, *args) -> str:
        """Execute a query and return status"""
        async with self.get_connection() as conn:
            return await conn.execute(query, *args)
    
    async def fetch_val(self, query: str, *args) -> Any:
        """Execute a query and fetch a single value"""
        async with self.get_connection() as conn:
            return await conn.fetchval(query, *args)
    
    # New transaction-aware methods
    async def fetch_one_in_transaction(self, conn: asyncpg.Connection, query: str, *args) -> Optional[Dict[str, Any]]:
        """Execute a query and fetch one row within a transaction"""
        row = await conn.fetchrow(query, *args)
        return dict(row) if row else None
    
    async def fetch_all_in_transaction(self, conn: asyncpg.Connection, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a query and fetch all rows within a transaction"""
        rows = await conn.fetch(query, *args)
        return [dict(row) for row in rows]
    
    async def execute_in_transaction(self, conn: asyncpg.Connection, query: str, *args) -> str:
        """Execute a query within a transaction"""
        return await conn.execute(query, *args)
    
    async def fetch_val_in_transaction(self, conn: asyncpg.Connection, query: str, *args) -> Any:
        """Execute a query and fetch a single value within a transaction"""
        return await conn.fetchval(query, *args)
    
    async def execute_many(self, query: str, args_list: List[tuple]) -> None:
        """Execute multiple queries in a single transaction"""
        async with self.transaction() as conn:
            await conn.executemany(query, args_list)

# Global database instance
db = Database()