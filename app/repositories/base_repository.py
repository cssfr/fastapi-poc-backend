from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional, List, Dict, Any
from app.database import db
import uuid

T = TypeVar('T')

class BaseRepository(ABC, Generic[T]):
    def __init__(self, database=None):
        self.db = database or db
    
    @abstractmethod
    def _table_name(self) -> str:
        """Return the table name for this repository"""
        pass
    
    @abstractmethod
    def _entity_class(self) -> type:
        """Return the entity class for this repository"""
        pass
    
    async def execute_in_transaction(self, queries: List[tuple]):
        """Execute multiple queries in a transaction"""
        async with self.db.transaction():
            results = []
            for query, *params in queries:
                result = await self.db.fetch_one(query, *params)
                results.append(result)
            return results 