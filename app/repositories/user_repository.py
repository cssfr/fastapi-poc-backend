from typing import Optional, Dict
import uuid
from app.repositories.base_repository import BaseRepository
from app.database import db

class UserRepository(BaseRepository):
    def _table_name(self) -> str:
        return "users"
    
    def _entity_class(self) -> type:
        return dict  # Returns raw database rows as dicts
    
    async def get_by_id(self, conn, user_id: uuid.UUID) -> Optional[Dict]:
        """Get user by ID"""
        return await db.fetch_one_in_transaction(
            conn,
            "SELECT * FROM users WHERE id = $1",
            user_id
        )
    
    async def update_email(self, conn, user_id: uuid.UUID, email: str) -> Optional[Dict]:
        """Update user email"""
        return await db.fetch_one_in_transaction(
            conn,
            "UPDATE users SET email = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 RETURNING *",
            email,
            user_id
        )
    
    async def create(self, conn, user_id: uuid.UUID, email: str) -> Optional[Dict]:
        """Create new user"""
        return await db.fetch_one_in_transaction(
            conn,
            "INSERT INTO users (id, email) VALUES ($1, $2) RETURNING *",
            user_id,
            email
        ) 