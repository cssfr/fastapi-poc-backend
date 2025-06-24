import uuid
import logging
from app.database import db
from app.models import UserResponse
from app.repositories.user_repository import UserRepository
from app.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)

class UserService:
    def __init__(self, repository: UserRepository = None):
        self.repository = repository or UserRepository()
    async def get_or_create_user(self, user_id: str, email: str) -> UserResponse:
        """Get or create a user with transaction support"""
        async with db.transaction() as conn:
            # Try to get existing user
            user = await self.repository.get_by_id(conn, uuid.UUID(user_id))
            
            if user:
                # Update email if different (business logic)
                if user['email'] != email:
                    user = await self.repository.update_email(conn, uuid.UUID(user_id), email)
                return UserResponse(**user)
            
            # Create new user
            user = await self.repository.create(conn, uuid.UUID(user_id), email)
            
            if user:
                return UserResponse(**user)
            raise DatabaseError("Failed to create user") 