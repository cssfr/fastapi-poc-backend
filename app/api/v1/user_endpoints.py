from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.security import HTTPAuthorizationCredentials
from app.auth import get_user_info, security, verify_token
from app.services.user_service import UserService
from app.models import UserResponse
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/users",
    tags=["users"],
    # Remove router-level auth to allow OPTIONS preflight requests
)

@router.get("/", response_model=UserResponse)
async def get_current_user(request: Request, user_id: str = Depends(verify_token), cred: HTTPAuthorizationCredentials = Depends(security)):
    """Get current user profile"""
    try:
        user_info = get_user_info(cred)
        user_id = user_info["user_id"]
        
        logger.info(
            "Fetching user profile",
            extra={
                "user_id": user_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        # Get or create user in database
        service = UserService()
        user = await service.get_or_create_user(
            user_id=user_id,
            email=user_info.get("email", f"user_{user_id}@placeholder.com")
        )
        
        logger.info(
            "User profile retrieved successfully",
            extra={
                "user_id": user_id,
                "request_id": getattr(request.state, "request_id", "unknown")
            }
        )
        
        return user
        
    except Exception as e:
        logger.error(f"Error fetching user profile: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch user profile"
        ) 