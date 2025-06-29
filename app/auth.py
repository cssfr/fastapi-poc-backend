from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
import os
from typing import Dict, Any
from app.core.config import settings

security = HTTPBearer()

SUPABASE_URL = settings.supabase_url
JWT_SECRET = settings.supabase_jwt_secret

if not JWT_SECRET or not SUPABASE_URL:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_JWT_SECRET")

def verify_token(cred: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """
    Verify JWT token and return user ID
    """
    token = cred.credentials
    try:
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=["HS256"],
            options={"verify_aud": False}
        )
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return user_id

def get_user_info(cred: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """
    Extract user information from JWT token
    """
    token = cred.credentials
    try:
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=["HS256"],
            options={"verify_aud": False}
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user_id = payload.get("sub")
    email = payload.get("email")
    
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return {
        "user_id": user_id,
        "email": email or f"user-{user_id}@example.com",
        "payload": payload
    }