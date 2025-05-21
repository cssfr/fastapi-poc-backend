from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
import os

security = HTTPBearer()

SUPABASE_URL = os.getenv("SUPABASE_URL")
JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")

if not JWT_SECRET or not SUPABASE_URL:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_JWT_SECRET")

def verify_token(cred: HTTPAuthorizationCredentials = Depends(security)):
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
        )
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
        )
    return user_id
