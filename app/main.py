from fastapi import FastAPI, Depends, HTTPException, status
from .auth import verify_token
from typing import List
from pydantic import BaseModel

app = FastAPI()

class Item(BaseModel):
    id: int
    name: str

DUMMY_ITEMS = [
    {"id": 1, "name": "First item", "owner": "user-uuid-1"},
    {"id": 2, "name": "Second item", "owner": "user-uuid-2"},
]

@app.get("/items", response_model=List[Item])
async def read_items(user_id: str = Depends(verify_token)):
    items = [i for i in DUMMY_ITEMS if i["owner"] == user_id]
    if not items:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No items or unauthorized",
        )
    return items
