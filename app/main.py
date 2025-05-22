from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from .auth import verify_token
from typing import List
from pydantic import BaseModel

app = FastAPI()

# ← add this block
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
      "http://localhost:5173",                         # your local Vite
      "https://react-dev.backtesting.theworkpc.com/"  # your Coolify URL
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)
# ↑ end CORS block

class Item(BaseModel):
    id: int
    name: str

DUMMY_ITEMS = [
    {"id": 1, "name": "First item", "owner": "cca3ecd8-66db-43ce-9525-c3a919b47be1"},
    {"id": 2, "name": "Second item", "owner": "user-uuid-2"},
]


@app.get("/")
async def health_check():
    return {"status": "ok"}

@app.get("/items", response_model=List[Item])
async def read_items(user_id: str = Depends(verify_token)):
    items = [i for i in DUMMY_ITEMS if i["owner"] == user_id]
    if not items:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No items or unauthorized",
        )
    return items
