from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
class UserCreate(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    gender: Optional[str] = None
    joined_at: Optional[datetime] = None
    picture: Optional[str] = None

class UserResponse(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    gender: Optional[str] = None
    joined_at: Optional[datetime] = None
    picture: Optional[str] = None
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None


    class Config:
        from_attributes = True

