from fastapi import APIRouter
from app.schemas.user import UserCreate, UserResponse
from app.services.user_service import create_user_logic

router = APIRouter()

@router.post('/users', response_model=UserResponse)
def create_user(user: UserCreate):
    return create_user_logic(user)