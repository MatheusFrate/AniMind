from fastapi import FastAPI
from app.api.v1 import users, auth, anime_list
from starlette.middleware.sessions import SessionMiddleware
import os
from dotenv import load_dotenv
from app.database import Base, engine
from app.models import models

load_dotenv()
Base.metadata.create_all(bind=engine)
app = FastAPI(title='Animind')

# app.include_router(users.router)
app.include_router(auth.router)
app.include_router(anime_list.router)
SECRET_KEY = os.getenv("SESSION_SECRET_KEY", 'None')
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

@app.get('/')
def home():
    return {'message': "API online"}

# uvicorn app.main:app --reload.