import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()

raw_db_url = os.getenv("DATABASE_URL") or os.getenv("SUPA_CONNECTION_STRING") or "sqlite:///./animind.db"
SQLALCHEMY_DATABASE_URL = raw_db_url.strip()
if SQLALCHEMY_DATABASE_URL.startswith("postgres://"):
    SQLALCHEMY_DATABASE_URL = SQLALCHEMY_DATABASE_URL.replace("postgres://", "postgresql://", 1)

connect_args = {}
if SQLALCHEMY_DATABASE_URL.startswith("sqlite"):
    connect_args["check_same_thread"] = False

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Dependência para as rotas do FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()