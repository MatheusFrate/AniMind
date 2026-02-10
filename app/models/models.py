from sqlalchemy import Column, Integer, String, Date, ForeignKey, Float
from app.database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True) # ID do MAL
    name = Column(String, index=True)
    gender = Column(String, nullable=True)
    joined_at = Column(Date, nullable=True)
    picture = Column(String, nullable=True)
    access_token = Column(String)
    refresh_token = Column(String)

class Anime(Base):
    __tablename__ = "animes"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String)
    mean = Column(Float)
    num_episodes = Column(Integer)
    average_episode_duration = Column(Float)
    studio = Column(String)
    source = Column(String)
    media_type = Column(String)

class UserAnime(Base):
    __tablename__ = "user_animes"
    
    # Definimos as chaves estrangeiras apontando para 'nome_da_tabela.coluna'
    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    anime_id = Column(Integer, ForeignKey("animes.id"), primary_key=True)
    
    score =Column(Integer, nullable=True)
    status = Column(String, nullable=True)

    num_episodes_watched = Column(Integer, nullable=True)
    start_date = Column(Date, nullable=True)
    finish_date = Column(Date, nullable=True)

