from sqlalchemy import Column, Integer, String, Date, ForeignKey, Float, DateTime, Text, Boolean
from sqlalchemy.sql import func
from app.database import Base

class AppCredential(Base):
    """Armazena o token OAuth do sistema (login único do administrador)."""
    __tablename__ = "app_credentials"

    id = Column(Integer, primary_key=True, index=True)
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, index=True)
    gender = Column(String, nullable=True)
    joined_at = Column(Date, nullable=True)
    picture = Column(String, nullable=True)
    last_synced_at = Column(DateTime(timezone=True), nullable=True)



class Anime(Base):
    __tablename__ = "animes"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    main_picture = Column(String, nullable=True)
    mean = Column(Float, nullable=True)
    rank = Column(Integer, nullable=True)
    popularity = Column(Integer, nullable=True)
    num_episodes = Column(Integer, nullable=True)
    average_episode_duration = Column(Float, nullable=True)
    studio = Column(String, nullable=True)
    source = Column(String, nullable=True)
    media_type = Column(String, nullable=True)
    genres = Column(String, nullable=True)
    synopsis = Column(Text, nullable=True)
    num_scoring_users = Column(Integer, nullable=True)


class Genre(Base):
    __tablename__ = "genres"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False, index=True)


class AnimeGenre(Base):
    __tablename__ = "anime_genres"

    anime_id = Column(Integer, ForeignKey("animes.id"), primary_key=True)
    genre_id = Column(Integer, ForeignKey("genres.id"), primary_key=True)


class AnimeRelation(Base):
    """Grafo de relações entre animes (sequel, prequel, side_story, etc.)."""
    __tablename__ = "anime_relations"

    anime_id         = Column(Integer, ForeignKey("animes.id"), primary_key=True)
    related_anime_id = Column(Integer, ForeignKey("animes.id"), primary_key=True)
    relation_type    = Column(String, nullable=False)
    # Tipos possíveis: sequel | prequel | alternative_setting | alternative_version
    #                  side_story | parent_story | summary | full_story




class UserAnime(Base):
    __tablename__ = "user_animes"

    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    anime_id = Column(Integer, ForeignKey("animes.id"), primary_key=True)

    score = Column(Integer, nullable=True)
    status = Column(String, nullable=True)

    num_episodes_watched = Column(Integer, nullable=True)
    is_rewatching = Column(Boolean, default=False, nullable=True)
    num_times_rewatched = Column(Integer, default=0, nullable=True)
    start_date = Column(Date, nullable=True)
    finish_date = Column(Date, nullable=True)


class NewsRSS(Base):
    __tablename__ = "news_rss"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    title = Column(String, nullable=False)
    link = Column(String, unique=True, index=True, nullable=False)
    pub_date = Column(DateTime(timezone=True), nullable=True)
    description = Column(Text, nullable=True)
    image_url = Column(String, nullable=True)
    fetched_at = Column(DateTime(timezone=True), server_default=func.now())

class WatchEvent(Base):
    __tablename__ = "watch_events"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    anime_id = Column(Integer, ForeignKey("animes.id"), nullable=True)
    event_type = Column(String, nullable=False)  # EPISODE_WATCHED, STATUS_CHANGED, SCORE_UPDATED, COMPLETED
    description = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SystemState(Base):
    __tablename__ = "system_state"

    key = Column(String, primary_key=True, index=True)
    value = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

# =====================================================================
# CAMADA GOLD (DATA LAKEHOUSE) - Tabelas Analíticas e Machine Learning
# =====================================================================

class GoldRecommendation(Base):
    __tablename__ = "gold_recommendations"
    # O SQLAlchemy exige PK; usamos a dupla username + rank como identificador único
    username = Column(String, primary_key=True)
    rank = Column(Integer, primary_key=True)
    anime_id = Column(Integer, nullable=False)
    title = Column(String, nullable=True)
    main_picture = Column(String, nullable=True)
    global_mal_mean = Column(Float, nullable=True)
    predicted_score = Column(Float, nullable=True)

class GoldUserAnalytics(Base):
    __tablename__ = "gold_user_analytics"
    username = Column(String, primary_key=True)
    total_animes = Column(Integer, nullable=True)
    total_episodes_watched = Column(Integer, nullable=True)
    total_days_watched = Column(Float, nullable=True)
    user_mean_score = Column(Float, nullable=True)
    rewatched_count = Column(Integer, nullable=True)
    # fav_genre e fav_studio podem não existir em perfis muito novos
    favorite_genre = Column(String, nullable=True)
    favorite_studio = Column(String, nullable=True)

class GoldGenreInsight(Base):
    __tablename__ = "gold_genre_insights"
    genre_id = Column(Integer, primary_key=True)
    genre_name = Column(String, nullable=True)
    total_genre_views = Column(Integer, nullable=True)
    total_genre_completed = Column(Integer, nullable=True)
    total_genre_dropped = Column(Integer, nullable=True)
    total_episodes_watched = Column(Integer, nullable=True)
    calculated_at = Column(DateTime, nullable=True)

class GoldStudioInsight(Base):
    __tablename__ = "gold_studio_insights"
    studio_id = Column(Integer, primary_key=True)
    studio_name = Column(String, nullable=True)
    total_studio_views = Column(Integer, nullable=True)
    total_studio_completed = Column(Integer, nullable=True)
    total_studio_dropped = Column(Integer, nullable=True)
    total_episodes_watched = Column(Integer, nullable=True)
    calculated_at = Column(DateTime, nullable=True)
