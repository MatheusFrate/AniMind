from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class Anime(BaseModel):
    score:Optional[int] = None
    status: Optional[str] = None
    title: Optional[str] = None

    num_episodes_watched: Optional[int] = None
    start_date: Optional[datetime] = None
    finish_date: Optional[datetime] = None

    mean: Optional[float] = None
    media_type: Optional[str] = None
    num_episodes: Optional[int] = None
    average_episode_duration: Optional[float] = None
    studio: Optional[str] = None
    source: Optional[str] = None