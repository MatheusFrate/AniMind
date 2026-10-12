from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

from app.database import get_db
from app.models.models import NewsRSS

router = APIRouter()


@router.get('/news')
def get_news(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Retorna as notícias e artigos de animes mais recentes coletados via RSS Feed do MAL.
    """
    query = db.query(NewsRSS).order_by(NewsRSS.pub_date.desc().nullslast())
    total_count = query.count()
    items = query.offset(offset).limit(limit).all()

    return {
        "total": total_count,
        "limit": limit,
        "offset": offset,
        "items": [
            {
                "id": news.id,
                "title": news.title,
                "link": news.link,
                "pub_date": news.pub_date,
                "description": news.description,
                "image_url": news.image_url,
                "fetched_at": news.fetched_at
            }
            for news in items
        ]
    }


class NewsItemSchema(BaseModel):
    title: str
    link: str
    pub_date: Optional[datetime] = None
    description: Optional[str] = None
    image_url: Optional[str] = None


@router.post('/batch')
@router.post('/news/batch')
def batch_insert_news(
    news_items: List[NewsItemSchema],
    db: Session = Depends(get_db)
):
    """
    Recebe uma lista de notícias via RSS e insere no banco de dados.
    Evita duplicação verificando a URL (link).
    """
    inserted = 0
    for item in news_items:
        exists = db.query(NewsRSS).filter(NewsRSS.link == item.link).first()
        if not exists:
            new_entry = NewsRSS(
                title=item.title,
                link=item.link,
                pub_date=item.pub_date,
                description=item.description,
                image_url=item.image_url
            )
            db.add(new_entry)
            inserted += 1
    db.commit()
    return {"status": "success", "inserted": inserted, "received": len(news_items)}
