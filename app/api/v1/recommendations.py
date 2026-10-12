from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Dict, Any

from app.database import get_db
from app.models.models import GoldRecommendation, GoldUserAnalytics, User

router = APIRouter()

@router.get("/{username}")
def get_user_recommendations(username: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    Retorna o Top 20 de Recomendações (Camada Gold/Machine Learning) para um usuário específico,
    além dos seus insights analíticos básicos.
    """
    username_clean = username.strip().lower()
    
    # 1. Busca os insights analíticos do usuário (se existirem na Gold)
    user_stats = db.query(GoldUserAnalytics).filter(GoldUserAnalytics.username == username_clean).first()
    
    # 2. Busca o Top 20 de recomendações (ordenadas pelo rank gerado pela IA)
    recs = db.query(GoldRecommendation).filter(
        GoldRecommendation.username == username_clean
    ).order_by(GoldRecommendation.rank).all()
    
    if not recs:
        raise HTTPException(
            status_code=404, 
            detail=f"Recomendações não encontradas para '{username}'. Sincronize o perfil e aguarde o motor de IA rodar."
        )

    # Formatar os dados para o Front-End
    recommendations_list = []
    for r in recs:
        recommendations_list.append({
            "rank": r.rank,
            "anime_id": r.anime_id,
            "title": r.title,
            "main_picture": r.main_picture,
            "global_mal_mean": r.global_mal_mean,
            "predicted_score": round(r.predicted_score, 2) if r.predicted_score else None
        })

    response = {
        "user": username_clean,
        "analytics": {
            "total_animes": user_stats.total_animes if user_stats else 0,
            "total_days": user_stats.total_days_watched if user_stats else 0,
            "mean_score": user_stats.user_mean_score if user_stats else 0,
            "fav_genre": user_stats.favorite_genre if user_stats else "N/A",
            "fav_studio": user_stats.favorite_studio if user_stats else "N/A"
        },
        "recommendations": recommendations_list
    }
    
    return response
