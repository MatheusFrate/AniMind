from typing import Any, Dict, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.rag_service import get_rag_status, semantic_search_animes

router = APIRouter()


@router.get("/status")
def rag_status(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Retorna o status da base vetorial (pgvector) e cobertura de embeddings."""
    return get_rag_status(db)


@router.get("/search")
def rag_semantic_search(
    q: str = Query(..., min_length=3, description="Descrição em linguagem natural da obra ou atmosfera desejada"),
    limit: int = Query(12, ge=1, le=40, description="Quantidade máxima de animes retornados"),
    exclude_user: Optional[str] = Query(None, description="Username do MAL para ocultar animes já assistidos"),
    min_score: Optional[float] = Query(None, ge=0.0, le=10.0, description="Nota mínima global no MyAnimeList"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Busca semântica de animes em linguagem natural usando Google Gemini Embeddings + Supabase pgvector.
    """
    try:
        results = semantic_search_animes(
            db=db,
            query=q.strip(),
            limit=limit,
            exclude_user=exclude_user,
            min_mal_score=min_score,
        )
        return {
            "query": q.strip(),
            "exclude_user": exclude_user,
            "min_score": min_score,
            "total": len(results),
            "results": results,
        }
    except ValueError as ve:
        raise HTTPException(status_code=503, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na busca semântica: {str(e)}")
