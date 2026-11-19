from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.rag_service import (
    ask_anime_concierge,
    detect_media_type_intent,
    get_rag_status,
    semantic_search_animes,
)

router = APIRouter()


class ChatMessage(BaseModel):
    role: str = Field(..., description="Papel da mensagem no histórico: 'user' ou 'model'")
    content: str = Field(..., min_length=1, max_length=3000)


class RagAskRequest(BaseModel):
    query: str = Field(..., min_length=3, max_length=1000, description="Pergunta ou descrição em linguagem natural")
    username: Optional[str] = Field(None, description="Username do MAL para personalizar e ocultar animes já assistidos")
    exclude_watched: bool = Field(True, description="Oculta animes que já estão na lista do usuário")
    min_score: Optional[float] = Field(None, ge=0.0, le=10.0, description="Nota mínima global no MyAnimeList")
    media_type: Optional[str] = Field(None, description="Filtrar por formato: tv, movie, ova, ona, special")
    limit: int = Field(8, ge=1, le=20, description="Quantidade de candidatos enviados ao Gemini e retornados em sources")
    history: List[ChatMessage] = Field(default_factory=list, description="Histórico curto de turnos anteriores (até 3 turnos)")


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
    media_type: Optional[str] = Query(None, description="Filtrar por formato: tv, movie, ova, ona, special"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Busca semântica de animes em linguagem natural usando Google Gemini Embeddings + Supabase pgvector,
    com rerank híbrido e filtro por formato.
    """
    try:
        clean_q = q.strip()
        explicit_media_type = media_type.strip().lower() if media_type and media_type.strip() else None
        auto_media_type = None if explicit_media_type else detect_media_type_intent(clean_q)
        effective_media_type = explicit_media_type or auto_media_type

        results = semantic_search_animes(
            db=db,
            query=clean_q,
            limit=limit,
            exclude_user=exclude_user,
            min_mal_score=min_score,
            media_type=effective_media_type,
            rerank=True,
        )
        if auto_media_type and not results:
            effective_media_type = None
            results = semantic_search_animes(
                db=db,
                query=clean_q,
                limit=limit,
                exclude_user=exclude_user,
                min_mal_score=min_score,
                media_type=None,
                rerank=True,
            )

        return {
            "query": clean_q,
            "exclude_user": exclude_user,
            "min_score": min_score,
            "media_type": effective_media_type,
            "total": len(results),
            "results": results,
        }
    except ValueError as ve:
        raise HTTPException(status_code=503, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na busca semântica: {str(e)}")


@router.post("/ask")
def rag_ask_concierge(
    payload: RagAskRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Endpoint completo do AniMind Concierge (RAG):
    combina perfil compacto do usuário + busca vetorial híbrida + resposta personalizada via Gemini 2.5 Flash.
    """
    if len(payload.query.strip()) < 3:
        raise HTTPException(status_code=422, detail="A pergunta deve ter pelo menos 3 caracteres válidos.")

    try:
        return ask_anime_concierge(
            db=db,
            query=payload.query,
            username=payload.username,
            exclude_watched=payload.exclude_watched,
            min_mal_score=payload.min_score,
            media_type=payload.media_type,
            limit=payload.limit,
            history=[{"role": m.role, "content": m.content} for m in payload.history],
        )
    except ValueError as ve:
        raise HTTPException(status_code=503, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o Concierge IA: {str(e)}")
