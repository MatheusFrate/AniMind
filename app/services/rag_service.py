import hashlib
import os
import time
from typing import Any, Dict, List, Optional
import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

EMBEDDING_DIM = 768
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
# Modelos suportados pela API do Google AI Studio (tenta text-embedding-004 e faz fallback para gemini-embedding-001)
EMBEDDING_MODELS = ["gemini-embedding-001", "text-embedding-004"]


def get_gemini_api_key() -> Optional[str]:
    return (
        os.getenv("GEMINI_API_KEY")
        or os.getenv("GOOGLE_API_KEY")
        or None
    )


def ensure_vector_schema(db_or_conn) -> None:
    """Garante que a extensão pgvector, a tabela anime_embeddings e o índice HNSW existam."""
    db_or_conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
    db_or_conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS anime_embeddings (
            anime_id INTEGER PRIMARY KEY REFERENCES animes(id) ON DELETE CASCADE,
            content_hash VARCHAR(64),
            embedding vector({EMBEDDING_DIM}),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """))
    db_or_conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_anime_embeddings_hnsw
        ON anime_embeddings USING hnsw (embedding vector_cosine_ops);
    """))
    db_or_conn.execute(text("ALTER TABLE anime_embeddings ENABLE ROW LEVEL SECURITY;"))
    db_or_conn.commit()


def build_anime_document(anime: Dict[str, Any]) -> str:
    """Monta o documento semântico enriquecido de um anime para geração de embedding."""
    title = (anime.get("title") or "").strip()
    genres = (anime.get("genres") or "Unknown").strip()
    studio = (anime.get("studio") or "Unknown").strip()
    media_type = (anime.get("media_type") or "TV").strip()
    source = (anime.get("source") or "Unknown").strip()
    synopsis = (anime.get("synopsis") or "").strip()

    return (
        f"Title: {title}\n"
        f"Genres & Themes: {genres}\n"
        f"Studio: {studio} | Format: {media_type} | Source: {source}\n"
        f"Synopsis: {synopsis}"
    )


def compute_content_hash(doc_text: str) -> str:
    return hashlib.sha256(doc_text.encode("utf-8")).hexdigest()


def _normalize_l2(vec: List[float]) -> List[float]:
    """Normaliza o vetor (L2 norm = 1.0), recomendado para Matryoshka embeddings de 768 dimensões."""
    norm = sum(x * x for x in vec) ** 0.5
    if norm > 0:
        return [x / norm for x in vec]
    return vec


def embed_query(query_text: str) -> List[float]:
    """Gera o vetor de 768 dimensões para a busca em linguagem natural do usuário via Gemini API."""
    api_key = get_gemini_api_key()
    if not api_key:
        raise ValueError("Variável GEMINI_API_KEY não configurada no ambiente.")

    last_err = None
    for model_name in EMBEDDING_MODELS:
        url = f"{GEMINI_API_BASE}/models/{model_name}:embedContent?key={api_key}"
        payload = {
            "model": f"models/{model_name}",
            "content": {"parts": [{"text": query_text}]},
            "taskType": "RETRIEVAL_QUERY",
            "outputDimensionality": EMBEDDING_DIM,
        }
        try:
            resp = requests.post(url, json=payload, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                values = data.get("embedding", {}).get("values")
                if values and len(values) == EMBEDDING_DIM:
                    return _normalize_l2(values)
            elif resp.status_code == 404:
                continue
            else:
                last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
        except Exception as e:
            last_err = str(e)

    raise RuntimeError(f"Falha ao gerar embedding na API do Gemini: {last_err}")


def embed_documents_batch(items: List[Dict[str, str]], max_retries: int = 4) -> List[List[float]]:
    """
    Gera embeddings em lote (até 100 documentos por chamada HTTP) usando batchEmbedContents.
    Cada item deve conter {'title': ..., 'text': ...}.
    """
    api_key = get_gemini_api_key()
    if not api_key:
        raise ValueError("Variável GEMINI_API_KEY não configurada no ambiente.")

    last_err = None
    for model_name in EMBEDDING_MODELS:
        url = f"{GEMINI_API_BASE}/models/{model_name}:batchEmbedContents?key={api_key}"
        requests_payload = [
            {
                "model": f"models/{model_name}",
                "content": {"parts": [{"text": it["text"]}]},
                "taskType": "RETRIEVAL_DOCUMENT",
                "title": (it.get("title") or "")[:120],
                "outputDimensionality": EMBEDDING_DIM,
            }
            for it in items
        ]
        payload = {"requests": requests_payload}

        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.post(url, json=payload, timeout=45)
                if resp.status_code == 200:
                    data = resp.json()
                    embeddings = [_normalize_l2(e.get("values", [])) for e in data.get("embeddings", [])]
                    if len(embeddings) == len(items):
                        return embeddings
                elif resp.status_code == 404:
                    break  # Tenta próximo modelo da lista
                elif resp.status_code == 429:
                    wait_sec = 12 * attempt
                    print(f"[RAG BATCH] Rate limit (429) atingido. Aguardando {wait_sec}s (tentativa {attempt}/{max_retries})...")
                    time.sleep(wait_sec)
                    continue
                else:
                    last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
                    time.sleep(3)
            except Exception as e:
                last_err = str(e)
                time.sleep(3)

    raise RuntimeError(f"Falha ao gerar lote de embeddings no Gemini: {last_err}")


def semantic_search_animes(
    db: Session,
    query: str,
    limit: int = 12,
    exclude_user: Optional[str] = None,
    min_mal_score: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """
    Realiza busca vetorial por similaridade de cosseno no Supabase (pgvector).
    Opcionalmente oculta animes que já estão na lista do usuário (exclude_user).
    """
    query_vec = embed_query(query)
    vec_literal = "[" + ",".join(f"{v:.6f}" for v in query_vec) + "]"

    params: Dict[str, Any] = {
        "query_vec": vec_literal,
        "limit": limit,
    }

    sql_query = """
        SELECT
            a.id,
            a.title,
            a.main_picture,
            a.mean,
            a.rank,
            a.popularity,
            a.num_episodes,
            a.studio,
            a.genres,
            a.media_type,
            a.synopsis,
            ROUND(((1 - (ae.embedding <=> CAST(:query_vec AS vector))) * 100)::numeric, 1) AS similarity_pct
        FROM anime_embeddings ae
        JOIN animes a ON a.id = ae.anime_id
        WHERE 1=1
    """

    if min_mal_score is not None and min_mal_score > 0:
        sql_query += " AND a.mean >= :min_mal_score"
        params["min_mal_score"] = min_mal_score

    if exclude_user:
        params["exclude_user"] = exclude_user.strip().lower()
        sql_query += """
            AND NOT EXISTS (
                SELECT 1
                FROM user_animes ua
                JOIN users u ON u.id = ua.user_id
                WHERE LOWER(u.name) = :exclude_user
                  AND ua.anime_id = a.id
                  AND ua.status IN ('completed', 'watching', 'on_hold', 'dropped')
            )
        """

    sql_query += """
        ORDER BY ae.embedding <=> CAST(:query_vec AS vector) ASC
        LIMIT :limit
    """

    sql = text(sql_query)

    rows = db.execute(sql, params).mappings().all()
    results = []
    for r in rows:
        results.append({
            "anime_id": r["id"],
            "title": r["title"],
            "main_picture": r["main_picture"],
            "mean": r["mean"],
            "rank": r["rank"],
            "popularity": r["popularity"],
            "num_episodes": r["num_episodes"],
            "studio": r["studio"],
            "genres": r["genres"],
            "media_type": r["media_type"],
            "synopsis": r["synopsis"],
            "similarity_pct": float(r["similarity_pct"]) if r["similarity_pct"] is not None else 0.0,
        })
    return results


def get_rag_status(db: Session) -> Dict[str, Any]:
    """Retorna estatísticas da base vetorial no Supabase."""
    try:
        indexed_count = db.execute(text("SELECT count(*) FROM anime_embeddings")).scalar() or 0
        eligible_count = db.execute(text("""
            SELECT count(*) FROM animes
            WHERE synopsis IS NOT NULL
              AND length(trim(synopsis)) > 40
              AND synopsis NOT LIKE 'No synopsis%'
              AND mean IS NOT NULL
        """)).scalar() or 0
    except Exception:
        indexed_count = 0
        eligible_count = 0

    return {
        "indexed_animes": indexed_count,
        "eligible_animes": eligible_count,
        "coverage_pct": round((indexed_count / eligible_count) * 100, 1) if eligible_count > 0 else 0.0,
        "gemini_key_configured": bool(get_gemini_api_key()),
        "embedding_dim": EMBEDDING_DIM,
    }
