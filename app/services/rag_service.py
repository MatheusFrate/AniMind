import hashlib
import json
import math
import os
import re
import time
from typing import Any, Dict, List, Optional
import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

EMBEDDING_DIM = 768
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
# Modelos suportados pela API do Google AI Studio (tenta text-embedding-004 e faz fallback para gemini-embedding-001)
EMBEDDING_MODELS = ["gemini-embedding-001", "text-embedding-004"]
# Ordem de fallback para geração priorizando maior RPD (permite override via GEMINI_CHAT_MODEL)
GENERATION_MODELS = [
    "gemini-3.5-flash-lite",
    "gemini-3.1-flash-lite",
    "gemini-flash-lite-latest",
    "gemini-3-flash-preview",
    "gemini-flash-latest",
]
VALID_MEDIA_TYPES = {"tv", "movie", "ova", "ona", "special", "music"}

_MEDIA_TYPE_PATTERNS = {
    "movie": re.compile(r"\b(filme|filmes|movie|movies|longa[\s-]metragem)\b", re.IGNORECASE),
    "ova": re.compile(r"\b(ova|ovas)\b", re.IGNORECASE),
    "ona": re.compile(r"\b(ona|onas)\b", re.IGNORECASE),
    "tv": re.compile(r"\b(s[eé]rie de tv|series de tv|formato tv)\b", re.IGNORECASE),
}


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


def detect_media_type_intent(query: str) -> Optional[str]:
    """Detecta intenção explícita de formato (movie, ova, ona, tv) na consulta em linguagem natural."""
    if not query:
        return None
    matched = [
        mtype
        for mtype, pattern in _MEDIA_TYPE_PATTERNS.items()
        if pattern.search(query)
    ]
    if len(matched) == 1:
        return matched[0]
    return None


def _compute_hybrid_score(item: Dict[str, Any], requested_media_type: Optional[str] = None) -> float:
    """Calcula score híbrido combinando similaridade vetorial, nota MAL, popularidade e formato."""
    sim_norm = max(0.0, min(1.0, (item.get("similarity_pct") or 0.0) / 100.0))

    mean = item.get("mean")
    mal_quality = max(0.0, min(1.0, (float(mean) - 5.0) / 4.5)) if mean is not None else 0.35

    num_scoring = item.get("num_scoring_users")
    popularity = item.get("popularity")
    if num_scoring and num_scoring > 0:
        pop_norm = min(1.0, math.log10(num_scoring + 1) / 6.0)
    elif popularity and popularity > 0:
        pop_norm = max(0.0, 1.0 - (min(float(popularity), 15000.0) / 15000.0))
    else:
        pop_norm = 0.3

    mtype = (item.get("media_type") or "tv").strip().lower()
    if requested_media_type and mtype == requested_media_type:
        format_score = 1.0
    elif mtype in ("tv", "movie", "ona"):
        format_score = 1.0
    elif mtype == "ova":
        format_score = 0.6
    else:
        format_score = 0.2

    hybrid = (0.72 * sim_norm + 0.18 * mal_quality + 0.06 * pop_norm + 0.04 * format_score) * 100.0
    return round(hybrid, 1)


def semantic_search_animes(
    db: Session,
    query: str,
    limit: int = 12,
    exclude_user: Optional[str] = None,
    min_mal_score: Optional[float] = None,
    media_type: Optional[str] = None,
    rerank: bool = True,
) -> List[Dict[str, Any]]:
    """
    Realiza busca vetorial por similaridade de cosseno no Supabase (pgvector)
    com rerank híbrido opcional (similaridade + qualidade MAL + popularidade + formato).
    """
    query_vec = embed_query(query)
    vec_literal = "[" + ",".join(f"{v:.6f}" for v in query_vec) + "]"

    normalized_media_type = (
        media_type.strip().lower()
        if media_type and media_type.strip().lower() in VALID_MEDIA_TYPES
        else None
    )
    # Busca janela maior de candidatos quando o rerank híbrido está ativo
    candidate_limit = min(max(limit * 4, 36), 80) if rerank else limit

    params: Dict[str, Any] = {
        "query_vec": vec_literal,
        "limit": candidate_limit,
    }

    sql_query = """
        SELECT
            a.id,
            a.title,
            a.main_picture,
            a.mean,
            a.rank,
            a.popularity,
            a.num_scoring_users,
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

    if normalized_media_type:
        sql_query += " AND LOWER(a.media_type) = :media_type"
        params["media_type"] = normalized_media_type

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
        item = {
            "anime_id": r["id"],
            "title": r["title"],
            "main_picture": r["main_picture"],
            "mean": r["mean"],
            "rank": r["rank"],
            "popularity": r["popularity"],
            "num_scoring_users": r["num_scoring_users"],
            "num_episodes": r["num_episodes"],
            "studio": r["studio"],
            "genres": r["genres"],
            "media_type": r["media_type"],
            "synopsis": r["synopsis"],
            "similarity_pct": float(r["similarity_pct"]) if r["similarity_pct"] is not None else 0.0,
        }
        item["hybrid_score"] = _compute_hybrid_score(item, requested_media_type=normalized_media_type)
        results.append(item)

    if rerank:
        results.sort(key=lambda it: (it["hybrid_score"], it["similarity_pct"]), reverse=True)

    return results[:limit]


def _aggregate_top_attribute(
    rows: List[Any],
    field_name: str,
    label_key: str,
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    """Agrega gêneros ou estúdios ponderando nota média e frequência do usuário."""
    stats: Dict[str, Dict[str, float]] = {}
    for r in rows:
        raw = r[field_name]
        if not raw:
            continue
        score = float(r["score"]) if r["score"] and r["score"] > 0 else 0.0
        seen_in_row = set()
        for part in str(raw).split(","):
            name = part.strip()
            if not name or name.lower() == "unknown" or name in seen_in_row:
                continue
            seen_in_row.add(name)
            entry = stats.setdefault(name, {"count": 0, "score_sum": 0.0, "scored_count": 0})
            entry["count"] += 1
            if score > 0:
                entry["score_sum"] += score
                entry["scored_count"] += 1

    ranked = []
    for name, entry in stats.items():
        count = int(entry["count"])
        scored_count = int(entry["scored_count"])
        avg_score = (entry["score_sum"] / scored_count) if scored_count > 0 else 0.0
        ranked.append({
            label_key: name,
            "avg_score": round(avg_score, 2),
            "count": count,
        })

    ranked.sort(
        key=lambda x: (
            x["count"] >= 2,
            x["avg_score"] + min(x["count"], 10) * 0.08,
            x["count"],
        ),
        reverse=True,
    )
    return ranked[:top_k]


def get_user_taste_profile(db: Session, username: Optional[str]) -> Optional[Dict[str, Any]]:
    """Extrai um resumo compacto do gosto do usuário (top gêneros, estúdios e amostra de favoritos)."""
    if not username or not username.strip():
        return None

    uname = username.strip().lower()
    user_row = db.execute(
        text("SELECT id, name FROM users WHERE LOWER(name) = :uname LIMIT 1"),
        {"uname": uname},
    ).mappings().first()
    if not user_row:
        return None

    rows = db.execute(
        text("""
            SELECT
                a.id, a.title, a.genres, a.studio, a.media_type, a.mean,
                ua.score, ua.status, ua.finish_date
            FROM user_animes ua
            JOIN animes a ON a.id = ua.anime_id
            WHERE ua.user_id = :user_id
              AND ua.status IN ('completed', 'watching', 'on_hold', 'dropped')
        """),
        {"user_id": user_row["id"]},
    ).mappings().all()

    if not rows:
        return None

    total_watched = sum(1 for r in rows if r["status"] in ("completed", "watching"))
    scored_rows = [r for r in rows if r["score"] is not None and r["score"] > 0]
    user_mean_score = (
        round(sum(r["score"] for r in scored_rows) / len(scored_rows), 2)
        if scored_rows
        else None
    )

    agg_rows = (
        scored_rows
        if scored_rows
        else ([r for r in rows if r["status"] in ("completed", "watching")] or list(rows))
    )
    top_genres = _aggregate_top_attribute(agg_rows, "genres", "genre", top_k=5)
    top_studios = _aggregate_top_attribute(agg_rows, "studio", "studio", top_k=5)

    high_rated = [r for r in scored_rows if r["score"] >= 8]
    sample_pool = high_rated if len(high_rated) >= 5 else scored_rows
    sample_sorted = sorted(
        sample_pool,
        key=lambda r: (
            r["score"] or 0,
            r["finish_date"] is not None,
            str(r["finish_date"] or ""),
            float(r["mean"]) if r["mean"] is not None else 0.0,
        ),
        reverse=True,
    )[:10]

    favorite_animes = [
        {
            "anime_id": r["id"],
            "title": r["title"],
            "user_score": r["score"],
            "genres": r["genres"],
            "studio": r["studio"],
            "media_type": r["media_type"],
        }
        for r in sample_sorted
    ]

    return {
        "username": user_row["name"],
        "total_watched": total_watched,
        "user_mean_score": user_mean_score,
        "top_genres": top_genres,
        "top_studios": top_studios,
        "favorite_animes": favorite_animes,
    }


def format_user_profile_for_prompt(profile: Optional[Dict[str, Any]]) -> str:
    """Formata o perfil de gosto do usuário em texto compacto para injeção no prompt."""
    if not profile:
        return ""

    mean_str = f"{profile['user_mean_score']:.2f}/10" if profile.get("user_mean_score") is not None else "N/A"
    lines = [
        f"Usuário: {profile['username']} | Obras vistas/assistindo: {profile.get('total_watched', 0)} | Nota média: {mean_str}"
    ]

    top_genres = profile.get("top_genres") or []
    if top_genres:
        genres_txt = ", ".join(
            f"{g['genre']} (média {g['avg_score']}, {g['count']} obras)"
            if g["avg_score"] > 0
            else f"{g['genre']} ({g['count']} obras)"
            for g in top_genres
        )
        lines.append(f"Top Gêneros: {genres_txt}")

    top_studios = profile.get("top_studios") or []
    if top_studios:
        studios_txt = ", ".join(
            f"{s['studio']} (média {s['avg_score']}, {s['count']} obras)"
            if s["avg_score"] > 0
            else f"{s['studio']} ({s['count']} obras)"
            for s in top_studios
        )
        lines.append(f"Top Estúdios: {studios_txt}")

    favs = profile.get("favorite_animes") or []
    if favs:
        lines.append("Amostra de favoritos com nota alta:")
        for fav in favs:
            genres_list = [g.strip() for g in (fav.get("genres") or "").split(",") if g.strip()][:3]
            genres_short = ", ".join(genres_list) if genres_list else "N/A"
            mtype = (fav.get("media_type") or "tv").upper()
            studio = fav.get("studio") or "N/A"
            lines.append(
                f"- {fav['title']} (nota {fav['user_score']}/10 | {mtype} | {studio} | {genres_short})"
            )

    return "\n".join(lines)


def _format_candidates_for_prompt(candidates: List[Dict[str, Any]]) -> str:
    """Formata os candidatos recuperados do pgvector de forma estruturada e enxuta."""
    lines = []
    for idx, c in enumerate(candidates, start=1):
        mtype = (c.get("media_type") or "TV").upper()
        eps = c.get("num_episodes") if c.get("num_episodes") is not None else "?"
        studio = c.get("studio") or "N/A"
        mean = f"{c['mean']:.2f}" if c.get("mean") is not None else "N/A"
        genres = c.get("genres") or "N/A"
        synopsis = (c.get("synopsis") or "Sem sinopse.").replace("\n", " ").strip()[:450]
        lines.append(
            f"#{idx} [ID: {c['anime_id']}] {c['title']} | Formato: {mtype} ({eps} eps) | "
            f"Estúdio: {studio} | Nota MAL: {mean} | Match Semântico: {c['similarity_pct']}% "
            f"(Score Híbrido: {c['hybrid_score']}) | Gêneros: {genres} | Sinopse: {synopsis}"
        )
    return "\n".join(lines)


def _sanitize_chat_history(history: Optional[List[Dict[str, str]]]) -> List[Dict[str, Any]]:
    """Normaliza até 3 turnos anteriores (6 mensagens) para o formato contents da API REST do Gemini."""
    if not history:
        return []

    sanitized: List[Dict[str, Any]] = []
    for msg in history[-6:]:
        raw_role = (msg.get("role") or "").strip().lower()
        content = (msg.get("content") or "").strip()
        if not content:
            continue
        if raw_role == "user":
            role = "user"
        elif raw_role in ("model", "assistant"):
            role = "model"
        else:
            continue
        sanitized.append({"role": role, "parts": [{"text": content[:1500]}]})

    while sanitized and sanitized[0]["role"] != "user":
        sanitized.pop(0)

    return sanitized


CONCIERGE_RESPONSE_SCHEMA: Dict[str, Any] = {
    "type": "OBJECT",
    "properties": {
        "summary": {
            "type": "STRING",
            "description": (
                "Resposta direta de no máximo 1 frase (opcional). "
                "NÃO use saudações como 'Olá', 'Analisando seu perfil' ou introduções genéricas. "
                "Na recomendação inicial, deixe vazio ''."
            ),
        },
        "recommendations": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "anime_id": {"type": "INTEGER"},
                    "reason": {
                        "type": "STRING",
                        "description": (
                            "Explicação direta e personalizada em 2 a 3 frases sobre por que este anime "
                            "combina com o pedido e com o perfil do usuário (sem repetir ficha técnica)."
                        ),
                    },
                },
                "required": ["anime_id", "reason"],
            },
        },
    },
    "required": ["summary", "recommendations"],
}


def _parse_concierge_response(
    raw_text: str,
    candidates: List[Dict[str, Any]],
) -> tuple[str, List[Dict[str, Any]], str]:
    """Faz parse do JSON estruturado do Gemini e enriquece cada recomendação com os metadados do card."""
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    candidates_by_id = {c["anime_id"]: c for c in candidates}
    summary = ""
    enriched_recs: List[Dict[str, Any]] = []

    try:
        parsed = json.loads(cleaned)
        summary = (parsed.get("summary") or "").strip()
        seen_ids = set()
        for rec in parsed.get("recommendations") or []:
            aid = rec.get("anime_id")
            reason = (rec.get("reason") or "").strip()
            if aid in candidates_by_id and aid not in seen_ids and reason:
                seen_ids.add(aid)
                card = dict(candidates_by_id[aid])
                card["reason"] = reason
                enriched_recs.append(card)
    except Exception:
        # Fallback seguro caso o modelo devolva texto livre
        return "", [], raw_text.strip()

    md_parts: List[str] = []
    if summary:
        md_parts.append(summary)
    for rec in enriched_recs:
        md_parts.append(f"- **{rec['title']}**: {rec['reason']}")
    answer_markdown = "\n".join(md_parts).strip() or raw_text.strip()
    return summary, enriched_recs, answer_markdown


def _call_gemini_generate(
    api_key: str,
    system_instruction: str,
    contents: List[Dict[str, Any]],
) -> tuple[str, str]:
    """Executa chamada REST leve (requests.post) para geração estruturada com Gemini."""
    payload = {
        "system_instruction": {"parts": [{"text": system_instruction}]},
        "contents": contents,
        "generationConfig": {
            "temperature": 0.4,
            "maxOutputTokens": 2048,
            "responseMimeType": "application/json",
            "responseSchema": CONCIERGE_RESPONSE_SCHEMA,
        },
    }

    preferred_model = (os.getenv("GEMINI_CHAT_MODEL") or "").strip()
    models_to_try = [preferred_model] + [m for m in GENERATION_MODELS if m != preferred_model] if preferred_model else GENERATION_MODELS

    last_err = None
    for model_name in models_to_try:
        url = f"{GEMINI_API_BASE}/models/{model_name}:generateContent?key={api_key}"
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                candidates = data.get("candidates") or []
                if candidates:
                    parts = (candidates[0].get("content") or {}).get("parts") or []
                    text_chunks = [
                        p.get("text", "")
                        for p in parts
                        if p.get("text") and not p.get("thought")
                    ]
                    answer = "\n".join(text_chunks).strip()
                    if answer:
                        return answer, model_name
                last_err = f"Resposta vazia do modelo {model_name}"
            elif resp.status_code in (404, 429, 503):
                last_err = f"{model_name} HTTP {resp.status_code}: {resp.text[:250]}"
                continue
            else:
                last_err = f"{model_name} HTTP {resp.status_code}: {resp.text[:300]}"
        except Exception as e:
            last_err = f"{model_name}: {str(e)}"

    raise RuntimeError(f"Falha ao gerar resposta no Gemini: {last_err}")


def ask_anime_concierge(
    db: Session,
    query: str,
    username: Optional[str] = None,
    exclude_watched: bool = True,
    min_mal_score: Optional[float] = None,
    media_type: Optional[str] = None,
    limit: int = 8,
    history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Pipeline completo de RAG (AniMind Concierge):
    1. Recupera candidatos via busca vetorial + rerank híbrido;
    2. Extrai perfil compacto do usuário (caso username informado);
    3. Gera recomendação personalizada e grounded via Gemini 2.5 Flash.
    """
    api_key = get_gemini_api_key()
    if not api_key:
        raise ValueError("Variável GEMINI_API_KEY não configurada no ambiente.")

    clean_query = query.strip()
    prev_user_msgs = [
        (h.get("content") or "").strip()
        for h in (history or [])
        if (h.get("role") or "").strip().lower() == "user" and (h.get("content") or "").strip()
    ]
    search_query = f"{' | '.join(prev_user_msgs[-2:])} | {clean_query}" if prev_user_msgs else clean_query

    explicit_media_type = (
        media_type.strip().lower()
        if media_type and media_type.strip().lower() in VALID_MEDIA_TYPES
        else None
    )
    auto_media_type = None if explicit_media_type else detect_media_type_intent(search_query)
    effective_media_type = explicit_media_type or auto_media_type

    clean_username = username.strip().lower() if username and username.strip() else None
    exclude_user = clean_username if exclude_watched else None

    candidates = semantic_search_animes(
        db=db,
        query=search_query,
        limit=limit,
        exclude_user=exclude_user,
        min_mal_score=min_mal_score,
        media_type=effective_media_type,
        rerank=True,
    )
    if auto_media_type and not candidates:
        effective_media_type = None
        candidates = semantic_search_animes(
            db=db,
            query=search_query,
            limit=limit,
            exclude_user=exclude_user,
            min_mal_score=min_mal_score,
            media_type=None,
            rerank=True,
        )

    user_profile = get_user_taste_profile(db, clean_username) if clean_username else None
    profile_context_str = format_user_profile_for_prompt(user_profile)

    if not candidates:
        empty_msg = (
            "Não encontrei candidatos no índice vetorial que atendam aos filtros atuais "
            "(como nota mínima, formato ou exclusão de animes já assistidos). "
            "Tente flexibilizar os filtros ou reformular sua busca."
        )
        return {
            "query": clean_query,
            "username": clean_username,
            "exclude_watched": exclude_watched,
            "min_score": min_mal_score,
            "media_type": effective_media_type,
            "model": GENERATION_MODELS[0],
            "user_profile": user_profile,
            "summary": empty_msg,
            "recommendations": [],
            "answer": empty_msg,
            "total": 0,
            "sources": [],
        }

    candidates_block = _format_candidates_for_prompt(candidates)

    system_instruction = (
        "Você é o AniMind Concierge, um curador especialista em animes que responde em Português (pt-BR) "
        "em JSON estruturado.\n\n"
        "REGRAS ESTRITAS:\n"
        "1. SEM ENROLAÇÃO: NUNCA escreva saudações ('Olá', 'Oi'), frases introdutórias ('Analisando seu perfil...', "
        "'Separei X recomendações...') ou despedidas. Deixe 'summary' vazio '' na recomendação inicial; "
        "use 'summary' (no máximo 1 frase direta) apenas em perguntas de acompanhamento quando precisar responder "
        "algo objetivo (ex: qual é o mais curto).\n"
        "2. ANTI-ALUCINAÇÃO (GROUNDING): Escolha EXCLUSIVAMENTE `anime_id` presentes na lista "
        "[CANDIDATOS RECUPERADOS DO PGVECTOR]. Nunca invente títulos ou altere metadados.\n"
        "3. PERSONALIZAÇÃO DIRETA: Em cada item de `recommendations` (selecione os 3 a 5 melhores candidatos, "
        "ou os relevantes no acompanhamento), escreva `reason` em 2 a 3 frases objetivas explicando por que "
        "o usuário vai curtir, conectando ao pedido e às obras/estúdios de [PERFIL DE GOSTO DO USUÁRIO]. "
        "Não repita ficha técnica (formato/episódios/nota MAL), pois o card visual já exibe esses dados."
    )

    user_sections = []
    if profile_context_str:
        user_sections.append(f"[PERFIL DE GOSTO DO USUÁRIO]\n{profile_context_str}")
    user_sections.append(f"[CANDIDATOS RECUPERADOS DO PGVECTOR]\n{candidates_block}")
    user_sections.append(f"[PEDIDO ATUAL DO USUÁRIO]\n{clean_query}")

    contents = _sanitize_chat_history(history)
    contents.append({"role": "user", "parts": [{"text": "\n\n".join(user_sections)}]})

    raw_answer, used_model = _call_gemini_generate(
        api_key=api_key,
        system_instruction=system_instruction,
        contents=contents,
    )
    summary_text, enriched_recs, answer_markdown = _parse_concierge_response(raw_answer, candidates)

    return {
        "query": clean_query,
        "username": clean_username,
        "exclude_watched": exclude_watched,
        "min_score": min_mal_score,
        "media_type": effective_media_type,
        "model": used_model,
        "user_profile": user_profile,
        "summary": summary_text,
        "recommendations": enriched_recs,
        "answer": answer_markdown,
        "total": len(candidates),
        "sources": candidates,
    }


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
