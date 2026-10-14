from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel

from app.database import get_db
from app.models.models import User, UserAnime, Anime

router = APIRouter()

@router.get('/user/active')
def get_active_users( db: Session = Depends(get_db)):
    """
    Retorna uma lista de usuários ativos no banco de dados.
    """
    active_users = db.query(User).all()
    return [{"id": user.id, "name": user.name, "last_synced_at": user.last_synced_at} for user in active_users]

@router.get('/user/{username}/stats')
@router.get('/user/{username}')
def get_user_stats(username: str, db: Session = Depends(get_db)):
    """
    Retorna dados do perfil do usuário e métricas estatísticas agregadas a partir do banco de dados.
    """
    username = username.strip().lower()
    user = db.query(User).filter(User.name == username).first()

    if not user:
        raise HTTPException(status_code=404, detail=f"Usuário '{username}' não encontrado no banco de dados. Realize a sincronização primeiro.")

    # Consulta relação de animes do usuário
    user_animes = db.query(UserAnime, Anime).join(Anime, UserAnime.anime_id == Anime.id).filter(UserAnime.user_id == user.id).all()

    total_animes = len(user_animes)
    watching_count = 0
    completed_count = 0
    on_hold_count = 0
    dropped_count = 0
    plan_to_watch_count = 0
    
    total_episodes_watched = 0
    total_seconds_watched = 0
    rewatched_count = 0
    scored_animes_count = 0
    sum_scores = 0

    status_distribution = {}
    studios_count = {}

    for ua, anime in user_animes:
        status = ua.status or 'unknown'
        status_distribution[status] = status_distribution.get(status, 0) + 1

        if status == 'watching':
            watching_count += 1
        elif status == 'completed':
            completed_count += 1
        elif status == 'on_hold':
            on_hold_count += 1
        elif status == 'dropped':
            dropped_count += 1
        elif status == 'plan_to_watch':
            plan_to_watch_count += 1

        if ua.is_rewatching or (ua.num_times_rewatched and ua.num_times_rewatched > 0):
            rewatched_count += 1

        episodes = ua.num_episodes_watched or 0
        total_episodes_watched += episodes

        if anime.average_episode_duration:
            total_seconds_watched += (episodes * anime.average_episode_duration)

        if ua.score and ua.score > 0:
            scored_animes_count += 1
            sum_scores += ua.score

        if anime.studio:
            studios_count[anime.studio] = studios_count.get(anime.studio, 0) + 1

    user_mean_score = round(sum_scores / scored_animes_count, 2) if scored_animes_count > 0 else 0
    total_days_watched = round(total_seconds_watched / 86400, 1)

    # Top 5 estúdios
    top_studios = sorted(studios_count.items(), key=lambda item: item[1], reverse=True)[:5]

    return {
        "user": {
            "id": user.id,
            "name": user.name,
            "gender": user.gender,
            "joined_at": user.joined_at,
            "picture": user.picture
        },
        "stats": {
            "total_animes": total_animes,
            "watching_count": watching_count,
            "completed_count": completed_count,
            "on_hold_count": on_hold_count,
            "dropped_count": dropped_count,
            "plan_to_watch_count": plan_to_watch_count,
            "rewatched_count": rewatched_count,
            "total_episodes_watched": total_episodes_watched,
            "total_days_watched": total_days_watched,
            "user_mean_score": user_mean_score,
            "scored_animes_count": scored_animes_count,
            "status_distribution": status_distribution,
            "top_studios": [{"studio": s[0], "count": s[1]} for s in top_studios]
        }
    }

@router.get('/user/{username}/animelist')
def get_user_animelist(
    username: str,
    status: Optional[str] = Query(None, description="Filtrar por status: watching, completed, on_hold, dropped, plan_to_watch"),
    search: Optional[str] = Query(None, description="Filtrar por título do anime"),
    limit: int = Query(5000, ge=1),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):

    """
    Retorna a lista de animes de um usuário armazenada no banco com suporte a busca e paginação.
    """
    username = username.strip().lower()
    user = db.query(User).filter(User.name == username).first()

    if not user:
        raise HTTPException(status_code=404, detail=f"Usuário '{username}' não encontrado no banco de dados.")

    query = db.query(UserAnime, Anime).join(Anime, UserAnime.anime_id == Anime.id).filter(UserAnime.user_id == user.id)

    if status:
        query = query.filter(UserAnime.status == status.lower())

    if search:
        query = query.filter(Anime.title.ilike(f"%{search}%"))

    total_count = query.count()
    results = query.offset(offset).limit(limit).all()

    animelist = []
    for ua, anime in results:
        animelist.append({
            "id": anime.id,
            "title": anime.title,
            "main_picture": anime.main_picture,
            "media_type": anime.media_type,
            "num_episodes": anime.num_episodes,
            "global_mean": anime.mean,
            "rank": anime.rank,
            "popularity": anime.popularity,
            "genres": anime.genres,
            "synopsis": anime.synopsis,
            "num_scoring_users": anime.num_scoring_users,
            "studio": anime.studio,
            "user_status": ua.status,
            "user_score": ua.score,
            "num_episodes_watched": ua.num_episodes_watched,
            "is_rewatching": bool(ua.is_rewatching),
            "num_times_rewatched": ua.num_times_rewatched or 0,
            "start_date": ua.start_date,
            "finish_date": ua.finish_date
        })

    return {
        "username": username,
        "total_items": total_count,
        "limit": limit,
        "offset": offset,
        "items": animelist
    }


@router.get('/user/{username}/events')
def get_user_events(
    username: str,
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Retorna o histórico de eventos de atividade (WatchEvent) capturados pelo ETL.
    """
    from app.models.models import WatchEvent
    username = username.strip().lower()
    user = db.query(User).filter(User.name == username).first()

    if not user:
        raise HTTPException(status_code=404, detail=f"Usuário '{username}' não encontrado.")

    events = db.query(WatchEvent, Anime).outerjoin(Anime, WatchEvent.anime_id == Anime.id)\
        .filter(WatchEvent.user_id == user.id)\
        .order_by(WatchEvent.created_at.desc())\
        .limit(limit).all()

    results = []
    for ev, anime in events:
        results.append({
            "id": ev.id,
            "event_type": ev.event_type,
            "description": ev.description,
            "created_at": ev.created_at,
            "anime_title": anime.title if anime else None,
            "anime_picture": anime.main_picture if anime else None
        })

    return {
        "username": username,
        "total_events": len(results),
        "events": results
    }


class WatchEventCreate(BaseModel):
    anime_id: Optional[int] = None
    event_type: str
    description: str
    created_at: Optional[datetime] = None


@router.post('/user/{username}/events/batch')
def batch_insert_user_events(
    username: str,
    events: List[WatchEventCreate],
    db: Session = Depends(get_db)
):
    """
    Recebe eventos de atividade capturados pelo RSS Trigger e salva no banco de dados.
    Evita duplicações comparando user_id, description e created_at.
    """
    from app.models.models import WatchEvent
    username = username.strip().lower()
    user = db.query(User).filter(User.name == username).first()
    if not user:
        raise HTTPException(status_code=404, detail=f"Usuário '{username}' não encontrado.")

    inserted = 0
    for ev in events:
        query = db.query(WatchEvent).filter(
            WatchEvent.user_id == user.id,
            WatchEvent.description == ev.description
        )
        if ev.created_at:
            query = query.filter(WatchEvent.created_at == ev.created_at)

        exists = query.first()
        if not exists:
            new_event = WatchEvent(
                user_id=user.id,
                anime_id=ev.anime_id,
                event_type=ev.event_type,
                description=ev.description,
                created_at=ev.created_at or datetime.now()
            )
            db.add(new_event)
            inserted += 1

    db.commit()
    return {"status": "success", "inserted": inserted, "total_received": len(events)}


@router.get('/compare')
def compare_users(
    user1: str = Query(..., description="Username do primeiro perfil"),
    user2: str = Query(..., description="Username do segundo perfil"),
    db: Session = Depends(get_db)
):
    """
    Compara dois perfis do AniMind: KPIs lado a lado, % de Compatibilidade,
    animes em comum, divergências de opinião e recomendações cruzadas.
    """
    user1 = user1.strip().lower()
    user2 = user2.strip().lower()

    u1 = db.query(User).filter(User.name == user1).first()
    u2 = db.query(User).filter(User.name == user2).first()

    if not u1:
        raise HTTPException(status_code=404, detail=f"Usuário '{user1}' não encontrado. Sincronize o perfil primeiro.")
    if not u2:
        raise HTTPException(status_code=404, detail=f"Usuário '{user2}' não encontrado. Sincronize o perfil primeiro.")

    # ── Fetch anime lists ────────────────────────────────────────────────────
    def fetch_animelist(user_id):
        rows = db.query(UserAnime, Anime).join(Anime, UserAnime.anime_id == Anime.id).filter(UserAnime.user_id == user_id).all()
        return {anime.id: (ua, anime) for ua, anime in rows}

    list1 = fetch_animelist(u1.id)  # {anime_id: (ua, anime)}
    list2 = fetch_animelist(u2.id)

    # ── KPIs helper ──────────────────────────────────────────────────────────
    def calc_stats(alist):
        total = len(alist)
        total_eps = 0
        total_secs = 0
        scored_count = 0
        sum_scores = 0
        rewatched = 0
        status_dist = {}
        timeline = {}  # YYYY-MM -> count of completed
        for anime_id, (ua, anime) in alist.items():
            st = ua.status or 'unknown'
            status_dist[st] = status_dist.get(st, 0) + 1
            eps = ua.num_episodes_watched or 0
            total_eps += eps
            if anime.average_episode_duration:
                total_secs += eps * anime.average_episode_duration
            if ua.score and ua.score > 0:
                scored_count += 1
                sum_scores += ua.score
            if ua.is_rewatching or (ua.num_times_rewatched and ua.num_times_rewatched > 0):
                rewatched += 1
            if ua.finish_date and st == 'completed':
                key = ua.finish_date.strftime('%Y-%m')
                timeline[key] = timeline.get(key, 0) + 1
        mean_score = round(sum_scores / scored_count, 2) if scored_count > 0 else 0
        total_days = round(total_secs / 86400, 1)
        return {
            "total_animes": total,
            "total_episodes": total_eps,
            "total_days": total_days,
            "mean_score": mean_score,
            "rewatched_count": rewatched,
            "status_distribution": status_dist,
            "timeline": timeline
        }

    stats1 = calc_stats(list1)
    stats2 = calc_stats(list2)

    # ── Shared animes ────────────────────────────────────────────────────────
    shared_ids = set(list1.keys()) & set(list2.keys())

    shared_animes = []
    mutual_diffs = []  # for compatibility: only when both scored

    for aid in shared_ids:
        ua1, anime = list1[aid]
        ua2, _ = list2[aid]
        s1 = ua1.score or 0
        s2 = ua2.score or 0
        diff = s1 - s2
        if s1 > 0 and s2 > 0:
            mutual_diffs.append(abs(diff))
        shared_animes.append({
            "id": anime.id,
            "title": anime.title,
            "picture": anime.main_picture,
            "u1_score": s1 if s1 > 0 else None,
            "u2_score": s2 if s2 > 0 else None,
            "diff": diff,
            "abs_diff": abs(diff),
            "u1_status": ua1.status,
            "u2_status": ua2.status,
            "global_mean": anime.mean
        })

    # Sort shared by average score desc (most relevant first)
    shared_animes.sort(key=lambda x: -((x['u1_score'] or 0) + (x['u2_score'] or 0)) / 2)
    shared_animes = shared_animes[:200]

    # ── Top divergences ──────────────────────────────────────────────────────
    # Only where both scored
    both_scored = [a for a in shared_animes if a['u1_score'] and a['u2_score'] and a['u1_score'] > 0 and a['u2_score'] > 0]
    top_divergences = sorted(both_scored, key=lambda x: -x['abs_diff'])[:10]

    # ── Compatibility score ──────────────────────────────────────────────────
    compatibility_score = None
    compatibility_note = None
    if len(mutual_diffs) >= 5:
        avg_diff = sum(mutual_diffs) / len(mutual_diffs)
        score_agreement = max(0.0, 1.0 - (avg_diff / 10.0))
        total_max = max(stats1['total_animes'], stats2['total_animes'])
        overlap_ratio = len(shared_ids) / total_max if total_max > 0 else 0.0
        compatibility_score = round((overlap_ratio * 0.4 + score_agreement * 0.6) * 100, 1)
    else:
        compatibility_note = f"Poucos animes avaliados em comum ({len(mutual_diffs)}). Sincronize mais dados para calcular compatibilidade."

    # ── Cross-recommendations ────────────────────────────────────────────────
    ids2 = set(list2.keys())
    ids1 = set(list1.keys())

    # What u1 loves that u2 hasn't seen (score >= 8, u2 doesn't have it)
    rec_for_u2 = []
    for aid, (ua, anime) in list1.items():
        if aid not in ids2 and ua.score and ua.score >= 8:
            rec_for_u2.append({
                "id": anime.id, "title": anime.title, "picture": anime.main_picture,
                "recommender_score": ua.score, "global_mean": anime.mean,
                "studio": anime.studio, "media_type": anime.media_type
            })
    rec_for_u2.sort(key=lambda x: -x['recommender_score'])
    rec_for_u2 = rec_for_u2[:20]

    # What u2 loves that u1 hasn't seen
    rec_for_u1 = []
    for aid, (ua, anime) in list2.items():
        if aid not in ids1 and ua.score and ua.score >= 8:
            rec_for_u1.append({
                "id": anime.id, "title": anime.title, "picture": anime.main_picture,
                "recommender_score": ua.score, "global_mean": anime.mean,
                "studio": anime.studio, "media_type": anime.media_type
            })
    rec_for_u1.sort(key=lambda x: -x['recommender_score'])
    rec_for_u1 = rec_for_u1[:20]

    return {
        "user1": {
            "name": u1.name,
            "picture": u1.picture,
            "stats": stats1
        },
        "user2": {
            "name": u2.name,
            "picture": u2.picture,
            "stats": stats2
        },
        "compatibility_score": compatibility_score,
        "compatibility_note": compatibility_note,
        "shared_count": len(shared_ids),
        "shared_animes": shared_animes,
        "top_divergences": top_divergences,
        "recommended_for_u2": rec_for_u2,
        "recommended_for_u1": rec_for_u1,
        "timeline_u1": stats1["timeline"],
        "timeline_u2": stats2["timeline"]
    }


@router.post('/recommendations/spark/train')
def trigger_spark_recommendations(background_tasks: BackgroundTasks):
    """
    Dispara o treinamento do modelo de Recomendação PySpark MLlib (ALS) em background.
    """
    from spark_jobs.als_recommendation import run_als_recommendation
    background_tasks.add_task(run_als_recommendation)
    return {
        "message": "Job PySpark MLlib (ALS Collaborative Filtering) disparado em background com sucesso!",
        "status": "processing"
    }


@router.get('/user/{username}/unwatched-sequels')
def get_unwatched_sequels(
    username: str,
    relation_types: Optional[str] = Query(
        None,
        description="Filtrar por tipos separados por vírgula. Ex: sequel,prequel. Padrão: todos."
    ),
    db: Session = Depends(get_db)
):
    """
    Retorna animes relacionados (sequências, prequelas, histórias paralelas, etc.)
    que o usuário ainda não tem na sua lista.
    """
    from app.models.models import AnimeRelation

    # Tipos por grupo de filtro
    ALL_TYPES = {
        "sequel", "prequel",
        "side_story", "parent_story",
        "alternative_setting", "alternative_version",
        "summary", "full_story"
    }

    # Prioridade de exibição (menor = mais relevante)
    TYPE_PRIORITY = {
        "sequel": 1, "prequel": 2,
        "full_story": 3, "parent_story": 4,
        "side_story": 5, "alternative_version": 6,
        "alternative_setting": 7, "summary": 8
    }

    # Traduções para o frontend
    TYPE_LABELS = {
        "sequel": "Sequência",
        "prequel": "Prequela",
        "side_story": "História Paralela",
        "parent_story": "Obra Original",
        "alternative_setting": "Universo Alternativo",
        "alternative_version": "Versão Alternativa",
        "summary": "Resumo",
        "full_story": "História Completa"
    }

    username = username.strip().lower()
    user = db.query(User).filter(User.name == username).first()
    if not user:
        raise HTTPException(status_code=404, detail=f"Usuário '{username}' não encontrado.")

    # Filtro de tipos selecionados
    if relation_types:
        selected_types = {t.strip() for t in relation_types.split(",") if t.strip() in ALL_TYPES}
    else:
        selected_types = ALL_TYPES

    # IDs de animes que o usuário já tem na lista (qualquer status) - Usado para garantir que a sugestão não está na lista
    user_anime_ids = {
        row.anime_id
        for row in db.query(UserAnime.anime_id).filter(UserAnime.user_id == user.id).all()
    }

    # IDs de animes que o usuário de fato completou (status == completed) - Usado como base/origem para sugestões
    completed_anime_ids = {
        row.anime_id
        for row in db.query(UserAnime.anime_id)
        .filter(UserAnime.user_id == user.id, UserAnime.status == "completed")
        .all()
    }

    # Mapa de animes do usuário com score e status
    user_anime_map = {
        ua.anime_id: ua
        for ua in db.query(UserAnime).filter(UserAnime.user_id == user.id).all()
    }

    # Busca relações partindo APENAS de animes completados, recomendando algo fora da lista do usuário
    relations = (
        db.query(AnimeRelation, Anime)
        .join(Anime, AnimeRelation.related_anime_id == Anime.id)
        .filter(
            AnimeRelation.anime_id.in_(completed_anime_ids),
            AnimeRelation.relation_type.in_(selected_types),
            AnimeRelation.relation_type != "__none__",
            AnimeRelation.related_anime_id.notin_(user_anime_ids)
        )
        .all()
    )


    # Busca dados dos animes-fonte (o que o usuário já viu)
    source_ids = {rel.anime_id for rel, _ in relations}
    source_animes = {
        a.id: a
        for a in db.query(Anime).filter(Anime.id.in_(source_ids)).all()
    }

    # Deduplica: se o mesmo anime relacionado aparecer por múltiplos caminhos,
    # mantém o de maior prioridade de relation_type
    seen: dict[int, dict] = {}
    for relation, related_anime in relations:
        rid = related_anime.id
        priority = TYPE_PRIORITY.get(relation.relation_type, 99)
        if rid in seen and seen[rid]["_priority"] <= priority:
            continue

        source = source_animes.get(relation.anime_id)
        ua = user_anime_map.get(relation.anime_id)

        seen[rid] = {
            "_priority": priority,
            "source_anime_id": relation.anime_id,
            "source_anime_title": source.title if source else None,
            "source_anime_picture": source.main_picture if source else None,
            "source_user_score": ua.score if ua else None,
            "source_user_status": ua.status if ua else None,
            "relation_type": relation.relation_type,
            "relation_type_label": TYPE_LABELS.get(relation.relation_type, relation.relation_type),
            "related_anime_id": related_anime.id,
            "related_anime_title": related_anime.title,
            "related_anime_picture": related_anime.main_picture,
            "related_anime_mean": related_anime.mean,
            "related_anime_rank": related_anime.rank,
            "related_anime_media_type": related_anime.media_type,
            "related_anime_num_episodes": related_anime.num_episodes,
            "related_anime_genres": related_anime.genres,
            "related_anime_studio": related_anime.studio,
        }

    # Ordena: prioridade de tipo → nota global desc
    results = sorted(
        seen.values(),
        key=lambda x: (x["_priority"], -(x["related_anime_mean"] or 0))
    )

    # Remove campo interno antes de retornar
    for r in results:
        r.pop("_priority", None)

    return {
        "username": username,
        "total": len(results),
        "selected_types": list(selected_types),
        "items": results
    }

