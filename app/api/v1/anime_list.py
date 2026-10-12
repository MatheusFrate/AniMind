import json
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.models import Anime, UserAnime, User
from app.services.mal_data import get_data_from_mal_api, get_anime_data_from_mal

router = APIRouter()

def _parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(str(date_str)[:10], '%Y-%m-%d').date()
    except Exception:
        return None

def _save_user_animelist_to_bronze(username: str, all_data_items: list, profile_data: dict = None):
    """Salva uma cópia imutável em JSON do perfil e lista de animes do usuário no Data Lake (Bronze)."""
    try:
        raiz_projeto = Path(__file__).resolve().parent.parent.parent.parent
        caminho_bronze_users = raiz_projeto / "data_lake" / "bronze" / "users"
        caminho_bronze_users.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"user_{username}_{timestamp}.json"
        filepath = caminho_bronze_users / filename

        payload = {
            "username": username,
            "synced_at": datetime.now().isoformat(),
            "profile": profile_data,
            "data": all_data_items
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=4)

        print(f"[BRONZE] Perfil e lista de '{username}' salvos com sucesso em: {filepath}")
    except Exception as e:
        print(f"[BRONZE] Erro ao salvar JSON de usuário no Data Lake: {e}")


def sync_user_animelist_core(db: Session, username: str) -> int:
    """
    Função principal de sincronização via API v2 do MAL.
    Atualiza foto de perfil, notas pessoais, datas de início/fim e toda a biblioteca.
    """
    username = username.strip().lower()
    from app.services.mal_data import get_user_profile_from_mal

    profile_data = get_user_profile_from_mal(username, db=db)

    user_db = db.query(User).filter(User.name == username).first()
    if not user_db:
        user_db = User(name=username)
        db.add(user_db)

    if profile_data:
        if profile_data.get('picture'):
            user_db.picture = profile_data.get('picture')
        if profile_data.get('gender'):
            user_db.gender = profile_data.get('gender')
        if profile_data.get('joined_at'):
            user_db.joined_at = _parse_date(profile_data.get('joined_at'))

    user_db.last_synced_at = datetime.now()
    db.commit()
    db.refresh(user_db)

    user_anime_list_url = f'https://api.myanimelist.net/v2/users/{username}/animelist'
    animes_processed = 0
    all_user_items = []

    while user_anime_list_url:
        user_list, user_anime_list_url = get_data_from_mal_api(user_anime_list_url, db=db)

        if not user_list or 'data' not in user_list:
            break

        all_user_items.extend(user_list['data'])

        for item in user_list['data']:
            node = item.get('node', {})
            anime_id = node.get('id')
            if not anime_id:
                continue

            list_status = item.get('list_status', {})
            existing_anime = db.query(Anime).filter(Anime.id == int(anime_id)).first()

            if not existing_anime:
                anime_info = get_anime_data_from_mal(anime_id, db=db) or {}
                studios_list = anime_info.get('studios', []) or node.get('studios', [])
                studio_name = None
                if isinstance(studios_list, list) and len(studios_list) > 0:
                    studio_name = studios_list[0].get('name')
                elif isinstance(studios_list, str):
                    studio_name = studios_list

                main_picture = node.get('main_picture', {}).get('medium') or anime_info.get('main_picture', {}).get('medium')

                genres_list = node.get('genres', []) or anime_info.get('genres', [])
                genres_str = None
                if isinstance(genres_list, list) and len(genres_list) > 0:
                    genres_str = ", ".join([g.get("name") for g in genres_list if isinstance(g, dict) and g.get("name")])

                db_anime = Anime(
                    id=int(anime_id),
                    title=node.get('title') or anime_info.get('title', 'Desconhecido'),
                    main_picture=main_picture,
                    mean=anime_info.get('mean') or node.get('mean'),
                    rank=node.get('rank') or anime_info.get('rank'),
                    popularity=node.get('popularity') or anime_info.get('popularity'),
                    num_episodes=anime_info.get('num_episodes') or node.get('num_episodes'),
                    average_episode_duration=anime_info.get('average_episode_duration'),
                    studio=studio_name,
                    source=anime_info.get('source'),
                    media_type=anime_info.get('media_type') or node.get('media_type'),
                    genres=genres_str,
                    synopsis=node.get('synopsis') or anime_info.get('synopsis'),
                    num_scoring_users=node.get('num_scoring_users') or anime_info.get('num_scoring_users')
                )
                db.add(db_anime)
                db.flush()

            user_anime = UserAnime(
                user_id=user_db.id,
                anime_id=int(anime_id),
                status=list_status.get('status'),
                score=list_status.get('score'),
                num_episodes_watched=list_status.get('num_episodes_watched'),
                is_rewatching=list_status.get('is_rewatching', False),
                num_times_rewatched=list_status.get('num_times_rewatched', 0),
                start_date=_parse_date(list_status.get('start_date')),
                finish_date=_parse_date(list_status.get('finish_date'))
            )
            db.merge(user_anime)
            animes_processed += 1

        db.commit()

    # Salva cópia bruta do perfil e lista do usuário no Data Lake (Bronze)
    _save_user_animelist_to_bronze(username, all_user_items, profile_data)

    return animes_processed


@router.post("/animelist")
@router.post("/animelist/{username}")
async def atualizar_dados(
    username: str = None,
    db: Session = Depends(get_db)
):
    if not username:
        raise HTTPException(status_code=400, detail="Por favor, informe o nome de usuário do MAL.")

    try:
        animes_processed = sync_user_animelist_core(db, username)
        return {
            'message': f'Sincronização concluída com sucesso para "{username}"!',
            'total_animes': animes_processed
        }
    except Exception as e:
        db.rollback()
        print(f"[SYNC] Erro durante a sincronização: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao sincronizar dados: {str(e)}")


@router.get('/animelist/active_ids')
def get_active_animes_id(db: Session = Depends(get_db)):
    """
    Retorna a lista de IDs de animes que estão presentes na lista de algum usuário para coleta de animes relacionados.
    """
    try:
        active_anime_ids = db.query(UserAnime.anime_id).distinct().all()
        return [anime_id for (anime_id,) in active_anime_ids]
    except Exception as e:
        print(f"[SYNC] Erro ao buscar IDs de animes ativos: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar IDs de animes ativos: {str(e)}")