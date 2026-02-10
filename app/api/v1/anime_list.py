from fastapi import APIRouter
from sqlalchemy.orm import Session
from fastapi import Depends
from app.schemas.user import UserCreate, UserResponse
# from app.schemas.anime import Anime
from app.models.models import Anime, UserAnime, User
from app.services.user_service import create_user_logic
import json
from app.services.mal_data import get_data_from_mal_api, get_anime_data_from_mal
from fastapi import Request
from fastapi.encoders import jsonable_encoder
from app.database import get_db

router = APIRouter()

@router.post("/animelist")
async def atualizar_dados(request: Request, db: Session = Depends(get_db)):
    username = request.query_params.get('username')
    print('Atualizando dados...', username)
    print('='*100)
    try:
        username = username.lower()
    except Exception as e:
        print(e)
        return {'error': 'digite o nome do usuario'}
    print(username)
    user_db = db.query(User).filter(User.name == username.lower()).first()
    if not user_db:
        return {'error': 'Utilizador não logado ou não encontrado no banco'}
    try:
        user_anime_list_url = f'https://api.myanimelist.net/v2/users/{username}/animelist'
        animelist = []
        while user_anime_list_url:
            user_list, user_anime_list_url = get_data_from_mal_api(user_anime_list_url) 
            for anime in user_list['data']:
                animeId  = anime['node']['id']
                anime_info = get_anime_data_from_mal(animeId)
                dbanime = Anime(
                    id = int(animeId),
                    title = anime['node']['title'],
                    mean = anime_info['mean'],
                    num_episodes = anime_info['num_episodes'],
                    average_episode_duration = anime_info['average_episode_duration'],
                    studios = anime_info.get('studios'),
                    source = anime_info.get('source'),
                    media_type = anime_info['media_type']
                )
                db.merge(dbanime)
                userAnime = UserAnime(
                    user_id=user_db.id,
                    anime_id=animeId,
                    status = anime['list_status']['status'],
                    score = anime['list_status']['score'],
                    num_episodes_watched = anime['list_status']['num_episodes_watched'],
                    finish_date= anime['list_status'].get('finish_date'),
                    start_date= anime['list_status'].get('start_date')
                )
                db.merge(userAnime)
            db.commit()
        print('Atualização concluida com sucesso!')
        return {'message': 'Atualização concluída com sucesso!'}
    except Exception as e:
        print('deu erro', e)

        return {'error': 'Usuario nao encontrado na base de dados'}
