import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CLIENT_ID = os.getenv("CLIENT_ID")


def _get_auth_headers(db=None) -> dict:
    """
    Retorna os cabeçalhos de autenticação para as chamadas da API v2 do MAL.
    Prioriza o Bearer Token do sistema; fallback para X-MAL-CLIENT-ID.
    """
    headers = {}
    if db is not None:
        try:
            from app.services.token_service import get_valid_access_token
            access_token = get_valid_access_token(db)
            if access_token:
                headers['Authorization'] = f'Bearer {access_token}'
                return headers
        except Exception:
            pass

    if CLIENT_ID:
        headers['X-MAL-CLIENT-ID'] = CLIENT_ID
    return headers


def get_data_from_mal_api(user_anime_list_url: str, db=None):
    """Busca a lista pública de animes de um usuário de forma paginada na API do MAL."""
    headers = _get_auth_headers(db)

    # Se a URL já possui parâmetros (como a próxima página do paging.next), não sobrescreve os params
    if '?' in user_anime_list_url:
        params = None
    else:
        params = {
            'fields': 'list_status{status,score,num_episodes_watched,is_rewatching,num_times_rewatched,start_date,finish_date},num_episodes,mean,rank,popularity,genres,synopsis,num_scoring_users,media_type,status,studios,main_picture',

            'nsfw': 'true',
            'limit': 100
        }

    try:
        user_list_response = requests.get(user_anime_list_url, headers=headers, params=params, timeout=15)

        if user_list_response.status_code != 200:
            print(f"[MAL API] Erro na requisição animelist: {user_list_response.status_code} - {user_list_response.text}")
            return None, None

        user_list_data = user_list_response.json()
        next_url = user_list_data.get('paging', {}).get('next')
        return user_list_data, next_url
    except Exception as e:
        print(f"[MAL API] Exceção ao consultar animelist: {e}")
        return None, None


def get_anime_data_from_mal(mal_id: int, db=None):
    """Busca dados detalhados de um anime pelo ID na API do MAL."""
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)

    url = f'https://api.myanimelist.net/v2/anime/{mal_id}'
    headers = _get_auth_headers(db)
    params = {
        'fields': 'id,title,main_picture,mean,media_type,status,genres,num_episodes,source,average_episode_duration,studios,statistics'
    }

    try:
        response = http.get(url, headers=headers, params=params, timeout=(5, 15))
        if response.status_code == 200:
            return response.json()
        else:
            print(f"[MAL API] Erro HTTP {response.status_code} para anime {mal_id}")
            return None
    except Exception as err:
        print(f"[MAL API] Erro ao consultar anime {mal_id}: {err}")

    return None


def scrape_mal_public_avatar(username: str) -> str | None:
    """
    Extrai a foto de perfil pública diretamente da página de perfil HTML do MyAnimeList (igual ao Anime.plus).
    Super leve, rápido e funciona para qualquer usuário sem necessidade de API/Login.
    """
    url = f"https://myanimelist.net/profile/{username}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            import re
            # 1. Procura pela meta tag OpenGraph og:image
            match = re.search(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\'\?]+)', response.text, re.IGNORECASE)
            if match:
                img_url = match.group(1)
                if 'userimages' in img_url or 'myanimelist.net' in img_url:
                    return img_url

            # 2. Procura direto pela imagem do CDN do MAL
            match_img = re.search(r'https://cdn\.myanimelist\.net/images/userimages/\d+\.[a-zA-Z0-9]+', response.text)
            if match_img:
                return match_img.group(0)
    except Exception as e:
        print(f"[SCRAPE AVATAR] Exceção ao buscar foto pública de {username}: {e}")

    return None


def get_user_profile_from_mal(username: str, db=None):
    """
    Busca informações de perfil do usuário via API oficial v2 do MAL.
    Para o próprio perfil autenticado, usa /v2/users/@me.
    Para amigos e usuários públicos, obtém a foto oficial diretamente via HTML (estilo Anime.plus).
    """
    profile_info = {
        'picture': None,
        'gender': None,
        'joined_at': None
    }

    headers = _get_auth_headers(db)
    params = {'fields': 'id,name,picture,gender,joined_at'}

    # 1. Se houver Bearer Token, consulta /v2/users/@me para o usuário autenticado
    if headers and 'Authorization' in headers:
        try:
            me_url = 'https://api.myanimelist.net/v2/users/@me'
            res = requests.get(me_url, headers=headers, params=params, timeout=10)
            if res.status_code == 200:
                data = res.json()
                me_name = (data.get('name') or '').strip().lower()
                req_name = (username or '').strip().lower()

                if not req_name or req_name == me_name or req_name == '@me':
                    profile_info['picture'] = data.get('picture')
                    profile_info['gender'] = data.get('gender')
                    profile_info['joined_at'] = data.get('joined_at')
                    return profile_info
        except Exception as e:
            print(f"[MAL PROFILE] Exceção ao consultar /v2/users/@me: {e}")

    # 2. Se a foto não foi obtida via @me (ex: buscando amigo), raspa a foto pública oficial do HTML
    public_avatar = scrape_mal_public_avatar(username)
    if public_avatar:
        profile_info['picture'] = public_avatar

    return profile_info