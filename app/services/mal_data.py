import requests
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_data_from_mal_api(user_anime_list_url):
    with open("user.json", "r", encoding="utf-8") as f: 
        dados = json.load(f)
    user_token = dados
    headers = {'Authorization': 'Bearer ' + user_token['access_token']}
    params = {'fields': 'list_status', 'nsfw': 'True'}
    user_list_response = requests.get(user_anime_list_url, headers=headers, params=params)
    if user_list_response.status_code != 200:
        return user_list_response
    user_list_data = user_list_response.json()
    user_anime_list_url = user_list_data['paging'].get('next')
    return user_list_data, user_anime_list_url


def get_anime_data_from_mal(mal_id):
    retry_strategy = Retry(
        total=3,              
        backoff_factor=10,     
        status_forcelist=[400, 401, 402, 403, 404],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)

    try:
        with open("user.json", "r", encoding="utf-8") as f: 
            user_token = json.load(f)
    except Exception as e:
        print(f"Erro ao ler user.json: {e}")
        return None

    url = f'https://api.myanimelist.net/v2/anime/{mal_id}'
    headers = {'Authorization': f"Bearer {user_token['access_token']}"}
    params = {
        'fields': 'id,title,main_picture,mean,media_type,status,genres,my_list_status,num_episodes,source,average_episode_duration,studios,statistics'
    }

    try:
        response = http.get(url, headers=headers, params=params, timeout=(5, 15))
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.HTTPError as err:
        print(f"Erro HTTP: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Erro de Conexão: {err}")
    except requests.exceptions.Timeout as err:
        print(f"A API demorou demasiado tempo (Timeout): {err}")
    except Exception as err:
        print(f"Erro desconhecido: {err}")

    return None