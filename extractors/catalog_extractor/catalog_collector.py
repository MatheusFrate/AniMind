import os
import sys
import time
from pathlib import Path
import requests
import json
import dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

dotenv.load_dotenv()
RAIZ_PROJETO = Path(__file__).resolve().parent.parent.parent
CAMINHO_ESTADO = RAIZ_PROJETO / "data_lake" / "configs" / "catalog_state.json"
CLIENT_ID = os.getenv("CLIENT_ID") or os.getenv("MAL_CLIENT_ID")

def fetch_ranking_page(offset: int, limit: int = 500):
    """Busca uma página de animes do ranking global da API do MAL."""
    headers = {}
    headers['X-MAL-CLIENT-ID'] = CLIENT_ID
    url = "https://api.myanimelist.net/v2/anime/ranking"
    params = {
        'ranking_type': 'all',
        'limit': limit,
        'offset': offset,
        'fields': 'id,title,main_picture,alternative_titles,start_date,end_date,synopsis,mean,rank,popularity,num_list_users,num_scoring_users,nsfw,genres,created_at,updated_at,media_type,status,num_episodes,start_season,broadcast,source,average_episode_duration,rating,studios'
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"[CATALOG ETL] Erro HTTP {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"[CATALOG ETL] Exceção na requisição do catálogo (offset={offset}): {e}")
        return None

def create_data_lake_structure():
    """
    Cria a estrutura de diretórios para o data lake, se não existir.
    """
    data_lake_path = RAIZ_PROJETO / "data_lake"
    configs_path = data_lake_path / "configs"
    bronze_path = data_lake_path / "bronze"
    catalog_path = bronze_path / "catalog"

    if not data_lake_path.exists():
        data_lake_path.mkdir()
        print(f"[CATALOG ETL] Diretório 'data_lake' criado em: {data_lake_path}")

    if not configs_path.exists():
        configs_path.mkdir()
        print(f"[CATALOG ETL] Diretório 'configs' criado em: {configs_path}")

    if not bronze_path.exists():
        bronze_path.mkdir()
        print(f"[CATALOG ETL] Diretório 'bronze' criado em: {bronze_path}")

    if not catalog_path.exists():
        catalog_path.mkdir()
        print(f"[CATALOG ETL] Diretório 'catalog' criado em: {catalog_path}")
    
    # Cria o arquivo de estado se não existir
    if not CAMINHO_ESTADO.exists():
        initial_state = {
            "last_offset": 0,
            "last_run": None
        }
        with open(CAMINHO_ESTADO, "w") as f:
            json.dump(initial_state, f)
        print(f"[CATALOG ETL] Arquivo de estado inicial criado em: {CAMINHO_ESTADO}")

def save_files(data, filename: str):
    """
    Salva os dados em um arquivo JSON no diretório data_lake.
    """
    data_lake_path = RAIZ_PROJETO / "data_lake"
    file_path = data_lake_path / "bronze" / "catalog" / filename
    try:
        if isinstance(data, dict):
            data = [data]  # Converte para lista se for um dicionário único
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"[CATALOG ETL] Dados salvos em: {file_path}")
    except Exception as e:
        print(f"[CATALOG ETL] Erro ao salvar arquivo {file_path}: {e}")


def run_catalog_collector(target_count: int = 1000) -> int:
    """
    Coleta em lote animes do catálogo global do MAL (500 por requisição)
    e armazena no banco de dados. Atualiza o offset em system_state.
    """
    processed_count = 0

    try:
        # Recupera o último offset salvo em system_state
        if not CAMINHO_ESTADO.exists():
            create_data_lake_structure()

        with open(CAMINHO_ESTADO, "r") as f:
            state_data = json.load(f)
            last_offset = state_data.get("last_offset", 0)
            last_run = state_data.get("last_run", None)

        current_offset = int(last_offset) if last_offset is not None and str(last_offset).isdigit() else 0
        processed_count = 0
        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        print("=" * 60) 
        print(f"[CATALOG ETL] - {timestamp}: Coletando {target_count} animes a partir do offset {current_offset}...")
        start_time = time.time()

        limit_per_req = 500  # Máximo aceito pelo endpoint /anime/ranking da MAL API
        requests_needed = (target_count + limit_per_req - 1) // limit_per_req  # ex: 1000 -> 2 requisições

        reached_end = False
        for req_idx in range(requests_needed):
            fetch_offset = current_offset + (req_idx * limit_per_req)
            data = fetch_ranking_page(offset=fetch_offset, limit=limit_per_req)
            timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")

            if not data or 'data' not in data or len(data['data']) == 0:
                print(f"[CATALOG ETL] - {timestamp}: Fim do catálogo atingido no offset {fetch_offset}! Reiniciando offset para 0 para o próximo ciclo de atualização.")
                reached_end = True
                break

            items = data['data']
          
            timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
            processed_count += len(items)
            save_files(items, f"catalog_raw_{fetch_offset}_{timestamp}.json")
            print(f"[CATALOG ETL] - {timestamp}: Requisição {req_idx + 1}/{requests_needed} concluída: {len(items)} animes processados.")

        # Atualiza o offset em system_state (se chegou ao fim, reseta para 0)
        new_offset = 0 if reached_end else current_offset + processed_count
        #  Salva o novo offset no arquivo de estado
        state_data = {
            "last_offset": new_offset,
            "last_run": time.strftime("%Y-%m-%dT%H-%M-%S")
        }
        with open(CAMINHO_ESTADO, "w") as f:
            json.dump(state_data, f)

        elapsed = round(time.time() - start_time, 2)

        print(f"[CATALOG ETL] - {timestamp}: Lote concluído em {elapsed}s! Total nesta rodada: {processed_count} animes. Próximo offset: {new_offset}")
        print("=" * 60)
        # finaliza a execução do script se atingiu o fim do catálogo
        if reached_end:
             sys.exit(0)

    except Exception as e:
        print(f"[CATALOG ETL] - {timestamp}: Erro durante a execução: {e}")
    return processed_count

if __name__ == "__main__":
    create_data_lake_structure()
    while True:
        run_catalog_collector(1000)
