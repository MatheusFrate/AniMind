import json
import logging
import os
from pathlib import Path
import sys
import time
import requests
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("relation_collector")

RAIZ_PROJETO = Path(__file__).resolve().parent.parent.parent
CAMINHO_ESTADO = RAIZ_PROJETO / "data_lake" / "configs" / "relation_state.json"

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000/api/v1")
CLIENT_ID = os.getenv("CLIENT_ID")


def create_data_lake_structure():
    """
    Cria a estrutura de diretórios para o data lake, se não existir.
    """
    data_lake_path = RAIZ_PROJETO / "data_lake"
    configs_path = data_lake_path / "configs"
    bronze_path = data_lake_path / "bronze"
    relations_path = bronze_path / "relations"

    if not data_lake_path.exists():
        data_lake_path.mkdir()
        print(f"[RELATION ETL] Diretório 'data_lake' criado em: {data_lake_path}")

    if not configs_path.exists():
        configs_path.mkdir()
        print(f"[RELATION ETL] Diretório 'configs' criado em: {configs_path}")

    if not bronze_path.exists():
        bronze_path.mkdir()
        print(f"[RELATION ETL] Diretório 'bronze' criado em: {bronze_path}")

    if not relations_path.exists():
        relations_path.mkdir()
        print(f"[RELATION ETL] Diretório 'relations' criado em: {relations_path}")
    
    # Cria o arquivo de estado se não existir
    if not CAMINHO_ESTADO.exists():
        initial_state = {
            "last_offset": 0,
            "last_run": None
        }
        with open(CAMINHO_ESTADO, "w") as f:
            json.dump(initial_state, f)
        print(f"[RELATION ETL] Arquivo de estado inicial criado em: {CAMINHO_ESTADO}")

def save_files(data, filename: str):
    """
    Salva os dados em um arquivo JSON no diretório data_lake.
    """
    data_lake_path = RAIZ_PROJETO / "data_lake"
    file_path = data_lake_path / "bronze" / "relations" / filename
    try:
        if isinstance(data, dict):
            data = [data]  # Converte para lista se for um dicionário único
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"[RELATION ETL] Dados salvos em: {file_path}")
    except Exception as e:
        print(f"[RELATION ETL] Erro ao salvar arquivo {file_path}: {e}")


def fetch_active_animes():
    """Busca a lista de animes ativos na API."""
    url = f"{API_BASE_URL}/animelist/active_ids"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Erro ao buscar animes ativos em {url}: {e}")
        return []

def run_relation_collector() -> int:
    """
    Busca related_anime via GET /v2/anime/{id} para cada anime
    presente na lista de algum usuário e salva em anime_relations.

    O endpoint /ranking nao suporta o campo related_anime, entao
    precisamos buscar individualmente apenas os animes relevantes.
    
    Possui uma validade de cache (TTL) de 14 dias.
    """
    logger.info("Iniciando ciclo de verificação de RSS...")
    animes = fetch_active_animes()

    try:


        logger.info("=" * 60)
        logger.info(f"[RELATION ETL] Buscando related_anime para {len(animes)} animes da lista dos usuarios...")
        headers = {}
        headers['X-MAL-CLIENT-ID'] = CLIENT_ID
        start = time.time()

        inserted = 0
        lote_tamanho = 500
        anime_relations = []

        last_processed_id = 0
        last_run = None

        if CAMINHO_ESTADO.exists():
            with open(CAMINHO_ESTADO, "r") as f:
                estado = json.load(f)
                last_processed_id = estado.get("last_offset", 0)
                last_run_str = estado.get("last_run")
                if last_run_str:
                    try:
                        last_run = datetime.strptime(last_run_str, "%Y-%m-%d_%H-%M-%S")
                    except ValueError:
                        pass
        
        # TTL de 14 dias: se já rodou inteiro (last_offset == 0) e faz menos de 14 dias, não faz nada e desliga
        if last_processed_id == 0 and last_run:
            dias_passados = (datetime.now() - last_run).days
            if dias_passados < 14:
                logger.info(f"[RELATION ETL] Coleta já foi realizada há {dias_passados} dias (TTL de 14 dias). Nenhuma ação necessária.")
                logger.info("[RELATION ETL] Desligando container graciosamente...")
                sys.exit(0)
                
        # Garante que a lista está ordenada para o last_processed_id funcionar corretamente
        animes = sorted(animes)
        
        # Filtra a lista só com os animes que AINDA não processamos neste ciclo
        animes_pendentes = [a for a in animes if a > last_processed_id]
        
        if not animes_pendentes:
            logger.info("[RELATION ETL] Nenhum anime novo para processar.")
            # Reseta estado e marca timestamp
            timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
            with open(CAMINHO_ESTADO, "w") as f:
                json.dump({"last_offset": 0, "last_run": timestamp}, f)
            logger.info("[RELATION ETL] Desligando container graciosamente...")
            sys.exit(0)
            
        logger.info(f"[RELATION ETL] Retomando a partir do ID {last_processed_id}. Restam {len(animes_pendentes)} animes.")

        for i, anime_id in enumerate(animes_pendentes):
            logger.info(f"[RELATION ETL] ({i+1}/{len(animes_pendentes)}) Coletando dados do anime ID {anime_id}...")
            try:
                url = f"https://api.myanimelist.net/v2/anime/{anime_id}"
                
                # Mecanismo de Retry
                res = None
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        res = requests.get(
                            url,
                            headers=headers,
                            params={"fields": "related_anime"},
                            timeout=30
                        )
                        break
                    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as req_err:
                        if attempt < max_retries - 1:
                            wait_time = (attempt + 1) * 2
                            logger.info(f"[RELATION ETL] Timeout no anime {anime_id} (tentativa {attempt + 1}/{max_retries}). Retentando em {wait_time}s...")
                            time.sleep(wait_time)
                        else:
                            raise req_err

                if not res:
                    continue

                if res.status_code == 429:
                    logger.info(f"[RELATION ETL] Rate limit (429) atingido no anime {anime_id}, aguardando 60s para esfriar...")
                    time.sleep(60)
                    continue

                if res.status_code != 200:
                    logger.info(f"[RELATION ETL] Erro HTTP {res.status_code} no anime {anime_id} - Ignorando...")
                    continue

                data = res.json()
                related_list = data.get("related_anime", [])


                # Evita duplicados na mesma resposta da API (bug do MAL)
                added_in_loop = set()
                for rel in related_list:
                    rel_node = rel.get("node", {})
                    rel_id = rel_node.get("id")
                    rel_type = rel.get("relation_type")
                    if not rel_id or not rel_type:
                        continue
                    
                    key = (anime_id, int(rel_id))
                    if key in added_in_loop:
                        continue

                    AnimeRelation = {
                        "anime_id": anime_id,
                        "related_anime_id": int(rel_id),
                        "relation_type": rel_type
                    }
                    added_in_loop.add(key)
                    anime_relations.append(AnimeRelation)
                    inserted += 1

                if len(anime_relations) >= lote_tamanho:
                    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
                    nome_arquivo = f"anime_relations_batch_{anime_id}_{timestamp}.json"
                    
                    # Salva os dados na Bronze
                    save_files(anime_relations, nome_arquivo)
                    
                    # Atualiza o arquivo de estado para não precisarmos repetir
                    with open(CAMINHO_ESTADO, "w") as f:
                        json.dump({"last_offset": anime_id, "last_run": timestamp}, f)
                    
                    # Limpa a lista da memória para o próximo lote
                    anime_relations.clear()


                # Log de progresso a cada 10 animes
                if (i + 1) % 10 == 0:
                    elapsed = round(time.time() - start, 1)
                    logger.info(f"[RELATION ETL] {i+1}/{len(animes_pendentes)} processados ({elapsed}s) — Relações geradas: {inserted} | Lote atual: {len(anime_relations)}/{lote_tamanho}")

                time.sleep(0.35)

            except Exception as e:
                import traceback
                logger.error(f"[RELATION ETL] ERRO no anime {anime_id}: {e}\n{traceback.format_exc()}")
                continue

        if anime_relations:
            timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
            save_files(anime_relations, f"anime_relations_{timestamp}.json")
            logger.info(f"[RELATION ETL] Dados finais salvos em: anime_relations_{timestamp}.json")

        # Ao finalizar a lista, reseta o offset para 0 e atualiza a data da última execução (TTL)
        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        with open(CAMINHO_ESTADO, "w") as f:
            json.dump({"last_offset": 0, "last_run": timestamp}, f)

        elapsed = round(time.time() - start, 1)
        logger.info(f"[RELATION ETL] Concluido em {elapsed}s! {inserted} novas relacoes para {len(animes_pendentes)} animes.")
        logger.info("=" * 60)
        logger.info("[RELATION ETL] Missão cumprida. Desligando container graciosamente...")
        sys.exit(0)

    except Exception as e:
        logger.info(f"[RELATION ETL] Erro geral: {e}")
  


if __name__ == "__main__":
    run_relation_collector()
