import os
import time
import requests
import xml.etree.ElementTree as ET
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# Configuração de Log
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("rss_trigger")

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000/api/v1")
INTERVAL_MINUTES = int(os.getenv("RSS_TRIGGER_INTERVAL", "15"))

def fetch_active_users():
    """Busca a lista de usuários ativos na API."""
    url = f"{API_BASE_URL}/user/active"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Erro ao buscar usuários ativos em {url}: {e}")
        return []

def get_latest_rss_pubdate(username):
    """Lê o RSS do usuário e retorna a data do item mais recente como um datetime UTC."""
    url = f"https://myanimelist.net/rss.php?type=rw&u={username}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # Parseia o XML
        root = ET.fromstring(response.content)
        
        # O RSS padrão do MAL tem a estrutura: rss -> channel -> item -> pubDate
        first_item = root.find("./channel/item")
        if first_item is None:
            return None
            
        pubdate_str = first_item.findtext("pubDate")
        if not pubdate_str:
            return None
            
        # Parseia a data do formato RFC 2822 (ex: Thu, 01 Aug 2024 12:00:00 -0700)
        dt = parsedate_to_datetime(pubdate_str)
        return dt.astimezone(timezone.utc)
        
    except Exception as e:
        logger.error(f"Erro ao ler RSS do usuário {username}: {e}")
        return None

def trigger_sync(username):
    """Aciona o endpoint de sync da API."""
    url = f"{API_BASE_URL}/animelist/{username}"
    try:
        response = requests.post(url, timeout=60)
        response.raise_for_status()
        logger.info(f"Sincronização concluída para o usuário: {username}")
    except Exception as e:
        logger.error(f"Falha ao acionar sync para {username}: {e}")

def fetch_and_send_news():
    """Busca notícias globais do MAL e envia para a API via POST /batch."""
    url = "https://myanimelist.net/rss/news.xml"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        news_items = []
        for item in root.findall("./channel/item"):
            title = item.findtext("title")
            link = item.findtext("link")
            pub_date = item.findtext("pubDate")
            description = item.findtext("description")
            
            # Formata a data se existir
            dt_iso = None
            if pub_date:
                try:
                    dt = parsedate_to_datetime(pub_date)
                    dt_iso = dt.isoformat()
                except:
                    pass
                    
            if title and link:
                news_items.append({
                    "title": title,
                    "link": link,
                    "pub_date": dt_iso,
                    "description": description,
                    "image_url": None
                })
        
        if news_items:
            api_url = f"{API_BASE_URL}/news/batch"
            res = requests.post(api_url, json=news_items, timeout=30)
            res.raise_for_status()
            logger.info(f"Enviadas {len(news_items)} notícias para a API.")
            
    except Exception as e:
        logger.error(f"Erro ao buscar/enviar notícias: {e}")

def run_cycle():
    logger.info("Iniciando ciclo de verificação de RSS...")
    
    # 1. Busca e atualiza as notícias globais
    fetch_and_send_news()
    
    # 2. Verifica usuários ativos
    users = fetch_active_users()
    
    for u in users:
        username = u.get("name")
        last_synced_str = u.get("last_synced_at")
        
        if not username:
            continue
            
        # Converte a data do banco para UTC (se existir)
        last_synced_dt = None
        if last_synced_str:
            try:
                # O formato retornado do banco normalmente é ISO-8601
                # Remover o 'Z' se houver para o fromisoformat funcionar e garantir timezone
                last_synced_str = last_synced_str.replace("Z", "+00:00")
                last_synced_dt = datetime.fromisoformat(last_synced_str)
                # Garante que seja UTC para comparação correta
                if last_synced_dt.tzinfo is None:
                    last_synced_dt = last_synced_dt.replace(tzinfo=timezone.utc)
                else:
                    last_synced_dt = last_synced_dt.astimezone(timezone.utc)
            except Exception as e:
                logger.warning(f"Erro ao parsear last_synced_at '{last_synced_str}' do usuário {username}: {e}")
        
        # Pega a data do item mais recente no RSS
        latest_rss_dt = get_latest_rss_pubdate(username)
        
        if not latest_rss_dt:
            logger.info(f"[{username}] Nenhum item encontrado no RSS.")
            continue
            
        logger.info(f"[{username}] Última atividade RSS: {latest_rss_dt} | Último sync BD: {last_synced_dt}")
        
        # Decide se precisa sincronizar
        needs_sync = False
        if not last_synced_dt:
            logger.info(f"[{username}] Primeira sincronização (sem histórico no DB).")
            needs_sync = True
        elif latest_rss_dt > last_synced_dt:
            logger.info(f"[{username}] Nova atividade detectada no RSS! Disparando sync...")
            needs_sync = True
            
        if needs_sync:
            trigger_sync(username)

def main():
    logger.info("Serviço RSS Trigger inicializado.")
    while True:
        run_cycle()
        logger.info(f"Aguardando {INTERVAL_MINUTES} minutos para o próximo ciclo...")
        time.sleep(INTERVAL_MINUTES * 60)

if __name__ == "__main__":
    main()