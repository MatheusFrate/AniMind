"""
aniMind - RSS News & User Activity Ingestion Flow
Orquestrado via Prefect Cloud (Frequência recomendada: a cada 1 hora)
"""

import sys
from pathlib import Path
from prefect import flow, task

# Garante que a raiz do projeto esteja no sys.path
RAIZ_PROJETO = Path(__file__).resolve().parent.parent
if str(RAIZ_PROJETO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROJETO))

from extractors.rss_extractor.trigger import (
    fetch_and_send_news,
    fetch_active_users,
    fetch_and_send_user_rss_events,
    get_latest_rss_pubdate,
    trigger_sync,
)
from datetime import datetime, timezone


@task(name="01 - Extrair Notícias Globais (MAL RSS)", retries=2, retry_delay_seconds=15)
def task_fetch_global_news():
    """Coleta as últimas notícias da indústria do anime via RSS e persiste na tabela news_rss."""
    print("📰 [Task] Buscando notícias globais do MyAnimeList RSS...")
    fetch_and_send_news()
    print("✅ [Task] Notícias globais sincronizadas com sucesso!")


@task(name="02 - Sincronizar Atividades e Watch Events dos Usuários", retries=2, retry_delay_seconds=15)
def task_sync_user_activities():
    """Lê o feed RSS pessoal de cada usuário ativo, atualiza o histórico de episódios e dispara sync se necessário."""
    print("👤 [Task] Verificando atividades recentes dos usuários ativos...")
    users = fetch_active_users()
    print(f"📊 Usuários ativos encontrados: {len(users)}")

    for u in users:
        username = u.get("name")
        last_synced_str = u.get("last_synced_at")

        if not username:
            continue

        # 1. Puxa as atividades recentes do RSS e grava na tabela watch_events
        fetch_and_send_user_rss_events(username)

        # 2. Converte a data do banco para UTC (se existir)
        last_synced_dt = None
        if last_synced_str:
            try:
                last_synced_str = last_synced_str.replace("Z", "+00:00")
                last_synced_dt = datetime.fromisoformat(last_synced_str)
                if last_synced_dt.tzinfo is None:
                    last_synced_dt = last_synced_dt.replace(tzinfo=timezone.utc)
                else:
                    last_synced_dt = last_synced_dt.astimezone(timezone.utc)
            except Exception as e:
                print(f"⚠️ Erro ao parsear data de sync do usuário {username}: {e}")

        # 3. Compara com o RSS para ver se precisa sincronizar a biblioteca completa
        latest_rss_dt = get_latest_rss_pubdate(username)
        if not latest_rss_dt:
            continue

        needs_sync = False
        if not last_synced_dt:
            print(f"[{username}] Primeira sincronização necessária.")
            needs_sync = True
        elif latest_rss_dt > last_synced_dt:
            print(f"[{username}] Nova atividade no RSS ({latest_rss_dt}) > Último sync ({last_synced_dt}). Disparando sync...")
            needs_sync = True

        if needs_sync:
            trigger_sync(username)

    print("✅ [Task] Atividades dos usuários processadas com sucesso!")


@flow(name="aniMind-RSS-Activity-Flow", log_prints=True)
def rss_activity_flow():
    """
    Fluxo de Ingestão Frequente / Near Real-Time:
    Coleta notícias globais e rastreia novidades assistidas pelos usuários.
    """
    print("🔔 ========================================================")
    print("🔔 Iniciando Ingestão de Atividades RSS (aniMind)")
    print("🔔 ========================================================")

    task_fetch_global_news()
    task_sync_user_activities()

    print("🎉 ========================================================")
    print("🎉 Ingestão RSS finalizada com sucesso!")
    print("🎉 ========================================================")


if __name__ == "__main__":
    # Pode ser executado diretamente ou servido com schedule horário
    rss_activity_flow()
