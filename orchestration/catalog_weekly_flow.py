"""
aniMind - Catalog & Relations Ingestion Flow
Orquestrado via Prefect Cloud (Frequência recomendada: Semanal)
"""

import sys
from pathlib import Path
from prefect import flow, task

# Garante que a raiz do projeto esteja no sys.path
RAIZ_PROJETO = Path(__file__).resolve().parent.parent
if str(RAIZ_PROJETO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROJETO))

from extractors.catalog_extractor.catalog_collector import (
    create_data_lake_structure as create_catalog_lake_structure,
    run_catalog_collector,
)
from extractors.relation_collector.relation_collector import (
    create_data_lake_structure as create_relation_lake_structure,
    run_relation_collector,
)


@task(name="01 - Extrair Catálogo Global MAL (Bronze)", retries=1, retry_delay_seconds=30)
def task_extract_catalog(batch_size: int = 2000):
    """
    Coleta lotes de animes do ranking global da API do MyAnimeList (500 por request)
    e armazena os arquivos JSON brutos em data_lake/bronze/catalog/.
    """
    print(f"📦 [Task] Iniciando extração do catálogo global (meta: {batch_size} animes)...")
    create_catalog_lake_structure()
    try:
        total = run_catalog_collector(target_count=batch_size)
        print(f"✅ [Task] Catálogo processado com sucesso! ({total} animes salvos nesta rodada)")
    except SystemExit as e:
        if e.code not in (0, None):
            raise e
        print("ℹ️ [Task] Fim do catálogo atingido com sucesso (offset reiniciado).")


@task(name="02 - Extrair Grafo de Relações de Animes (Bronze)", retries=1, retry_delay_seconds=30)
def task_extract_relations():
    """
    Busca as relações (sequels, prequels, side stories) de animes ativos na plataforma
    e salva os JSONs brutos em data_lake/bronze/relations/.
    """
    print("🔗 [Task] Iniciando extração do grafo de relações de animes ativos...")
    create_relation_lake_structure()
    try:
        run_relation_collector()
        print("✅ [Task] Grafo de relações atualizado com sucesso!")
    except SystemExit as e:
        if e.code not in (0, None):
            raise e
        print("ℹ️ [Task] Relações já atualizadas recentemente (TTL de 14 dias ativo).")


@flow(name="aniMind-Catalog-Weekly-Flow", log_prints=True)
def catalog_weekly_flow(catalog_batch_size: int = 2000):
    """
    Fluxo de Ingestão Semanal:
    Atualiza o catálogo global do MyAnimeList e o grafo de relações dos animes na camada Bronze.
    """
    print("📚 ========================================================")
    print("📚 Iniciando Ingestão de Catálogo e Relações (Semanal)")
    print("📚 ========================================================")

    # 1. Extrai lotes do catálogo global
    task_extract_catalog(batch_size=catalog_batch_size)

    # 2. Extrai relações de animes ativos
    task_extract_relations()

    print("🎉 ========================================================")
    print("🎉 Ingestão Semanal concluída com sucesso!")
    print("🎉 ========================================================")


if __name__ == "__main__":
    # Pode ser executado diretamente ou agendado no Prefect Cloud
    catalog_weekly_flow()
