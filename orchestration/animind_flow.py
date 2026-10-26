"""
aniMind - Data Lakehouse & Machine Learning Orchestrated Pipeline
Powered by Prefect Cloud & Apache PySpark
"""

import sys
import importlib
from pathlib import Path
from prefect import flow, task

# Garante que a raiz do projeto esteja no sys.path
RAIZ_PROJETO = Path(__file__).resolve().parent.parent
if str(RAIZ_PROJETO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROJETO))


from orchestration.lake_storage import sync_lake_from_cloud, sync_lake_to_cloud


@task(name="00 - Sincronizar Lakehouse do Cloud S3 (Supabase Storage)")
def task_pull_lake():
    """Baixa o estado persistente do Data Lakehouse do Supabase Storage antes do processamento Spark."""
    sync_lake_from_cloud()


@task(name="05 - Sincronizar Lakehouse Atualizado com o Cloud S3 (Supabase Storage)")
def task_push_lake():
    """Envia as tabelas atualizadas (Silver, Gold, Features) de volta para o Supabase Storage S3."""
    sync_lake_to_cloud()


@task(name="06 - Indexação Vetorial Incremental RAG (Gemini -> pgvector)")
def task_vector_indexing():
    """Vetoriza incrementalmente até 800 novos animes por dia (respeitando os limites 80 RPM / 20K TPM / 800 RPD do Free Tier)."""
    from app.services.rag_service import get_gemini_api_key
    if not get_gemini_api_key():
        print("ℹ️ [Task] GEMINI_API_KEY ausente no ambiente. Pulando indexação vetorial incremental.")
        return
    from extractors.rag_indexer.index_animes import run_anime_vector_indexing
    run_anime_vector_indexing(max_items=800, batch_size=80, sleep_between_batches=62.0)


@task(name="01 - Bronze to Silver (Catálogo, Relações e Usuários)", retries=1, retry_delay_seconds=10)
def task_bronze_to_silver():
    """Lê os JSONs brutos da Bronze e gera as tabelas normalizadas em Parquet na Silver."""
    print("▶️ [Task] Executando transformações Bronze -> Silver...")
    
    mod_cat = importlib.import_module("elt_spark.01_bronze_to_silver.process_catalog")
    mod_rel = importlib.import_module("elt_spark.01_bronze_to_silver.process_relations")
    mod_usr = importlib.import_module("elt_spark.01_bronze_to_silver.process_users")

    mod_cat.process_catalog_bronze_to_silver()
    mod_rel.process_relations_bronze_to_silver()
    mod_usr.process_users_bronze_to_silver()
    
    print("✅ [Task] Camada Silver processada com sucesso!")


@task(name="02 - Silver to Gold (Feature Store e Agregações Analíticas)", retries=1, retry_delay_seconds=10)
def task_silver_to_gold():
    """Processa agregações analíticas e a Feature Store na camada Gold."""
    print("▶️ [Task] Executando transformações Silver -> Gold...")
    
    mod_usr_an = importlib.import_module("elt_spark.02_silver_to_gold.process_user_analytics")
    mod_genre  = importlib.import_module("elt_spark.02_silver_to_gold.process_genre_insights")
    mod_studio = importlib.import_module("elt_spark.02_silver_to_gold.process_studio_insights")
    mod_anime  = importlib.import_module("elt_spark.02_silver_to_gold.process_anime_metrics")
    mod_feat   = importlib.import_module("elt_spark.02_silver_to_gold.process_recommendation_features")

    mod_usr_an.process_user_analytics_silver_to_gold()
    mod_genre.process_genre_insights()
    mod_studio.process_studio_insights()
    mod_anime.process_anime_metrics_silver_to_gold()
    mod_feat.process_recommendation_features()

    print("✅ [Task] Camada Gold e Feature Store processadas com sucesso!")


@task(name="03 - Machine Learning (Treinamento ALS e Recomendações Discovery)", retries=1, retry_delay_seconds=10)
def task_machine_learning():
    """Treina o modelo de Filtragem Colaborativa (ALS) e gera o Top 20 de descobertas."""
    print("▶️ [Task] Executando motor de Machine Learning (PySpark MLlib ALS)...")
    
    mod_als = importlib.import_module("elt_spark.03_machine_learning.train_als_model")
    mod_als.run_als_training_and_recommendations()
    
    print("✅ [Task] Recomendações de IA geradas com sucesso!")


@task(name="04 - Serving Layer Sync (Gold -> PostgreSQL)", retries=1, retry_delay_seconds=10)
def task_sync_postgres():
    """Sincroniza as tabelas da camada Gold para o banco PostgreSQL de produção."""
    print("▶️ [Task] Sincronizando tabelas Gold com o PostgreSQL...")
    
    mod_sync = importlib.import_module("elt_spark.04_gold_to_postgres.sync_gold")
    mod_sync.sync_gold_to_postgres()
    
    print("✅ [Task] Carga no PostgreSQL finalizada com sucesso!")


@flow(name="aniMind-Lakehouse-Pipeline", log_prints=True)
def animind_pipeline_flow():
    """
    Fluxo principal orquestrado do AniMind:
    Bronze -> Silver -> Gold (Analytics & ML Features) -> Machine Learning (ALS) -> PostgreSQL
    """
    print("🚀 ========================================================")
    print("🚀 Iniciando Pipeline End-to-End AniMind via Prefect Cloud")
    print("🚀 ========================================================")

    # 0. Sincroniza estado persistente da nuvem
    task_pull_lake()

    # 1. Processa Bronze -> Silver
    task_bronze_to_silver()

    # 2. Processa Silver -> Gold
    task_silver_to_gold()

    # 3. Treina modelo ALS e gera recomendações
    task_machine_learning()

    # 4. Sincroniza tabelas Gold para o PostgreSQL
    task_sync_postgres()

    # 5. Envia camadas atualizadas para o Supabase Storage S3
    task_push_lake()

    # 6. Atualiza índice vetorial RAG incrementalmente (lote diário seguro para o Free Tier)
    task_vector_indexing()

    print("🎉 ========================================================")
    print("🎉 Pipeline AniMind finalizado com sucesso!")
    print("🎉 ========================================================")


if __name__ == "__main__":
    animind_pipeline_flow()

