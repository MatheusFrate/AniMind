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

    # 1. Processa Bronze -> Silver
    task_bronze_to_silver()

    # 2. Processa Silver -> Gold
    task_silver_to_gold()

    # 3. Treina modelo ALS e gera recomendações
    task_machine_learning()

    # 4. Sincroniza tabelas Gold para o PostgreSQL
    task_sync_postgres()

    print("🎉 ========================================================")
    print("🎉 Pipeline AniMind finalizado com sucesso!")
    print("🎉 ========================================================")


if __name__ == "__main__":
    animind_pipeline_flow()
