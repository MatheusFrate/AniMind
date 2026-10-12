import time
from pathlib import Path
from pyspark.sql import functions as F
from elt_spark.utils.spark_session import get_spark_session


def process_studio_insights():
    print("=" * 70)
    print("🚀 [PySpark ETL] Iniciando transformação Silver -> Gold (Studio Insights)...")
    print("=" * 70)
    start_time = time.time()

    raiz_projeto = Path(__file__).resolve().parent.parent.parent
    caminho_silver = raiz_projeto / "data_lake" / "silver"
    caminho_gold = raiz_projeto / "data_lake" / "gold" / "genre_studio_insights"
    caminho_gold_studio_insights = raiz_projeto / "data_lake" / "gold" / "studio_insights"

    caminho_user_animes = caminho_silver / "user_animes"
 
    caminho_studios = caminho_silver / "studios"
    caminho_anime_studios = caminho_silver / "anime_studios"

    if not caminho_user_animes.exists():
        print(f"⚠️ [PySpark ETL] Tabela Silver user_animes não encontrada em {caminho_silver}. Execute a etapa Bronze -> Silver primeiro!")
        return

    spark = get_spark_session("AniMind-SilverToGold-StudioInsights")

    try:
        print("📖 [PySpark] Lendo tabelas da camada Silver...")
        df_user_animes = spark.read.parquet(str(caminho_user_animes))

        df_studios = None
        df_anime_studios = None

        if caminho_studios.exists():
            df_studios = spark.read.parquet(str(caminho_studios))
        if caminho_anime_studios.exists():
            df_anime_studios = spark.read.parquet(str(caminho_anime_studios))



        df_studios_clean = df_studios.select(
            F.col("id").alias("studio_id"),
            F.col("name").alias("studio_name")
        )
        df_studio_joined = (
            df_user_animes
            .join(
                df_anime_studios.select("anime_id", "studio_id"), 
                on="anime_id", 
                how="left"
            )
            .join(
                df_studios_clean, 
                on="studio_id", 
                how="left"
            )
        )


        # 1. Agregações das métricas dos animes com base na comunidade AniMind
        print("⚙️ [PySpark] Calculando estatísticas agregadas e engajamento por anime...")

        df_studio_stats = df_studio_joined.groupBy("studio_id", "studio_name").agg(
            F.sum(F.when(F.col("status") != "plan_to_watch", 1).otherwise(0)).alias("total_studio_views"), 
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("total_studio_completed"), 
            F.sum(F.when(F.col("status") == "dropped", 1).otherwise(0)).alias("total_studio_dropped"), 
            F.coalesce(F.sum("num_episodes_watched"), F.lit(0)).alias("total_episodes_watched"),
            F.avg(F.when(F.col("score") >0, F.col("score"))).alias("app_avg_user_mean"),
            F.countDistinct("anime_id").alias("total_catalog_animes")
        )

    
        # 2. Consolidação com os dados de catálogo do Anime (se existir)
        df_studio_stats = df_studio_stats.withColumn("calculated_at", F.current_timestamp())

        # Salvar em Parquet na Gold
        caminho_gold.parent.mkdir(parents=True, exist_ok=True)
        df_studio_stats.write.mode("overwrite").parquet(str(caminho_gold_studio_insights))

        total_records = df_studio_stats.count()
        elapsed = round(time.time() - start_time, 2)
        print("=" * 70)
        print(f"✅ [PySpark ETL] Tabela Gold 'studio_insights' salva com sucesso em {caminho_gold_studio_insights}!")
        print(f"📊 Registros salvos: {total_records} animes | Tempo decorrido: {elapsed}s")
        print("=" * 70)

        # Mostra amostra dos dados no terminal
        df_studio_stats.select(
            "studio_id", "studio_name", "total_studio_views", "total_studio_completed",
            "total_studio_dropped", "total_episodes_watched", "calculated_at"
            ).show(5, truncate=False)

    except Exception as e:
        print(f"❌ [PySpark ETL] Erro no processamento da tabela Gold (Anime Metrics): {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    process_studio_insights()
