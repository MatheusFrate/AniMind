import time
from pathlib import Path
from pyspark.sql import functions as F
from elt_spark.utils.spark_session import get_spark_session


def process_anime_metrics_silver_to_gold():
    print("=" * 70)
    print("🚀 [PySpark ETL] Iniciando transformação Silver -> Gold (Anime Metrics)...")
    print("=" * 70)
    start_time = time.time()

    raiz_projeto = Path(__file__).resolve().parent.parent.parent
    caminho_silver = raiz_projeto / "data_lake" / "silver"
    caminho_gold = raiz_projeto / "data_lake" / "gold" / "anime_metrics"

    caminho_animes = caminho_silver / "animes"
    caminho_user_animes = caminho_silver / "user_animes"

    if not caminho_user_animes.exists():
        print(f"⚠️ [PySpark ETL] Tabela Silver user_animes não encontrada em {caminho_silver}. Execute a etapa Bronze -> Silver primeiro!")
        return

    spark = get_spark_session("AniMind-SilverToGold-AnimeMetrics")

    try:
        print("📖 [PySpark] Lendo tabelas da camada Silver...")
        df_user_animes = spark.read.parquet(str(caminho_user_animes))

        df_animes = None
        if caminho_animes.exists():
            df_animes = spark.read.parquet(str(caminho_animes))

        # 1. Agregações das métricas dos animes com base na comunidade AniMind
        print("⚙️ [PySpark] Calculando estatísticas agregadas e engajamento por anime...")
        df_stats = df_user_animes.groupBy("anime_id").agg(
            F.count("username").alias("total_tracked_users"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_count"),
            F.sum(F.when(F.col("status") == "watching", 1).otherwise(0)).alias("watching_count"),
            F.sum(F.when(F.col("status") == "on_hold", 1).otherwise(0)).alias("on_hold_count"),
            F.sum(F.when(F.col("status") == "dropped", 1).otherwise(0)).alias("dropped_count"),
            F.sum(F.when(F.col("status") == "plan_to_watch", 1).otherwise(0)).alias("plan_to_watch_count"),
            F.sum(F.when(F.col("score") > 0, 1).otherwise(0)).alias("app_user_score_count"),
            F.avg(F.when(F.col("score") > 0, F.col("score"))).alias("app_user_mean")
        )

        # Calculando Taxa de Conclusão (completion_rate) e Taxa de Abandono (drop_rate)
        # completion_rate = completed / (completed + dropped)
        df_stats = df_stats.withColumn(
            "finished_or_dropped",
            F.col("completed_count") + F.col("dropped_count")
        ).withColumn(
            "completion_rate",
            F.when(
                F.col("finished_or_dropped") > 0,
                F.round(F.col("completed_count") / F.col("finished_or_dropped"), 4)
            ).otherwise(0.0)
        ).withColumn(
            "drop_rate",
            F.when(
                F.col("finished_or_dropped") > 0,
                F.round(F.col("dropped_count") / F.col("finished_or_dropped"), 4)
            ).otherwise(0.0)
        ).withColumn(
            "app_user_mean",
            F.round(F.col("app_user_mean"), 2)
        ).drop("finished_or_dropped")

        # 2. Consolidação com os dados de catálogo do Anime (se existir)
        if df_animes is not None:
            print("⚙️ [PySpark] Unindo métricas internas aos metadados do catálogo de animes...")
            df_gold = df_animes.join(
                df_stats,
                df_animes.id == df_stats.anime_id,
                "left"
            ).select(
                F.col("id").alias("anime_id"),
                F.col("title"),
                F.col("main_picture"),
                F.col("media_type"),
                F.col("mean").alias("global_mal_mean"),
                F.col("rank").alias("global_mal_rank"),
                F.col("popularity").alias("global_mal_popularity"),
                F.coalesce(F.col("app_user_mean"), F.lit(None)).alias("app_user_mean"),
                F.coalesce(F.col("app_user_score_count"), F.lit(0)).alias("app_user_score_count"),
                F.coalesce(F.col("total_tracked_users"), F.lit(0)).alias("total_tracked_users"),
                F.coalesce(F.col("completed_count"), F.lit(0)).alias("completed_count"),
                F.coalesce(F.col("watching_count"), F.lit(0)).alias("watching_count"),
                F.coalesce(F.col("dropped_count"), F.lit(0)).alias("dropped_count"),
                F.coalesce(F.col("on_hold_count"), F.lit(0)).alias("on_hold_count"),
                F.coalesce(F.col("plan_to_watch_count"), F.lit(0)).alias("plan_to_watch_count"),
                F.coalesce(F.col("completion_rate"), F.lit(0.0)).alias("completion_rate"),
                F.coalesce(F.col("drop_rate"), F.lit(0.0)).alias("drop_rate")
            )
        else:
            df_gold = df_stats

        df_gold = df_gold.withColumn("calculated_at", F.current_timestamp())

        # Salvar em Parquet na Gold
        caminho_gold.parent.mkdir(parents=True, exist_ok=True)
        df_gold.write.mode("overwrite").parquet(str(caminho_gold))

        total_records = df_gold.count()
        elapsed = round(time.time() - start_time, 2)
        print("=" * 70)
        print(f"✅ [PySpark ETL] Tabela Gold 'anime_metrics' salva com sucesso em {caminho_gold}!")
        print(f"📊 Registros salvos: {total_records} animes | Tempo decorrido: {elapsed}s")
        print("=" * 70)

        # Mostra amostra dos dados no terminal
        df_gold.select(
            "anime_id", "title", "global_mal_mean", "app_user_mean",
            "total_tracked_users", "completion_rate", "drop_rate"
        ).show(5, truncate=False)

    except Exception as e:
        print(f"❌ [PySpark ETL] Erro no processamento da tabela Gold (Anime Metrics): {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    process_anime_metrics_silver_to_gold()
