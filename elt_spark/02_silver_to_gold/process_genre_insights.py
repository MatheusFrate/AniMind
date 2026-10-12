import time
from pathlib import Path
from pyspark.sql import functions as F
from elt_spark.utils.spark_session import get_spark_session


def process_genre_insights():
    print("=" * 70)
    print("🚀 [PySpark ETL] Iniciando transformação Silver -> Gold (Genre Insights)...")
    print("=" * 70)
    start_time = time.time()

    raiz_projeto = Path(__file__).resolve().parent.parent.parent
    caminho_silver = raiz_projeto / "data_lake" / "silver"
    caminho_gold = raiz_projeto / "data_lake" / "gold" / "genre_studio_insights"
    caminho_gold_genre_insights = raiz_projeto / "data_lake" / "gold" / "genre_insights"

    caminho_user_animes = caminho_silver / "user_animes"
    caminho_genres = caminho_silver / "genres"
    caminho_anime_genres = caminho_silver / "anime_genres"

    if not caminho_user_animes.exists():
        print(f"⚠️ [PySpark ETL] Tabela Silver user_animes não encontrada em {caminho_silver}. Execute a etapa Bronze -> Silver primeiro!")
        return

    spark = get_spark_session("AniMind-SilverToGold-GenreInsights")

    try:
        print("📖 [PySpark] Lendo tabelas da camada Silver...")
        df_user_animes = spark.read.parquet(str(caminho_user_animes))

        df_genres = None
        df_anime_genres = None

        if caminho_genres.exists():
            df_genres = spark.read.parquet(str(caminho_genres))
        if caminho_anime_genres.exists():
            df_anime_genres = spark.read.parquet(str(caminho_anime_genres))


        df_genres_clean = df_genres.select(
            F.col("id").alias("genre_id"),
            F.col("name").alias("genre_name")
        )
        df_genres_joined = (
            df_user_animes
            .join(
                df_anime_genres.select("anime_id", "genre_id"), 
                on="anime_id", 
                how="left"
            )
            .join(
                df_genres_clean, 
                on="genre_id", 
                how="left"
            )
        )




        # 1. Agregações das métricas dos generos com base na comunidade AniMind
        print("⚙️ [PySpark] Calculando estatísticas agregadas e engajamento por genero...")

        df_genre_stats = df_genres_joined.groupBy("genre_id", "genre_name").agg(
            F.sum(F.when(F.col("status") != "plan_to_watch", 1).otherwise(0)).alias("total_genre_views"), 
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("total_genre_completed"), 
            F.sum(F.when(F.col("status") == "dropped", 1).otherwise(0)).alias("total_genre_dropped"), 
            F.avg(F.when(F.col("score") >0, F.col("score"))).alias("app_avg_user_mean"),
            F.countDistinct("anime_id").alias("app_avg_user_count"),
            F.coalesce(F.sum("num_episodes_watched"), F.lit(0)).alias("total_episodes_watched")
        )


        df_genre_stats = df_genre_stats.withColumn("calculated_at", F.current_timestamp())

        # Salvar em Parquet na Gold
        caminho_gold.parent.mkdir(parents=True, exist_ok=True)
        df_genre_stats.write.mode("overwrite").parquet(str(caminho_gold_genre_insights))

        total_records = df_genre_stats.count()
        elapsed = round(time.time() - start_time, 2)
        print("=" * 70)
        print(f"✅ [PySpark ETL] Tabela Gold 'genre_studio_insights' salva com sucesso em {caminho_gold_genre_insights}!")
        print(f"📊 Registros salvos: {total_records} animes | Tempo decorrido: {elapsed}s")
        print("=" * 70)

        # Mostra amostra dos dados no terminal
        df_genre_stats.select(
            "genre_id", "genre_name", "total_genre_views", "total_genre_completed",
            "total_genre_dropped", "total_episodes_watched", "calculated_at"
            ).show(5, truncate=False)

    except Exception as e:
        print(f"❌ [PySpark ETL] Erro no processamento da tabela Gold (Anime Metrics): {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    process_genre_insights()
