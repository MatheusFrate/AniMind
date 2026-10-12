import time
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from elt_spark.utils.spark_session import get_spark_session


def process_user_analytics_silver_to_gold():
    print("=" * 70)
    print("🚀 [PySpark ETL] Iniciando transformação Silver -> Gold (User Analytics)...")
    print("=" * 70)
    start_time = time.time()

    raiz_projeto = Path(__file__).resolve().parent.parent.parent
    caminho_silver = raiz_projeto / "data_lake" / "silver"
    caminho_gold = raiz_projeto / "data_lake" / "gold" / "user_analytics"

    caminho_users = caminho_silver / "users"
    caminho_user_animes = caminho_silver / "user_animes"
    caminho_animes = caminho_silver / "animes"
    caminho_anime_genres = caminho_silver / "anime_genres"
    caminho_genres = caminho_silver / "genres"
    caminho_anime_studio = caminho_silver / "anime_studio"
    caminho_studios = caminho_silver / "studios"

    if not caminho_users.exists() or not caminho_user_animes.exists():
        print(f"⚠️ [PySpark ETL] Tabelas Silver de usuários não encontradas em {caminho_silver}. Execute a etapa Bronze -> Silver primeiro!")
        return

    spark = get_spark_session("AniMind-SilverToGold-UserAnalytics")

    try:
        print("📖 [PySpark] Lendo tabelas da camada Silver...")
        df_users = spark.read.parquet(str(caminho_users))
        df_user_animes = spark.read.parquet(str(caminho_user_animes))

        df_animes = None
        if caminho_animes.exists():
            df_animes = spark.read.parquet(str(caminho_animes))

        # Join user_animes com animes para obter a duração média dos episódios
        if df_animes is not None:
            df_joined = df_user_animes.join(
                df_animes.select("id", "average_episode_duration"),
                df_user_animes.anime_id == df_animes.id,
                "left"
            )
        else:
            df_joined = df_user_animes.withColumn("average_episode_duration", F.lit(None))

        # 1. Agregações por Usuário (KPIs gerais)
        print("⚙️ [PySpark] Calculando estatísticas agregadas e KPIs dos usuários...")
        df_stats = df_joined.groupBy("username").agg(
            F.count("anime_id").alias("total_animes"),
            F.sum(F.when(F.col("status") == "watching", 1).otherwise(0)).alias("watching_count"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_count"),
            F.sum(F.when(F.col("status") == "on_hold", 1).otherwise(0)).alias("on_hold_count"),
            F.sum(F.when(F.col("status") == "dropped", 1).otherwise(0)).alias("dropped_count"),
            F.sum(F.when(F.col("status") == "plan_to_watch", 1).otherwise(0)).alias("plan_to_watch_count"),
            F.sum(
                F.when(
                    (F.col("is_rewatching") == True) | (F.col("num_times_rewatched") > 0), 1
                ).otherwise(0)
            ).alias("rewatched_count"),
            F.coalesce(F.sum("num_episodes_watched"), F.lit(0)).alias("total_episodes_watched"),
            F.coalesce(
                F.sum(F.col("num_episodes_watched") * F.col("average_episode_duration")),
                F.lit(0)
            ).alias("total_seconds_watched"),
            F.sum(F.when(F.col("score") > 0, 1).otherwise(0)).alias("scored_animes_count"),
            F.avg(F.when(F.col("score") > 0, F.col("score"))).alias("user_mean_score"),
            F.stddev(F.when(F.col("score") > 0, F.col("score"))).alias("user_score_stddev")
        ).withColumn(
            "total_days_watched",
            F.round(F.col("total_seconds_watched") / 86400, 2)
        ).withColumn(
            "user_mean_score",
            F.round(F.col("user_mean_score"), 2)
        ).withColumn(
            "user_score_stddev",
            F.round(F.col("user_score_stddev"), 2)
        )

        # 2. Gênero Mais Visto vs Gênero Favorito (Nota Média)
        print("⚙️ [PySpark] Identificando gênero mais visto e gênero favorito (melhor nota)...")
        most_watched_genre_df = None
        favorite_genre_df = None

        if caminho_anime_genres.exists() and caminho_genres.exists():
            df_ag = spark.read.parquet(str(caminho_anime_genres))
            df_g = spark.read.parquet(str(caminho_genres))

            df_user_genres = df_user_animes.join(df_ag, "anime_id") \
                                          .join(df_g, df_ag.genre_id == df_g.id) \
                                          .groupBy("username", "name") \
                                          .agg(
                                              F.count("anime_id").alias("genre_count"),
                                              F.avg(F.when(F.col("score") > 0, F.col("score"))).alias("avg_genre_score")
                                          )

            # 2.1 Gênero Mais Visto (maior volume)
            win_genre_cnt = Window.partitionBy("username").orderBy(F.col("genre_count").desc())
            most_watched_genre_df = df_user_genres.withColumn("rn", F.row_number().over(win_genre_cnt)) \
                                                  .filter(F.col("rn") == 1) \
                                                  .select(F.col("username"), F.col("name").alias("most_watched_genre"))

            # 2.2 Gênero Favorito (maior média de nota, mínimo 2 animes avaliados)
            df_fav_g = df_user_genres.filter((F.col("genre_count") >= 2) & F.col("avg_genre_score").isNotNull())
            win_genre_score = Window.partitionBy("username").orderBy(F.col("avg_genre_score").desc(), F.col("genre_count").desc())
            favorite_genre_df = df_fav_g.withColumn("rn", F.row_number().over(win_genre_score)) \
                                        .filter(F.col("rn") == 1) \
                                        .select(F.col("username"), F.col("name").alias("favorite_genre"))

        # 3. Estúdio Mais Visto vs Estúdio Favorito (Nota Média)
        print("⚙️ [PySpark] Identificando estúdio mais visto e estúdio favorito (melhor nota)...")
        most_watched_studio_df = None
        favorite_studio_df = None

        if caminho_anime_studio.exists() and caminho_studios.exists():
            df_ast = spark.read.parquet(str(caminho_anime_studio))
            df_st = spark.read.parquet(str(caminho_studios))

            df_user_studios = df_user_animes.join(df_ast, "anime_id") \
                                            .join(df_st, df_ast.studio_id == df_st.id) \
                                            .groupBy("username", "name") \
                                            .agg(
                                                F.count("anime_id").alias("studio_count"),
                                                F.avg(F.when(F.col("score") > 0, F.col("score"))).alias("avg_studio_score")
                                            )

            # 3.1 Estúdio Mais Visto (maior volume)
            win_studio_cnt = Window.partitionBy("username").orderBy(F.col("studio_count").desc())
            most_watched_studio_df = df_user_studios.withColumn("rn", F.row_number().over(win_studio_cnt)) \
                                                    .filter(F.col("rn") == 1) \
                                                    .select(F.col("username"), F.col("name").alias("most_watched_studio"))

            # 3.2 Estúdio Favorito (maior média de nota, mínimo 2 animes avaliados)
            df_fav_st = df_user_studios.filter((F.col("studio_count") >= 2) & F.col("avg_studio_score").isNotNull())
            win_studio_score = Window.partitionBy("username").orderBy(F.col("avg_studio_score").desc(), F.col("studio_count").desc())
            favorite_studio_df = df_fav_st.withColumn("rn", F.row_number().over(win_studio_score)) \
                                          .filter(F.col("rn") == 1) \
                                          .select(F.col("username"), F.col("name").alias("favorite_studio"))

        # Consolidar tudo na Tabela Gold com Left Joins
        df_gold = df_users.join(df_stats, "username", "left")

        if most_watched_genre_df is not None:
            df_gold = df_gold.join(most_watched_genre_df, "username", "left")
        else:
            df_gold = df_gold.withColumn("most_watched_genre", F.lit(None))

        if favorite_genre_df is not None:
            df_gold = df_gold.join(favorite_genre_df, "username", "left")
        else:
            df_gold = df_gold.withColumn("favorite_genre", F.lit(None))

        if most_watched_studio_df is not None:
            df_gold = df_gold.join(most_watched_studio_df, "username", "left")
        else:
            df_gold = df_gold.withColumn("most_watched_studio", F.lit(None))

        if favorite_studio_df is not None:
            df_gold = df_gold.join(favorite_studio_df, "username", "left")
        else:
            df_gold = df_gold.withColumn("favorite_studio", F.lit(None))

        df_gold = df_gold.withColumn("calculated_at", F.current_timestamp())

        # Salvar em Parquet na Gold
        caminho_gold.parent.mkdir(parents=True, exist_ok=True)
        df_gold.write.mode("overwrite").parquet(str(caminho_gold))

        total_records = df_gold.count()
        elapsed = round(time.time() - start_time, 2)
        print("=" * 70)
        print(f"✅ [PySpark ETL] Tabela Gold 'user_analytics' salva com sucesso em {caminho_gold}!")
        print(f"📊 Registros salvos: {total_records} usuários | Tempo decorrido: {elapsed}s")
        print("=" * 70)

        # Mostra amostra dos dados no terminal
        df_gold.select(
            "username", "total_animes", "total_episodes_watched",
            "total_days_watched", "user_mean_score",
            "most_watched_genre", "favorite_genre",
            "most_watched_studio", "favorite_studio"
        ).show(5, truncate=False)

    except Exception as e:
        print(f"❌ [PySpark ETL] Erro no processamento da tabela Gold (User Analytics): {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    process_user_analytics_silver_to_gold()
