import os
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from elt_spark.utils.spark_session import get_spark_session


def process_catalog_bronze_to_silver():
    # Inicializa a SparkSession
    spark = get_spark_session("AniMind-BronzeToSilver-Catalog")

    raiz = Path(__file__).resolve().parent.parent.parent
    caminho_bronze = str(raiz / "data_lake" / "bronze" / "catalog")
    caminho_silver = raiz / "data_lake" / "silver"

    df_raw = spark.read.option("multiline", "true").json(caminho_bronze)

    df_node = df_raw.select(F.col("node.*"))
    janela_anime = Window.partitionBy("id").orderBy(F.col("updated_at").desc())

    # Deduplica os animes mantendo a versão com updated_at mais recente
    df_node_deduplicado = (
        df_node
        .withColumn("rn", F.row_number().over(janela_anime))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # 1. Tabela Silver: Animes
    df_animes = df_node_deduplicado.select(
        F.col("id").cast("integer").alias("id"),
        F.col("title").alias("title"),
        F.col("main_picture.medium").alias("main_picture"),
        F.col("alternative_titles.en").alias("alt_title_en"),
        F.col("alternative_titles.ja").alias("alt_title_ja"),
        F.col("start_date").alias("start_date"),
        F.col("end_date").alias("end_date"),
        F.col("mean").cast("double").alias("mean"),
        F.col("rank").cast("integer").alias("rank"),
        F.col("popularity").cast("integer").alias("popularity"),
        F.col("num_list_users").cast("integer").alias("num_list_users"),
        F.col("num_scoring_users").cast("integer").alias("num_scoring_users"),
        F.col("nsfw").alias("nsfw"),
        F.col("created_at").alias("created_at"),
        F.col("updated_at").alias("updated_at"),
        F.col("media_type").alias("media_type"),
        F.col("status").alias("status"),
        F.col("num_episodes").cast("integer").alias("num_episodes"),
        F.col("start_season.season").alias("start_season"),
        F.col("start_season.year").cast("integer").alias("start_season_year"),
        F.col("broadcast.day_of_the_week").alias("broadcast_day_of_the_week"),
        F.col("broadcast.start_time").alias("broadcast_start_time"),
        F.col("source").alias("source"),
        F.col("average_episode_duration").cast("integer").alias("average_episode_duration"),
        F.col("rating").alias("rating"),
        F.col("synopsis").alias("synopsis")
    ).filter(F.col("id").isNotNull())

    df_animes.write.mode("overwrite").parquet(str(caminho_silver / "animes"))
    print("✅ Tabela 'animes' salva com sucesso na Silver!")

    # 2. Tabela Silver: Genres (Dimensão)
    df_genres = df_node_deduplicado.select(
        F.explode("genres").alias("g")).select(
            F.col("g.id").cast("integer").alias("id"),
            F.col("g.name").alias("name")
        ).filter(F.col("id").isNotNull()) \
        .dropDuplicates(["id"])

    df_genres.write.mode("overwrite").parquet(str(caminho_silver / "genres"))
    print("✅ Tabela 'genres' salva com sucesso na Silver!")

    # 3. Tabela Silver: Anime_Genres (Relacionamento N:N)
    df_anime_genres = df_node_deduplicado.select(
        F.col("id").cast("integer").alias("anime_id"),
        F.explode("genres").alias("g")
    ).select(
        F.col("anime_id"),
        F.col("g.id").cast("integer").alias("genre_id")
    ).filter(F.col("anime_id").isNotNull() & F.col("genre_id").isNotNull()) \
     .dropDuplicates(["anime_id", "genre_id"])

    df_anime_genres.write.mode("overwrite").parquet(str(caminho_silver / "anime_genres"))
    print("✅ Tabela 'anime_genres' salva com sucesso na Silver!")

    # 4. Tabela Silver: Studios (Dimensão)
    df_studios = df_node_deduplicado.select( 
        F.explode("studios").alias("s")).select(
            F.col("s.id").cast("integer").alias("id"),
            F.col("s.name").alias("name")
        ).filter(F.col("id").isNotNull()) \
        .dropDuplicates(["id"])
    
    df_studios.write.mode("overwrite").parquet(str(caminho_silver / "studios"))
    print("✅ Tabela 'studios' salva com sucesso na Silver!")

    # 5. Tabela Silver: Anime_Studios (Relacionamento N:N)
    df_anime_studio = df_node_deduplicado.select(
            F.col("id").cast("integer").alias("anime_id"),
            F.explode("studios").alias("s")
        ).select(
            F.col("anime_id"),
            F.col("s.id").cast("integer").alias("studio_id")
        ).filter(F.col("anime_id").isNotNull() & F.col("studio_id").isNotNull()) \
         .dropDuplicates(["anime_id", "studio_id"])
    
    df_anime_studio.write.mode("overwrite").parquet(str(caminho_silver / "anime_studios"))
    print("✅ Tabela 'anime_studios' salva com sucesso na Silver!")
    

    spark.stop()


if __name__ == "__main__":
    process_catalog_bronze_to_silver()