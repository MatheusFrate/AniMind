import os
import sys
from pathlib import Path

from pyspark.sql import functions as F

from elt_spark.utils.spark_session import get_spark_session


def process_relations_bronze_to_silver():
    # Inicializa a SparkSession
    spark = get_spark_session("AniMind-BronzeToSilver-Relations")

    raiz = Path(__file__).resolve().parent.parent.parent
    caminho_bronze = str(raiz / "data_lake" / "bronze" / "relations")
    caminho_silver = raiz / "data_lake" / "silver" / "anime_relations"

    # 1. Lê todos os lotes JSON da pasta Bronze de Relações
    df_raw = spark.read.option("multiline", "true").json(caminho_bronze)

    # 2. Tratamento, Tipagem e Deduplicação
    # Nota: Os JSONs de relações JÁ vêm planos (sem o objeto 'node' do catálogo)
    df_anime_relations = (
        df_raw.select(
            F.col("anime_id").cast("integer").alias("anime_id"),
            F.col("related_anime_id").cast("integer").alias("related_anime_id"),
            F.lower(F.trim(F.col("relation_type"))).alias("relation_type")
        )
        # Filtra nulos e auto-relacionamentos inválidos (caso a API do MAL devolva anime_id == related_anime_id)
        .filter(
            F.col("anime_id").isNotNull() &
            F.col("related_anime_id").isNotNull() &
            F.col("relation_type").isNotNull() &
            (F.col("anime_id") != F.col("related_anime_id"))
        )
        # Deduplica tuplas idênticas entre diferentes lotes/execuções
        .dropDuplicates(["anime_id", "related_anime_id", "relation_type"])
    )

    # 3. Salva em formato Parquet na Silver
    df_anime_relations.write.mode("overwrite").parquet(str(caminho_silver))
    
    total = df_anime_relations.count()
    print(f"✅ Tabela 'anime_relations' salva com sucesso na Silver ({total} registros)!")

    spark.stop()


if __name__ == "__main__":
    process_relations_bronze_to_silver()
