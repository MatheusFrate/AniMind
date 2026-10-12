import os
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from elt_spark.utils.spark_session import get_spark_session


def process_users_bronze_to_silver():
    print("=" * 70)
    print("🚀 [PySpark ETL] Iniciando transformação Bronze -> Silver (Usuários & Listas)...")
    print("=" * 70)
    start_time = time.time()

    raiz_projeto = Path(__file__).resolve().parent.parent.parent
    caminho_bronze = str(raiz_projeto / "data_lake" / "bronze" / "users")
    caminho_silver = raiz_projeto / "data_lake" / "silver"

    if not Path(caminho_bronze).exists() or not any(Path(caminho_bronze).glob("*.json")):
        print(f"⚠️ [PySpark ETL] Nenhum arquivo JSON encontrado em {caminho_bronze}. Sincronize um usuário via API primeiro!")
        return

    spark = get_spark_session("AniMind-BronzeToSilver-Users")

    try:
        # 1. Lê todos os JSONs da pasta Bronze de Usuários
        print(f"📖 [PySpark] Lendo arquivos Bronze em: {caminho_bronze}")
        df_raw = spark.read.option("multiline", "true").json(caminho_bronze)

        # 2. Tabela Silver: Users (Perfil)
        print("⚙️ [PySpark] Processando tabela Silver: Users...")
        janela_user = Window.partitionBy("username").orderBy(F.col("last_synced_at").desc())

        df_users = df_raw.select(
            F.col("username").alias("username"),
            F.col("synced_at").alias("last_synced_at"),
            F.col("profile.picture").alias("picture"),
            F.col("profile.gender").alias("gender"),
            F.col("profile.joined_at").alias("joined_at")
        ).withColumn("rn", F.row_number().over(janela_user)) \
         .filter(F.col("rn") == 1) \
         .drop("rn")

        caminho_silver_users = caminho_silver / "users"
        df_users.write.mode("overwrite").parquet(str(caminho_silver_users))
        print(f"✅ [PySpark] Tabela Users salva em: {caminho_silver_users} ({df_users.count()} registros)")

        # 3. Tabela Silver: User_Animes (Biblioteca/Notas dos Usuários)
        print("⚙️ [PySpark] Processando tabela Silver: User_Animes...")
        janela_user_anime = Window.partitionBy("username", "anime_id").orderBy(F.col("synced_at").desc())

        df_user_animes = df_raw.select(
            F.col("username").alias("username"),
            F.col("synced_at").alias("synced_at"),
            F.explode("data").alias("item")
        ).select(
            F.col("username"),
            F.col("item.node.id").cast("integer").alias("anime_id"),
            F.col("item.list_status.status").alias("status"),
            F.col("item.list_status.score").cast("integer").alias("score"),
            F.col("item.list_status.num_episodes_watched").cast("integer").alias("num_episodes_watched"),
            F.col("item.list_status.is_rewatching").cast("boolean").alias("is_rewatching"),
            F.col("item.list_status.num_times_rewatched").cast("integer").alias("num_times_rewatched"),
            F.col("item.list_status.start_date").alias("start_date"),
            F.col("item.list_status.finish_date").alias("finish_date"),
            F.col("synced_at")
        ).filter(F.col("username").isNotNull() & F.col("anime_id").isNotNull()) \
         .withColumn("rn", F.row_number().over(janela_user_anime)) \
         .filter(F.col("rn") == 1) \
         .drop("rn", "synced_at")

        caminho_silver_user_animes = caminho_silver / "user_animes"
        df_user_animes.write.mode("overwrite").parquet(str(caminho_silver_user_animes))
        print(f"✅ [PySpark] Tabela User_Animes salva em: {caminho_silver_user_animes} ({df_user_animes.count()} registros)")

        elapsed = round(time.time() - start_time, 2)
        print("=" * 70)
        print(f"🎉 [PySpark ETL] Camada Silver de Usuários concluída com sucesso em {elapsed}s!")
        print("=" * 70)

    except Exception as e:
        print(f"❌ [PySpark ETL] Erro no processamento de usuários: {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    process_users_bronze_to_silver()
