import time
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer
from elt_spark.utils.spark_session import get_spark_session


def process_recommendation_features():
    print("=" * 70)
    print("🚀 [PySpark ETL] Iniciando transformação Silver -> Gold (ML Features)...")
    print("=" * 70)
    start_time = time.time()

    raiz_projeto = Path(__file__).resolve().parent.parent.parent
    caminho_silver = raiz_projeto / "data_lake" / "silver"
    caminho_gold_features = raiz_projeto / "data_lake" / "gold" / "user_anime_features"

    caminho_user_animes = caminho_silver / "user_animes"

    if not caminho_user_animes.exists():
        print(f"⚠️ [PySpark ETL] Tabela Silver user_animes não encontrada em {caminho_silver}.")
        return

    spark = get_spark_session("AniMind-SilverToGold-MLFeatures")

    try:
        print("📖 [PySpark] Lendo tabelas da camada Silver...")
        df_user_animes = spark.read.parquet(str(caminho_user_animes))

        print("⚙️ [PySpark] Tratando notas explícitas e implícitas...")
        # Ignora plan_to_watch pois não reflete consumo real
        df_filtered = df_user_animes.filter(F.col("status") != "plan_to_watch")

        # Regra de negócio: Se não houver nota, infere uma baseada no status
        df_features = df_filtered.withColumn(
            "calculated_score",
            F.when(F.col("score") > 0, F.col("score").cast("double")).otherwise(
                F.when(F.col("status") == "completed", 7.0)
                 .when(F.col("status") == "watching", 6.0)
                 .when(F.col("status") == "on_hold", 5.0)
                 .when(F.col("status") == "dropped", 3.0)
                 .otherwise(None)
            )
        ).withColumn(
            "is_implicit",
            F.when(F.col("score") == 0, F.lit(True)).otherwise(F.lit(False))
        ).filter(F.col("calculated_score").isNotNull())

        print("⚙️ [PySpark] Convertendo 'username' para ID numérico (StringIndexer)...")
        # O modelo ALS do PySpark exige que a coluna de usuário seja numérica (integer)
        indexer = StringIndexer(inputCol="username", outputCol="user_idx", handleInvalid="skip")
        df_features_indexed = indexer.fit(df_features).transform(df_features)

        # Cast para inteiro, pois o StringIndexer retorna double
        df_features_final = df_features_indexed.withColumn("user_idx", F.col("user_idx").cast("integer"))

        # Selecionar apenas o necessário para o modelo e rastreabilidade
        df_ml_dataset = df_features_final.select(
            "user_idx",
            "username",
            "anime_id",
            "calculated_score",
            "is_implicit"
        ).withColumn("calculated_at", F.current_timestamp())

        # Salvar em Parquet na Gold
        caminho_gold_features.parent.mkdir(parents=True, exist_ok=True)
        df_ml_dataset.write.mode("overwrite").parquet(str(caminho_gold_features))

        total_records = df_ml_dataset.count()
        elapsed = round(time.time() - start_time, 2)
        print("=" * 70)
        print(f"✅ [PySpark ETL] Dataset de ML 'user_anime_features' salvo com sucesso em {caminho_gold_features}!")
        print(f"📊 Registros processados: {total_records} interações | Tempo decorrido: {elapsed}s")
        print("=" * 70)

        # Mostra amostra dos dados no terminal
        df_ml_dataset.show(10, truncate=False)

    except Exception as e:
        print(f"❌ [PySpark ETL] Erro no processamento do Dataset de ML: {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    process_recommendation_features()
