import time
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from elt_spark.utils.spark_session import get_spark_session


def run_als_training_and_recommendations():
    print("=" * 70)
    print("🧠 [PySpark MLlib] Iniciando Treinamento e Geração de Recomendações (ALS)...")
    print("=" * 70)
    start_time = time.time()

    raiz_projeto = Path(__file__).resolve().parent.parent.parent
    caminho_gold_features = raiz_projeto / "data_lake" / "gold" / "user_anime_features"
    caminho_silver_animes = raiz_projeto / "data_lake" / "silver" / "animes"
    caminho_gold_recs = raiz_projeto / "data_lake" / "gold" / "recommendations"

    if not caminho_gold_features.exists():
        print(f"⚠️ [PySpark MLlib] Features de ML não encontradas em {caminho_gold_features}.")
        return

    spark = get_spark_session("AniMind-ML-ALSRecommender")
    
    # [CORREÇÃO] Habilita Checkpointing para evitar java.lang.StackOverflowError no ALS
    caminho_checkpoint = raiz_projeto / "data_lake" / "checkpoints"
    caminho_checkpoint.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(caminho_checkpoint))

    try:
        # 1. Carregar os Dados de Treino (Camada Gold)
        print("📖 [PySpark] Lendo feature store (user_anime_features)...")
        features_df = spark.read.parquet(str(caminho_gold_features))

        total_features = features_df.count()
        if total_features == 0:
            print("⚠️ [PySpark] O dataset de features está vazio!")
            return
        
        print(f"📊 [PySpark] Dataset carregado com {total_features} interações (explícitas + implícitas).")

        # 2. Configurar e Treinar o Modelo ALS
        print("⚙️ [PySpark] Treinando modelo ALS de Filtragem Colaborativa...")
        als = ALS(
            maxIter=15,
            regParam=0.1,
            userCol="user_idx",
            itemCol="anime_id",
            ratingCol="calculated_score",
            coldStartStrategy="drop",  # Evita prever NaNs para usuários sem histórico suficiente
            nonnegative=True,
            checkpointInterval=5  # [CORREÇÃO] Salva o estado a cada 5 iterações para limpar o grafo de execução
        )
        model = als.fit(features_df)

        # 3. Gerar candidatos a recomendação para TODOS os usuários
        print("🔮 [PySpark] Gerando pool de recomendações para cada usuário...")
        # Recomendamos 5000 itens para garantir que, após o filtro dos já vistos, sobrem boas opções
        user_recs = model.recommendForAllUsers(5000)

        # 4. Tratar e Desaninhar os resultados
        exploded_recs = user_recs.select(
            F.col("user_idx"),
            F.explode("recommendations").alias("rec")
        ).select(
            F.col("user_idx"),
            F.col("rec.anime_id").alias("anime_id"),
            F.round(F.col("rec.rating"), 2).alias("predicted_score")
        )

        # 5. Filtrar os animes que o usuário JÁ ASSISTIU e as FRANQUIAS/CONTINUAÇÕES
        print("🛡️ [PySpark] Removendo os animes já assistidos e suas continuações/spin-offs...")
        caminho_silver_relations = raiz_projeto / "data_lake" / "silver" / "anime_relations"
        
        watched_df = features_df.select("user_idx", "anime_id")
        
        # 5.1 Busca os animes relacionados aos que o usuário já viu (Com expansão de franquia)
        if caminho_silver_relations.exists():
            print("🔗 [PySpark] Tabela de relações encontrada. Banindo franquias completas em profundidade...")
            relations_df = spark.read.parquet(str(caminho_silver_relations))
            
            all_to_exclude = watched_df
            current_layer = watched_df
            
            # Vamos aprofundar 4 níveis na árvore de relacionamentos (S1 -> S2 -> S3 -> S4 -> Filme)
            for nivel in range(1, 5):
                next_layer = current_layer.join(
                    relations_df,
                    on="anime_id",
                    how="inner"
                ).select(
                    "user_idx", 
                    F.col("related_anime_id").alias("anime_id")
                ).dropDuplicates(["user_idx", "anime_id"])
                
                # Junta o que encontramos nesse nível na lista global de exclusão
                all_to_exclude = all_to_exclude.unionByName(next_layer).dropDuplicates(["user_idx", "anime_id"])
                
                # A próxima busca parte dos animes que acabamos de encontrar
                current_layer = next_layer
        else:
            all_to_exclude = watched_df.dropDuplicates(["user_idx", "anime_id"])

        # 5.2 Anti-Join (Apenas universos completamente novos sobrevivem)
        unseen_recs = exploded_recs.join(
            all_to_exclude,
            on=["user_idx", "anime_id"],
            how="left_anti"
        )
        
        # 5.3 Filtro Global de Pontos de Entrada (Banir tudo que for continuação/spin-off)
        if caminho_silver_relations.exists():
            print("🚪 [PySpark] Mantendo apenas 'Pontos de Entrada' (removendo sequências e spin-offs globais)...")
            # Se um anime tem um prequel ou parent_story, ele NÃO é o ponto de partida.
            non_entry_points = relations_df.filter(
                F.col("relation_type").isin("prequel", "parent_story")
            ).select("anime_id").dropDuplicates()
            
            unseen_recs = unseen_recs.join(
                non_entry_points,
                on="anime_id",
                how="left_anti"
            )

        # 6. Mapear de volta de 'user_idx' para 'username'
        print("🔄 [PySpark] Mapeando identificadores numéricos de volta para usernames...")
        user_mapping = features_df.select("user_idx", "username").dropDuplicates()
        recs_with_username = unseen_recs.join(user_mapping, on="user_idx", how="inner")

        # 7. Selecionar o TOP 20 de recomendações finais para cada usuário
        print("🏆 [PySpark] Selecionando o TOP 20 Recomendações por usuário...")
        window_spec = Window.partitionBy("username").orderBy(F.col("predicted_score").desc())
        top20_recs = recs_with_username.withColumn("rank", F.row_number().over(window_spec)) \
                                       .filter(F.col("rank") <= 20)

        # 8. Enriquecer o resultado com os Metadados dos Animes (Silver)
        if caminho_silver_animes.exists():
            print("🎨 [PySpark] Enriquecendo recomendações com títulos e imagens dos animes...")
            animes_df = spark.read.parquet(str(caminho_silver_animes)).select(
                F.col("id").alias("anime_id"),
                "title",
                "main_picture",
                "mean"
            )
            final_recs = top20_recs.join(animes_df, on="anime_id", how="left")
        else:
            final_recs = top20_recs

        # Organizar as colunas finais
        final_recs = final_recs.select(
            "username",
            "rank",
            "anime_id",
            "title",
            "main_picture",
            F.col("mean").alias("global_mal_mean"),
            "predicted_score"
        ).orderBy("username", "rank")

        # 9. Salvar na Camada Gold
        print(f"💾 [PySpark] Salvando {final_recs.count()} recomendações na camada Gold...")
        caminho_gold_recs.parent.mkdir(parents=True, exist_ok=True)
        final_recs.write.mode("overwrite").parquet(str(caminho_gold_recs))

        elapsed = round(time.time() - start_time, 2)
        print("=" * 70)
        print(f"✅ [PySpark MLlib] Modelo treinado e recomendações salvas em: {caminho_gold_recs}")
        print(f"⏱️  Tempo total de execução: {elapsed}s")
        print("=" * 70)

        # Mostra amostra no terminal
        print("\n🔎 Amostra das Recomendações Geradas:")
        final_recs.show(10, truncate=False)

    except Exception as e:
        print(f"❌ [PySpark MLlib] Erro no pipeline de Machine Learning: {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    run_als_training_and_recommendations()
