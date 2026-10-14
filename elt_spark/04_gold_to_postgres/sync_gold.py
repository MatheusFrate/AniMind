import os
import sys
import time
from pathlib import Path
import pandas as pd
from sqlalchemy import text

# Adiciona a raiz ao sys.path para importar a conexao da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from app.database import engine


def sync_catalog_silver_to_postgres(conn, caminho_silver: Path):
    """
    Sincroniza as tabelas do catalogo mestre (Silver -> PostgreSQL):
    genres, animes, anime_genres e anime_relations.
    Utiliza tabela temporaria + UPSERT (ON CONFLICT) para preservar
    integridade referencial com tabelas transacionais de usuarios.
    """
    print("\n[CATALOG] Sincronizando Catalogo Mestre (Silver -> Postgres)...")

    # 1. Sincroniza GENRES
    caminho_genres = caminho_silver / "genres"
    if caminho_genres.exists():
        df_genres = pd.read_parquet(caminho_genres)[["id", "name"]].dropna().drop_duplicates(["id"])
        if len(df_genres) > 0:
            print(f"[INFO] Salvando {len(df_genres)} generos...")
            df_genres.to_sql("temp_genres", con=conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO genres (id, name)
                SELECT id, name FROM temp_genres
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
                DROP TABLE temp_genres;
            """))
            print("[OK] Tabela 'genres' atualizada com sucesso!")

    # 2. Sincroniza ANIMES
    caminho_animes = caminho_silver / "animes"
    if caminho_animes.exists():
        df_animes = pd.read_parquet(caminho_animes)
        animes_cols = [
            "id", "title", "main_picture", "mean", "rank", "popularity",
            "num_episodes", "average_episode_duration", "source",
            "media_type", "synopsis", "num_scoring_users"
        ]
        valid_cols = [c for c in animes_cols if c in df_animes.columns]
        df_animes = df_animes[valid_cols].drop_duplicates(["id"])
        if len(df_animes) > 0:
            print(f"[INFO] Salvando {len(df_animes)} animes do catalogo...")
            df_animes.to_sql("temp_animes", con=conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO animes (id, title, main_picture, mean, rank, popularity, num_episodes, average_episode_duration, source, media_type, synopsis, num_scoring_users)
                SELECT id, title, main_picture, mean, rank, popularity, num_episodes, average_episode_duration, source, media_type, synopsis, num_scoring_users
                FROM temp_animes
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    main_picture = EXCLUDED.main_picture,
                    mean = EXCLUDED.mean,
                    rank = EXCLUDED.rank,
                    popularity = EXCLUDED.popularity,
                    num_episodes = EXCLUDED.num_episodes,
                    average_episode_duration = EXCLUDED.average_episode_duration,
                    source = EXCLUDED.source,
                    media_type = EXCLUDED.media_type,
                    synopsis = EXCLUDED.synopsis,
                    num_scoring_users = EXCLUDED.num_scoring_users;
                DROP TABLE temp_animes;
            """))
            print("[OK] Tabela 'animes' atualizada com sucesso!")

    # 3. Sincroniza ANIME_GENRES
    caminho_ag = caminho_silver / "anime_genres"
    if caminho_ag.exists():
        df_ag = pd.read_parquet(caminho_ag)[["anime_id", "genre_id"]].dropna().drop_duplicates(["anime_id", "genre_id"])
        if len(df_ag) > 0:
            print(f"[INFO] Salvando {len(df_ag)} associacoes anime_genres...")
            df_ag.to_sql("temp_anime_genres", con=conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO anime_genres (anime_id, genre_id)
                SELECT tg.anime_id, tg.genre_id
                FROM temp_anime_genres tg
                JOIN animes a ON tg.anime_id = a.id
                JOIN genres g ON tg.genre_id = g.id
                ON CONFLICT (anime_id, genre_id) DO NOTHING;
                DROP TABLE temp_anime_genres;
            """))
            print("[OK] Tabela 'anime_genres' atualizada com sucesso!")

    # 4. Sincroniza ANIME_RELATIONS
    caminho_rel = caminho_silver / "anime_relations"
    if caminho_rel.exists():
        df_rel = pd.read_parquet(caminho_rel)[["anime_id", "related_anime_id", "relation_type"]].dropna().drop_duplicates(["anime_id", "related_anime_id"])
        if len(df_rel) > 0:
            print(f"[INFO] Salvando {len(df_rel)} relacoes anime_relations...")
            df_rel.to_sql("temp_anime_relations", con=conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO anime_relations (anime_id, related_anime_id, relation_type)
                SELECT tr.anime_id, tr.related_anime_id, tr.relation_type
                FROM temp_anime_relations tr
                JOIN animes a1 ON tr.anime_id = a1.id
                JOIN animes a2 ON tr.related_anime_id = a2.id
                ON CONFLICT (anime_id, related_anime_id) DO UPDATE SET relation_type = EXCLUDED.relation_type;
                DROP TABLE temp_anime_relations;
            """))
            print("[OK] Tabela 'anime_relations' atualizada com sucesso!")


def sync_gold_to_postgres():
    print("=" * 70)
    print("[SYNC] Iniciando sincronizacao Lakehouse -> PostgreSQL (Supabase)...")
    print("=" * 70)
    start_time = time.time()

    raiz_projeto = Path(__file__).resolve().parent.parent.parent
    caminho_silver = raiz_projeto / "data_lake" / "silver"
    caminho_gold = raiz_projeto / "data_lake" / "gold"

    # Mapeamento Gold
    tabelas_gold = {
        "user_analytics": "gold_user_analytics",
        "genre_insights": "gold_genre_insights",
        "studio_insights": "gold_studio_insights",
        "anime_metrics": "gold_anime_metrics",
        "recommendations": "gold_recommendations"
    }

    try:
        with engine.begin() as conn:
            # 1. Catalogo Mestre (Silver -> Postgres)
            if caminho_silver.exists():
                sync_catalog_silver_to_postgres(conn, caminho_silver)

            # 2. Camada Analitica (Gold -> Postgres)
            print("\n[ANALYTICS] Sincronizando Camada Gold...")
            for pasta_parquet, nome_tabela in tabelas_gold.items():
                caminho_parquet = caminho_gold / pasta_parquet
                
                if not caminho_parquet.exists():
                    print(f"[WARN] Pasta {pasta_parquet} nao encontrada. Ignorando...")
                    continue
                
                df = pd.read_parquet(caminho_parquet)
                total_linhas = len(df)
                if total_linhas == 0:
                    continue
                
                print(f"[INFO] Salvando {total_linhas} registros na tabela '{nome_tabela}'...")
                df.to_sql(name=nome_tabela, con=conn, if_exists="replace", index=False)
                print(f"[OK] Tabela '{nome_tabela}' atualizada com sucesso!")

        elapsed = round(time.time() - start_time, 2)
        print("=" * 70)
        print(f"[OK] Sincronizacao concluida em {elapsed}s!")
        print("Agora o PostgreSQL possui tanto o catalogo mestre quanto as tabelas gold_*!")
        print("=" * 70)

    except Exception as e:
        print(f"[ERROR] Erro critico ao enviar dados para o banco: {e}")
        raise e



if __name__ == "__main__":
    sync_gold_to_postgres()
