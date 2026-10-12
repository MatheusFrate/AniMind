import os
import sys
import time
from pathlib import Path
import pandas as pd
from sqlalchemy import text

# Adiciona a raiz ao sys.path para importar a conexão da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from app.database import engine

def sync_gold_to_postgres():
    print("=" * 70)
    print("🐘 [Sync] Iniciando transferência da camada Gold para o PostgreSQL...")
    print("=" * 70)
    start_time = time.time()

    raiz_projeto = Path(__file__).resolve().parent.parent.parent
    caminho_gold = raiz_projeto / "data_lake" / "gold"

    # Mapeamento: "Nome da pasta Parquet" -> "Nome da tabela no Postgres"
    tabelas_gold = {
        "user_analytics": "gold_user_analytics",
        "genre_insights": "gold_genre_insights",
        "studio_insights": "gold_studio_insights",
        "anime_metrics": "gold_anime_metrics",
        "recommendations": "gold_recommendations"
    }

    try:
        # Usa a mesma connection string da sua API
        with engine.begin() as conn:
            for pasta_parquet, nome_tabela in tabelas_gold.items():
                caminho_parquet = caminho_gold / pasta_parquet
                
                if not caminho_parquet.exists():
                    print(f"⚠️ [Sync] Pasta {pasta_parquet} não encontrada. Ignorando...")
                    continue
                
                print(f"📖 Lendo Parquet '{pasta_parquet}'...")
                # Lê a pasta inteira de arquivos parquet gerados pelo Spark
                df = pd.read_parquet(caminho_parquet)
                
                total_linhas = len(df)
                if total_linhas == 0:
                    print(f"⚠️ [Sync] Tabela {pasta_parquet} está vazia. Ignorando...")
                    continue
                
                print(f"💾 Salvando {total_linhas} registros na tabela '{nome_tabela}' no Postgres...")
                # Salva no banco de dados (if_exists="replace" recria a tabela do zero com o schema correto)
                df.to_sql(name=nome_tabela, con=conn, if_exists="replace", index=False)
                
                print(f"✅ Tabela '{nome_tabela}' atualizada com sucesso!")

        elapsed = round(time.time() - start_time, 2)
        print("=" * 70)
        print(f"🎉 [Sync] Sincronização concluída em {elapsed}s!")
        print("Agora a sua API FastAPI já consegue ler as tabelas gold_* nativamente!")
        print("=" * 70)

    except Exception as e:
        print(f"❌ [Sync] Erro crítico ao enviar dados para o banco: {e}")
        raise e

if __name__ == "__main__":
    sync_gold_to_postgres()
