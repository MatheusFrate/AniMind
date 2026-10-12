# 🧠 AniMind — Anime Data Platform

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-MLlib-E25A1C?logo=apachespark&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

Plataforma de dados **end-to-end** sobre animes: coleta dados do MyAnimeList, processa em um **Data Lakehouse com arquitetura medalhão** (Bronze → Silver → Gold) usando **PySpark**, treina um **sistema de recomendação (ALS)** e serve tudo via **FastAPI + PostgreSQL** em um dashboard interativo.

## 🏗️ Arquitetura

```mermaid
flowchart LR
    MAL["MyAnimeList API"] --> EXT["Extractors<br/>(catalog / relations / user lists)"]
    RSS["RSS Feed"] --> RSSX["rss_extractor"]
    EXT --> B["🥉 Bronze<br/>JSON bruto"]
    B --> S["🥈 Silver<br/>Parquet normalizado"]
    S --> G["🥇 Gold<br/>Analytics + Feature Store"]
    G --> ML["🤖 ALS (Spark MLlib)"]
    ML --> G
    G --> PG[("PostgreSQL")]
    RSSX --> PG
    PG --> API["FastAPI"]
    API --> UI["Dashboard"]
```

## ✨ Destaques de Engenharia

- **Arquitetura medalhão** com PySpark: deduplicação por `updated_at`, normalização N:N (gêneros, estúdios) e agregações analíticas.
- **Feature Store** para ML: indexação de usuários (`StringIndexer`) e ratings implícitos inferidos a partir do status (completed / dropped / on-hold).
- **Recomendação "Discovery"** com ALS (filtragem colaborativa):
  - remove animes já assistidos (`left_anti` join);
  - bane franquias inteiras já conhecidas expandindo o grafo de relações em profundidade (sequel → sequel → ...);
  - recomenda apenas **pontos de entrada** (exclui obras com `prequel` / `parent_story`).
- **Serving layer**: tabelas Gold sincronizadas para o PostgreSQL (full overwrite) e expostas via FastAPI.
- **Tudo containerizado** com Docker Compose (API, banco, extratores e jobs Spark).

## 📁 Estrutura

```text
aniMind/
├── app/                    # Serving: FastAPI, modelos SQLAlchemy, dashboard (static/)
├── extractors/             # Ingestão de dados
│   ├── catalog_extractor/  # Catálogo de animes do MAL → Bronze
│   ├── relation_collector/ # Grafo de relações (sequel, prequel, ...) → Bronze
│   └── rss_extractor/      # Notícias via RSS → PostgreSQL
├── elt_spark/
│   ├── 01_bronze_to_silver/
│   ├── 02_silver_to_gold/
│   ├── 03_machine_learning/   # Treino ALS + geração de recomendações
│   ├── 04_gold_to_postgres/   # Carga da Gold no PostgreSQL
│   └── utils/
├── data_lake/              # bronze/ silver/ gold/ (dados não versionados)
├── docker-compose.yml
└── Dockerfile.*
```

## 🚀 Como executar

```bash
# 1. Configure as variáveis (Client ID do MyAnimeList)
cp .env.example .env

# 2. Suba o ecossistema
docker compose up -d --build

# 3. Rode o pipeline (exemplo manual)
python -m elt_spark.01_bronze_to_silver.process_catalog
python elt_spark/03_machine_learning/train_als_model.py
python elt_spark/04_gold_to_postgres/sync_gold.py
```

Acesse o dashboard em **http://localhost:8000** e a documentação da API em **http://localhost:8000/docs**.

## 📸 Screenshots

> _Adicione aqui prints do dashboard em `docs/`._