"""
aniMind - Indexador Vetorial Incremental de Animes (Google Gemini Embeddings -> Supabase pgvector)

Lê os animes avaliados e com sinopse válida do banco de dados, gera embeddings
de 768 dimensões em lotes de até 100 documentos via Gemini batchEmbedContents
e armazena de forma incremental na tabela anime_embeddings (pgvector).
"""

import argparse
import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text

RAIZ_PROJETO = Path(__file__).resolve().parent.parent.parent
if str(RAIZ_PROJETO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROJETO))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

load_dotenv(override=True)

from app.database import SessionLocal
from app.services.rag_service import (
    build_anime_document,
    compute_content_hash,
    embed_documents_batch,
    ensure_vector_schema,
    get_gemini_api_key,
)


def run_anime_vector_indexing(max_items: int = 800, batch_size: int = 80, sleep_between_batches: float = 62.0) -> int:
    """
    Executa a indexação vetorial incremental dos animes elegíveis.
    Calibrado para os limites do Free Tier do Gemini Embedding 1:
      - RPM: 80/min (limite 100 RPM)
      - TPM: ~20K/min (limite 30K TPM)
      - RPD: 800/dia padrão (limite 1.000 RPD, reservando 200 RPD para buscas no Dashboard)
    Retorna o total de novos vetores gerados/atualizados nesta execução.
    """
    if not get_gemini_api_key():
        raise RuntimeError(
            "GEMINI_API_KEY não encontrada no .env ou variáveis de ambiente! "
            "Configure sua chave gratuita do Google AI Studio antes de indexar."
        )

    batch_size = max(1, min(100, batch_size))
    db = SessionLocal()
    try:
        print("🔧 [RAG Indexer] Garantindo schema pgvector e índice HNSW no Supabase...")
        ensure_vector_schema(db)

        print("📥 [RAG Indexer] Carregando hashes já indexados em anime_embeddings...")
        existing_rows = db.execute(text("SELECT anime_id, content_hash FROM anime_embeddings")).fetchall()
        existing_hashes = {row[0]: row[1] for row in existing_rows}
        print(f"ℹ️  [RAG Indexer] Animes já vetorizados no banco: {len(existing_hashes)}")

        print("🔍 [RAG Indexer] Buscando animes elegíveis (com nota e sinopse válida), ordenados por popularidade...")
        animes_rows = db.execute(text("""
            SELECT id, title, genres, studio, media_type, source, synopsis
            FROM animes
            WHERE synopsis IS NOT NULL
              AND length(trim(synopsis)) > 40
              AND synopsis NOT LIKE 'No synopsis%'
              AND mean IS NOT NULL
            ORDER BY popularity ASC NULLS LAST, num_scoring_users DESC NULLS LAST
        """)).mappings().all()

        pending = []
        for a in animes_rows:
            doc_text = build_anime_document(a)
            doc_hash = compute_content_hash(doc_text)
            if existing_hashes.get(a["id"]) != doc_hash:
                pending.append({
                    "anime_id": a["id"],
                    "title": a["title"] or "",
                    "text": doc_text,
                    "content_hash": doc_hash,
                })
                if len(pending) >= max_items:
                    break

        total_pending = len(pending)
        if total_pending == 0:
            print("✅ [RAG Indexer] Todos os animes elegíveis já estão vetorizados e atualizados!")
            return 0

        print(f"🚀 [RAG Indexer] Iniciando vetorização de {total_pending} animes em lotes de {batch_size}...")
        indexed_now = 0
        start_time = time.time()

        for i in range(0, total_pending, batch_size):
            batch = pending[i : i + batch_size]
            vectors = embed_documents_batch(batch)

            upsert_params = []
            for item, vec in zip(batch, vectors):
                vec_str = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
                upsert_params.append({
                    "anime_id": item["anime_id"],
                    "content_hash": item["content_hash"],
                    "embedding": vec_str,
                })

            db.execute(
                text("""
                    INSERT INTO anime_embeddings (anime_id, content_hash, embedding, updated_at)
                    VALUES (:anime_id, :content_hash, CAST(:embedding AS vector), NOW())
                    ON CONFLICT (anime_id) DO UPDATE SET
                        content_hash = EXCLUDED.content_hash,
                        embedding = EXCLUDED.embedding,
                        updated_at = NOW()
                """),
                upsert_params,
            )
            db.commit()

            indexed_now += len(batch)
            elapsed = time.time() - start_time
            rate = indexed_now / elapsed if elapsed > 0 else 0
            print(
                f"   ✔️ [{indexed_now}/{total_pending}] vetorizados e salvos "
                f"({(indexed_now / total_pending) * 100:.1f}% | {rate:.1f} animes/s)"
            )

            if i + batch_size < total_pending and sleep_between_batches > 0:
                time.sleep(sleep_between_batches)

        print(f"🎉 [RAG Indexer] Concluído! {indexed_now} animes vetorizados com sucesso em {round(time.time() - start_time, 1)}s.")
        return indexed_now

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Indexador Vetorial de Animes (Gemini Embeddings -> pgvector)")
    parser.add_argument("--limit", type=int, default=800, help="Limite máximo de animes a vetorizar na execução (padrão 800 para respeitar 1K RPD)")
    parser.add_argument("--batch-size", type=int, default=80, help="Tamanho do lote por minuto (padrão 80 para ficar abaixo de 100 RPM e 30K TPM)")
    parser.add_argument("--sleep", type=float, default=62.0, help="Pausa em segundos entre lotes (padrão 62s para janela de 1 minuto)")
    args = parser.parse_args()

    run_anime_vector_indexing(max_items=args.limit, batch_size=args.batch_size, sleep_between_batches=args.sleep)
