"""
aniMind - S3 Cloud Lakehouse Storage Sync (Supabase Storage)
Permite que o Lakehouse rode localmente ou em GitHub Actions de forma ultra-rápida,
sincronizando o estado persistente do Data Lake com o bucket S3 do Supabase Storage.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(override=True)

RAIZ_PROJETO = Path(__file__).resolve().parent.parent
LOCAL_LAKE_DIR = RAIZ_PROJETO / "data_lake"

def get_s3_client():
    endpoint = os.getenv("SUPA_LAKE_ENDPOINT")
    access_key = os.getenv("SUPA_LAKE_ACCESS_KEY")
    secret_key = os.getenv("SUPA_LAKE_SECRET_KEY")
    region = os.getenv("SUPA_LAKE_REGION", "us-west-2")

    if not endpoint or not access_key or not secret_key:
        return None, None

    import boto3
    bucket_name = os.getenv("SUPA_LAKE_BUCKET", "animind-lake")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )
    return s3, bucket_name


def sync_lake_from_cloud(prefix: str = "") -> int:
    """
    Baixa os arquivos do bucket S3 (Supabase Storage) para o data_lake local.
    Retorna o número de arquivos baixados.
    """
    s3, bucket = get_s3_client()
    if not s3:
        print("[INFO] [Lake Sync] Credenciais S3 do Supabase nao configuradas. Usando data_lake local.")
        return 0

    print(f"[SYNC] [Lake Sync] Sincronizando Cloud S3 ({bucket}/{prefix}) -> Local data_lake...")
    downloaded_count = 0
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix.strip("/") + "/"

    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            s3_key = obj["Key"]
            if s3_key.endswith("/"):
                continue  # ignora diretorios virtuais

            local_file_path = LOCAL_LAKE_DIR / s3_key
            local_file_path.parent.mkdir(parents=True, exist_ok=True)

            # Baixa se o arquivo local nao existir ou se o tamanho for diferente
            if not local_file_path.exists() or local_file_path.stat().st_size != obj["Size"]:
                try:
                    s3.download_file(bucket, s3_key, str(local_file_path))
                    downloaded_count += 1
                except Exception as e:
                    print(f"[WARN] Erro ao baixar {s3_key}: {e}")

    print(f"[OK] [Lake Sync] Download concluido! {downloaded_count} arquivos atualizados.")
    return downloaded_count


def sync_lake_to_cloud(prefix: str = "") -> int:
    """
    Envia arquivos novos ou alterados do data_lake local para o bucket S3 (Supabase Storage).
    Ignora checkpoints temporarios do Spark e arquivos ocultos.
    """
    s3, bucket = get_s3_client()
    if not s3:
        print("[INFO] [Lake Sync] Credenciais S3 do Supabase nao configuradas. Upload ignorado.")
        return 0

    target_dir = LOCAL_LAKE_DIR
    if prefix:
        target_dir = LOCAL_LAKE_DIR / prefix

    if not target_dir.exists():
        print(f"[INFO] [Lake Sync] Diretorio {target_dir} nao existe. Nada a sincronizar.")
        return 0

    print(f"[SYNC] [Lake Sync] Sincronizando Local data_lake ({prefix}) -> Cloud S3 ({bucket})...")
    uploaded_count = 0

    # Lista objetos ja existentes no S3 para evitar uploads duplicados de arquivos identicos
    existing_remote = {}
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix.strip("/") + "/"

    try:
        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []):
                existing_remote[obj["Key"]] = obj["Size"]
    except Exception as e:
        print(f"[WARN] Aviso ao listar objetos remotos: {e}")

    for file_path in target_dir.rglob("*"):
        if not file_path.is_file():
            continue

        # Ignorar arquivos temporarios e checkpoints do Spark
        rel_str = str(file_path.relative_to(LOCAL_LAKE_DIR)).replace("\\", "/")
        if "_temporary" in rel_str or "checkpoints" in rel_str or file_path.name.startswith("."):
            continue

        file_size = file_path.stat().st_size
        if existing_remote.get(rel_str) == file_size:
            continue  # Ja esta atualizado na nuvem

        try:
            s3.upload_file(str(file_path), bucket, rel_str)
            uploaded_count += 1
        except Exception as e:
            print(f"[WARN] Erro ao enviar {rel_str}: {e}")

    print(f"[OK] [Lake Sync] Upload concluido! {uploaded_count} arquivos sincronizados com a nuvem.")
    return uploaded_count


if __name__ == "__main__":
    import sys
    action = sys.argv[1] if len(sys.argv) > 1 else "status"
    if action == "pull":
        sync_lake_from_cloud()
    elif action == "push":
        sync_lake_to_cloud()
    else:
        s3, b = get_s3_client()
        print(f"Bucket: {b}, Conectado: {s3 is not None}")
