import os
import requests
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from app.models.models import AppCredential

TOKEN_URL = os.getenv("TOKEN_URL", "https://myanimelist.net/v1/oauth2/token")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")


def save_system_tokens(db: Session, access_token: str, refresh_token: str, expires_in: int) -> AppCredential:
    """Salva ou atualiza as credenciais OAuth do sistema na tabela app_credentials."""
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    credential = db.query(AppCredential).first()

    if not credential:
        credential = AppCredential(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=expires_at
        )
        db.add(credential)
    else:
        credential.access_token = access_token
        credential.refresh_token = refresh_token
        credential.expires_at = expires_at

    db.commit()
    db.refresh(credential)
    print("[TOKEN SERVICE] Tokens do sistema salvos com sucesso!")
    return credential


def get_valid_access_token(db: Session) -> str | None:
    """
    Retorna um access_token válido do sistema.
    Auto-renova via refresh_token se estiver próximo de expirar (margem de 5 minutos).
    """
    credential = db.query(AppCredential).first()

    if not credential:
        return None

    now = datetime.now(timezone.utc)
    expires_at = credential.expires_at
    if expires_at and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    # Token ainda válido
    if expires_at and (expires_at - now) > timedelta(minutes=5):
        return credential.access_token

    # Token expirado ou próximo: renovar via refresh_token
    print("[TOKEN SERVICE] Renovando access_token via refresh_token...")
    try:
        response = requests.post(TOKEN_URL, data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": credential.refresh_token,
        }, timeout=10)

        if response.status_code == 200:
            tokens = response.json()
            save_system_tokens(
                db,
                tokens["access_token"],
                tokens.get("refresh_token", credential.refresh_token),
                tokens.get("expires_in", 2592000)
            )
            print("[TOKEN SERVICE] Token renovado com sucesso!")
            return tokens["access_token"]
        else:
            print(f"[TOKEN SERVICE] Erro ao renovar token: {response.status_code} - {response.text}")
            return credential.access_token
    except Exception as e:
        print(f"[TOKEN SERVICE] Exceção ao renovar token: {e}")
        return credential.access_token
