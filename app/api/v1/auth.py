import os
import secrets
import requests
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.services.token_service import save_system_tokens

router = APIRouter()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8000/api/v1/auth/callback")
TOKEN_URL = os.getenv("TOKEN_URL", "https://myanimelist.net/v1/oauth2/token")
AUTH_URL = "https://myanimelist.net/v1/oauth2/authorize"


def generate_code_verifier() -> str:
    return secrets.token_urlsafe(100)[:128]


@router.get("/login")
def login(request: Request):
    """Inicia o fluxo OAuth com o MyAnimeList. Acesse uma única vez para autorizar o sistema."""
    code_verifier = generate_code_verifier()
    request.session["code_verifier"] = code_verifier

    auth_url = (
        f"{AUTH_URL}"
        f"?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&code_challenge={code_verifier}"
        f"&code_challenge_method=plain"
    )
    return RedirectResponse(url=auth_url)


@router.get("/auth/callback")
def auth_callback(request: Request, code: str = None):
    """Recebe o código de autorização do MAL e salva os tokens do sistema no banco."""
    if not code:
        return {"error": "Código de autorização ausente."}

    code_verifier = request.session.get("code_verifier")
    if not code_verifier:
        return {"error": "Code verifier não encontrado na sessão. Tente fazer login novamente."}

    response = requests.post(TOKEN_URL, data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "code_verifier": code_verifier,
        "grant_type": "authorization_code",
        "redirect_uri": REDIRECT_URI,
    }, timeout=15)

    if response.status_code != 200:
        return {"error": f"Falha ao obter token: {response.status_code} — {response.text}"}

    tokens = response.json()
    db: Session = SessionLocal()
    try:
        save_system_tokens(
            db,
            access_token=tokens["access_token"],
            refresh_token=tokens["refresh_token"],
            expires_in=tokens.get("expires_in", 2592000)
        )
    finally:
        db.close()

    return RedirectResponse(url="/?auth=success")


@router.get("/auth/status")
def auth_status():
    """Retorna se o sistema já possui um token OAuth salvo no banco."""
    db: Session = SessionLocal()
    try:
        from app.models.models import AppCredential
        credential = db.query(AppCredential).first()
        if credential:
            return {"authenticated": True, "message": "Token do sistema ativo."}
        return {"authenticated": False, "message": "Nenhum token do sistema encontrado. Acesse /api/v1/login para autorizar."}
    finally:
        db.close()