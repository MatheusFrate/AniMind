import os
import sys

# Adiciona a raiz do projeto ao sys.path para permitir a importação do pacote 'app'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from starlette.middleware.sessions import SessionMiddleware

from app.database import Base, engine
from app.models import models
from app.api.v1 import users, auth, anime_list, news, recommendations, rag

load_dotenv()

# Cria todas as tabelas no banco se ainda não existirem
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title='AniMind Data Platform',
    description='Plataforma de Dados de Animes com Ingestão ETL e Sincronização MyAnimeList',
    version='2.1.0'
)

# Middleware de Sessão (necessário para o fluxo OAuth)
SECRET_KEY = os.getenv("SESSION_SECRET_KEY", "animind_secret_session_key_2026")
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# Roteadores da API v1
app.include_router(auth.router, prefix="/api/v1", tags=["Autenticação OAuth"])
app.include_router(anime_list.router, prefix="/api/v1", tags=["Sincronização MAL"])
app.include_router(users.router, prefix="/api/v1", tags=["Estatísticas e Usuários"])
app.include_router(news.router, prefix="/api/v1", tags=["RSS News & ETL"])
app.include_router(recommendations.router, prefix="/api/v1/recommendations", tags=["Recomendações (Machine Learning)"])
app.include_router(rag.router, prefix="/api/v1/rag", tags=["Busca Semântica & RAG (IA)"])


# Arquivos estáticos e Dashboard Frontend
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get('/')
@app.get('/dashboard')
def dashboard():
    index_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {
        "status": "online",
        "app": "AniMind Data Platform",
        "docs": "/docs",
        "message": "API rodando perfeitamente!"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
