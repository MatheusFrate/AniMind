import os
import urllib.parse
import requests
from app.database import get_db
from fastapi.responses import RedirectResponse
from fastapi import APIRouter, HTTPException
from app.services.user_service import generate_code_challenge
from datetime import datetime
from app.models.models import User
from app.schemas.user import UserCreate, UserResponse
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from fastapi import Request, Depends
import json
from fastapi.encoders import jsonable_encoder
router = APIRouter()
load_dotenv()

@router.get('/login')
async def myanimelist_login(request: Request):
    """
        Passo 1: Redirecionar o utilizador para o MyAnimeList.
    """
    code_challenge = generate_code_challenge()
    code_challenge_method = 'plain'
    request.session["code_verifier"] = code_challenge
    params = {
        'response_type': 'code',
        'client_id': os.getenv('CLIENT_ID'),
        'redirect_uri': os.getenv('REDIRECT_URI'),
        'code_challenge': code_challenge,
        'code_challenge_method': code_challenge_method
    }
    print(params)
    url = 'https://myanimelist.net/v1/oauth2/authorize?' + urllib.parse.urlencode(params)
    return RedirectResponse(url)

# Função de callback do login
@router.get("/auth/callback")
async def my_animelist_callback(request: Request, db: Session = Depends(get_db)):
    code = request.query_params.get('code')
    state = request.query_params.get('state')
    code_verifier = request.session.get("code_verifier")
    print(f"get code verifier", code_verifier)
    if code:
        token_url = os.getenv('TOKEN_URL')
        if not token_url:
            return {'message': 'TOKEN_URL não configurado'}
        data = {
            'client_id': os.getenv('CLIENT_ID'),
            'client_secret': os.getenv('CLIENT_SECRET'),
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': os.getenv('REDIRECT_URI'),
            'code_verifier': code_verifier
        }
        response = requests.post(token_url, data=data)
        tokens = response.json()

        if tokens.get('error'):
            return {"message": f"Erro!, Erro ao realizar login, {tokens}"}
            # return HttpResponse('')
        
        user_info_url = 'https://api.myanimelist.net/v2/users/@me'
        headers = {'Authorization': 'Bearer ' + tokens['access_token']}
        user_info_response = requests.get(user_info_url, headers=headers)
        user_info = user_info_response.json()

        joined_at_str = user_info['joined_at']
        joined_at = datetime.strptime(joined_at_str, '%Y-%m-%dT%H:%M:%S%z').date() if joined_at_str else None
        name = str(user_info['name']).lower()
        picture_url = user_info.get("picture")
        try:
            user = User(
                id=user_info['id'],
                name = name,
                gender =  user_info['gender'],
                joined_at = joined_at,
                picture = user_info['picture'],
                access_token = tokens['access_token'],
                refresh_token = tokens['refresh_token'] 
            )
            db.merge(user)
            db.commit()
            return {'Message': f'perfil {name} coletado com sucesso'}
        
        except Exception as e:
            db.rollback()
            return {"message": f'Erro ao salvar o perfil: {str(e)}'}
    else:
        return {"message": 'Erro ao coletar o perfil'}
    

 