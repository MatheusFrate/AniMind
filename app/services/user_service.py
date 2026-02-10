import secrets

def create_user_logic(user_data):
    print(f'Processamento de criação do usuário: {user_data.username}')
    return {'id': 1, 'username': user_data.username}

def generate_code_challenge():
    code_challenge = secrets.token_urlsafe(64)
    return code_challenge