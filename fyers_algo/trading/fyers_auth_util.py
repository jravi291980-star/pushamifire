import redis
from django.conf import settings
from fyers_apiv3 import fyersModel

# Initialize Redis
r = redis.from_url(settings.REDIS_URL)

def get_fyers_client(access_token=None):
    """
    Initializes the Fyers V3 Client.
    """
    return fyersModel.FyersModel(
        client_id=settings.FYERS_APP_ID,
        token=access_token,
        is_async=False,
        log_path=""
    )

def generate_auth_url(app_id, secret_key, callback_url):
    """
    Generates the V3 authorization URL.
    """
    session = fyersModel.SessionModel(
        client_id=app_id,
        secret_key=secret_key,
        redirect_uri=callback_url,
        response_type='code',
        grant_type='authorization_code'
    )
    return session.generate_authcode()

def exchange_auth_code_for_token(auth_code, app_id, secret_key, callback_url):
    """
    Exchanges auth code for access token (V3).
    """
    session = fyersModel.SessionModel(
        client_id=app_id,
        secret_key=secret_key,
        redirect_uri=callback_url,
        response_type='code',
        grant_type='authorization_code'
    )
    session.set_token(auth_code)
    response = session.generate_token()
    
    # V3 Response structure: {'s': 'ok', 'code': 200, 'message': '...', 'access_token': '...'}
    if response.get('s') == 'ok':
        token = response.get('access_token')
        if token:
            r.publish('fyers_token_update', token)
            return token
    return None