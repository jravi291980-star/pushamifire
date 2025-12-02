import redis
import logging
import ssl
from django.conf import settings
from fyers_apiv3 import fyersModel

logger = logging.getLogger(__name__)

# --- FIX REDIS SSL CONNECTION ---
# Heroku uses Self-Signed Certs for Redis, which Python blocks by default.
# We must explicitly allow it by setting ssl_cert_reqs=ssl.CERT_NONE
if settings.REDIS_URL.startswith('rediss://'):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    r = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
else:
    # Localhost (No SSL)
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
    try:
        session = fyersModel.SessionModel(
            client_id=app_id,
            secret_key=secret_key,
            redirect_uri=callback_url,
            response_type='code',
            grant_type='authorization_code'
        )
        return session.generate_authcode()
    except Exception as e:
        logger.error(f"Error generating Auth URL: {e}")
        return "#"

def exchange_auth_code_for_token(auth_code, app_id, secret_key, callback_url):
    """
    Exchanges auth code for access token.
    """
    try:
        session = fyersModel.SessionModel(
            client_id=app_id,
            secret_key=secret_key,
            redirect_uri=callback_url,
            response_type='code',
            grant_type='authorization_code'
        )
        
        # 1. Set the auth code
        session.set_token(auth_code)
        
        # 2. Generate the access token
        response = session.generate_token()
        
        # 3. Extract token
        if response.get('s') == 'ok':
            access_token = response.get('access_token')
            if access_token:
                # Publish update to Redis (This is where it was crashing)
                r.publish('fyers_token_update', access_token)
                return access_token
        else:
            logger.error(f"Token Generation Failed: {response}")
            
    except Exception as e:
        logger.error(f"Error exchanging token: {e}")
        
    return None