import redis
import logging
from django.conf import settings
from fyers_apiv3 import fyersModel

# Initialize Redis
r = redis.from_url(settings.REDIS_URL)
logger = logging.getLogger(__name__)

def get_fyers_client(access_token=None):
    """
    Initializes the Fyers V3 Client.
    Refers to: 'fyers = fyersModel.FyersModel(...)' in documentation
    """
    return fyersModel.FyersModel(
        client_id=settings.FYERS_APP_ID,
        token=access_token,
        is_async=False,
        log_path=""
    )

def generate_auth_url(app_id, secret_key, callback_url):
    """
    Generates the V3 authorization URL using the Official SDK.
    Refers to: 'appSession.generate_authcode()' in documentation
    """
    try:
        session = fyersModel.SessionModel(
            client_id=app_id,
            secret_key=secret_key,
            redirect_uri=callback_url,
            response_type='code',
            grant_type='authorization_code'
        )
        # The SDK automatically builds the correct V3 URL with encoding
        generate_token_url = session.generate_authcode()
        return generate_token_url
    except Exception as e:
        logger.error(f"Error generating Auth URL: {e}")
        return "#"

def exchange_auth_code_for_token(auth_code, app_id, secret_key, callback_url):
    """
    Exchanges auth code for access token using Official SDK.
    Refers to: 'appSession.generate_token()' in documentation
    """
    try:
        session = fyersModel.SessionModel(
            client_id=app_id,
            secret_key=secret_key,
            redirect_uri=callback_url,
            response_type='code',
            grant_type='authorization_code'
        )
        
        # 1. Set the auth code received from the callback
        session.set_token(auth_code)
        
        # 2. Generate the access token
        response = session.generate_token()
        
        # 3. Extract token
        if response.get('s') == 'ok':
            access_token = response.get('access_token')
            if access_token:
                # Publish update for workers
                r.publish('fyers_token_update', access_token)
                return access_token
        else:
            logger.error(f"Token Generation Failed: {response}")
            
    except Exception as e:
        logger.error(f"Error exchanging token: {e}")
        
    return None