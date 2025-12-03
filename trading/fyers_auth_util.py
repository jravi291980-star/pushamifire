import redis
import logging
import ssl
from django.conf import settings
from fyers_apiv3 import fyersModel

logger = logging.getLogger(__name__)

# --- SSL FIX ---
if settings.REDIS_URL.startswith('rediss://'):
    r = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
else:
    r = redis.from_url(settings.REDIS_URL)

def get_fyers_client(access_token=None):
    return fyersModel.FyersModel(
        client_id=settings.FYERS_APP_ID,
        token=access_token,
        is_async=False,
        log_path=""
    )

def generate_auth_url(app_id, secret_key, callback_url):
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

def exchange_auth_code_for_token(auth_code, creds_obj, callback_url):
    """
    Exchanges code for token, SAVES to DB, then PUBLISHES to Redis.
    Now accepts the 'creds_obj' (Model Instance) instead of raw strings.
    """
    try:
        session = fyersModel.SessionModel(
            client_id=creds_obj.app_id,
            secret_key=creds_obj.secret_key,
            redirect_uri=callback_url,
            response_type='code',
            grant_type='authorization_code'
        )
        
        session.set_token(auth_code)
        response = session.generate_token()
        
        if response.get('s') == 'ok':
            access_token = response.get('access_token')
            if access_token:
                # 1. CRITICAL: Save to Database FIRST
                creds_obj.access_token = access_token
                creds_obj.is_active = True
                creds_obj.save()
                logger.info("Token saved to DB successfully.")

                # 2. THEN Publish to Redis (Workers will now read the NEW token)
                r.publish('fyers_token_update', access_token)
                logger.info("Token update signal sent to workers.")
                
                return access_token
        else:
            logger.error(f"Token Gen Failed: {response}")
            
    except Exception as e:
        logger.error(f"Error exchanging token: {e}")
        
    return None