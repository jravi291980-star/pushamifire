import json
import time
import redis
import logging
import ssl
from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from trading.models import FyersCredentials
from fyers_apiv3.FyersWebsocket import data_ws
from trading.constants import get_strategy_symbols

logger = logging.getLogger('data_engine')

if settings.REDIS_URL.startswith('rediss://'):
    r = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
else:
    r = redis.from_url(settings.REDIS_URL)

class Command(BaseCommand):
    help = 'Runs Fyers V3 Data Socket with Batched Subscription'

    def handle(self, *args, **options):
        while True:
            try:
                creds = FyersCredentials.objects.get(is_active=True)
                raw_token = creds.access_token
                app_id = creds.app_id
                
                # --- CRITICAL FIX: FORMAT TOKEN ---
                if ":" not in raw_token:
                    full_token = f"{app_id}:{raw_token}"
                else:
                    full_token = raw_token
                    
                logger.info(f"Data Engine Token Loaded for App: {app_id}")
            except FyersCredentials.DoesNotExist:
                logger.error("No active credentials.")
                time.sleep(10)
                continue

            symbols = get_strategy_symbols()
            candle_map = {}

            def on_message(message):
                if not isinstance(message, dict) or 'type' not in message: return
                if 'symbol' in message and 'ltp' in message:
                    symbol = message['symbol']
                    ltp = float(message['ltp'])
                    curr_vol = int(message.get('vol_traded_today', 0))
                    ts = time.time()
                    curr_min = int(ts // 60)

                    r.xadd('market_ticks', {'symbol': symbol, 'ltp': ltp, 'ts': ts})

                    if symbol not in candle_map:
                        candle_map[symbol] = {'minute': curr_min, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'start_vol': curr_vol}
                    
                    c = candle_map[symbol]
                    if curr_min > c['minute']:
                        vol = curr_vol - c['start_vol']
                        if vol < 0: vol = 0
                        final = {'symbol': symbol, 'open': c['open'], 'high': c['high'], 'low': c['low'], 'close': c['close'], 'volume': vol, 'ts': datetime.fromtimestamp(c['minute']*60).isoformat()}
                        r.xadd('candle_stream_1m', {'data': json.dumps(final)})
                        candle_map[symbol] = {'minute': curr_min, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'start_vol': curr_vol}
                    else:
                        c['high'] = max(c['high'], ltp); c['low'] = min(c['low'], ltp); c['close'] = ltp

            def on_error(msg):
                logger.error(f"Socket Error: {msg}")
                if '403' in str(msg):
                    logger.critical("Token 403. Restarting...")
                    # Raising exception breaks the internal loop to force outer loop restart
                    raise Exception("Token Expired")

            def on_close(msg): logger.info("Socket Closed")
            
            def on_open():
                logger.info(f"Connected. Subscribing to {len(symbols)} symbols...")
                batch_size = 50
                for i in range(0, len(symbols), batch_size):
                    batch = symbols[i : i + batch_size]
                    fyers_socket.subscribe(symbols=batch, data_type="SymbolUpdate")
                    time.sleep(0.5) 
                fyers_socket.keep_running()

            try:
                fyers_socket = data_ws.FyersDataSocket(
                    access_token=full_token, # Use the formatted token
                    log_path="",
                    litemode=False,
                    write_to_file=False,
                    reconnect=True,
                    on_connect=on_open, on_close=on_close, on_error=on_error, on_message=on_message
                )
                fyers_socket.connect()
            except Exception as e:
                logger.error(f"Engine Exception: {e}")
                time.sleep(5)