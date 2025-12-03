# import json
# import time
# import redis
# import logging
# import ssl
# from datetime import datetime
# from django.conf import settings
# from django.core.management.base import BaseCommand
# from trading.models import FyersCredentials
# from fyers_apiv3.FyersWebsocket import data_ws
# from trading.constants import get_strategy_symbols # Import the list

# logger = logging.getLogger('data_engine')

# # SSL Configuration for Heroku Redis
# if settings.REDIS_URL.startswith('rediss://'):
#     r = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
# else:
#     r = redis.from_url(settings.REDIS_URL)

# class Command(BaseCommand):
#     help = 'Runs Fyers V3 Data Socket for Strategy Symbols'

#     def handle(self, *args, **options):
#         try:
#             creds = FyersCredentials.objects.get(is_active=True)
#             access_token = creds.access_token
#         except FyersCredentials.DoesNotExist:
#             logger.error("No active credentials.")
#             return

#         # 1. Get the Master List
#         symbols = get_strategy_symbols()
#         logger.info(f"Loaded {len(symbols)} symbols for Data Stream.")
        
#         # State for candle aggregation
#         candle_map = {}

#         def on_message(message):
#             # Message processing logic matches V3 standards
#             if not isinstance(message, dict) or 'type' not in message:
#                 return

#             # Handle Symbol Update (LTP)
#             if 'symbol' in message and 'ltp' in message:
#                 symbol = message['symbol']
#                 ltp = message['ltp']
#                 timestamp = time.time() 

#                 # 1. Publish Tick
#                 r.xadd('market_ticks', {'symbol': symbol, 'ltp': ltp, 'ts': timestamp})

#                 # 2. Aggregate 1-Min Candle
#                 current_min = int(timestamp // 60)
                
#                 if symbol not in candle_map:
#                     candle_map[symbol] = {
#                         'minute': current_min,
#                         'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp
#                     }
                
#                 c = candle_map[symbol]
                
#                 if current_min > c['minute']:
#                     # Candle Closed
#                     final_candle = {
#                         'symbol': symbol,
#                         'open': c['open'], 'high': c['high'], 
#                         'low': c['low'], 'close': c['close'],
#                         'ts': datetime.fromtimestamp(c['minute'] * 60).isoformat()
#                     }
#                     r.xadd('candle_stream_1m', {'data': json.dumps(final_candle)})
                    
#                     # Start new candle
#                     candle_map[symbol] = {
#                         'minute': current_min,
#                         'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp
#                     }
#                 else:
#                     c['high'] = max(c['high'], ltp)
#                     c['low'] = min(c['low'], ltp)
#                     c['close'] = ltp

#         def on_error(message):
#             logger.error(f"Data Socket Error: {message}")

#         def on_close(message):
#             logger.info("Data Socket Closed")

#         def on_open():
#             logger.info("Connected. Subscribing to symbols...")
#             # Fyers Data Socket can handle list subscriptions
#             # We use 'SymbolUpdate' to get LTP for ticks
#             fyers_socket.subscribe(symbols=symbols, data_type="SymbolUpdate")
#             fyers_socket.keep_running()

#         fyers_socket = data_ws.FyersDataSocket(
#             access_token=access_token,
#             log_path="",
#             litemode=True,
#             write_to_file=False,
#             reconnect=True,
#             on_connect=on_open,
#             on_close=on_close,
#             on_error=on_error,
#             on_message=on_message
#         )
#         fyers_socket.connect()

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

# SSL Fix for Heroku
if settings.REDIS_URL.startswith('rediss://'):
    r = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
else:
    r = redis.from_url(settings.REDIS_URL)

class Command(BaseCommand):
    help = 'Runs Fyers V3 Data Socket with Volume Calculation'

    def handle(self, *args, **options):
        try:
            creds = FyersCredentials.objects.get(is_active=True)
            access_token = creds.access_token
        except FyersCredentials.DoesNotExist:
            logger.error("No active credentials.")
            return

        symbols = get_strategy_symbols()
        logger.info(f"Loaded {len(symbols)} symbols.")
        
        # State: { 'NSE:SBIN-EQ': {'minute': 1234, 'open': 500, ..., 'start_day_vol': 100000} }
        candle_map = {}

        def on_message(message):
            if not isinstance(message, dict) or 'type' not in message:
                return

            # Fyers V3 (litemode=False) provides 'symbol', 'ltp', 'vol_traded_today'
            if 'symbol' in message and 'ltp' in message:
                symbol = message['symbol']
                ltp = float(message['ltp'])
                # Get cumulative day volume (default to 0 if missing)
                curr_day_vol = int(message.get('vol_traded_today', 0))
                
                timestamp = time.time()
                current_min = int(timestamp // 60)

                # 1. Publish Tick
                r.xadd('market_ticks', {'symbol': symbol, 'ltp': ltp, 'ts': timestamp})

                # 2. Aggregate Candle
                if symbol not in candle_map:
                    candle_map[symbol] = {
                        'minute': current_min,
                        'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp,
                        'start_day_vol': curr_day_vol # Capture vol at start of minute
                    }
                
                c = candle_map[symbol]
                
                # Check for Minute Change
                if current_min > c['minute']:
                    # Calculate Volume for this specific minute
                    # If start_day_vol was 0 (first tick), assume entire curr_day_vol is the volume (approx)
                    # or use curr_day_vol if start was missing.
                    minute_vol = curr_day_vol - c['start_day_vol']
                    if minute_vol < 0: minute_vol = 0 # Safety check

                    final_candle = {
                        'symbol': symbol,
                        'open': c['open'], 'high': c['high'], 
                        'low': c['low'], 'close': c['close'],
                        'volume': minute_vol, # <--- NEW FIELD
                        'ts': datetime.fromtimestamp(c['minute'] * 60).isoformat()
                    }
                    
                    # Publish completed candle
                    r.xadd('candle_stream_1m', {'data': json.dumps(final_candle)})
                    
                    # Reset for new minute
                    candle_map[symbol] = {
                        'minute': current_min,
                        'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp,
                        'start_day_vol': curr_day_vol
                    }
                else:
                    # Update current candle stats
                    c['high'] = max(c['high'], ltp)
                    c['low'] = min(c['low'], ltp)
                    c['close'] = ltp
                    # Note: We don't update 'start_day_vol' during the minute

        def on_error(msg): logger.error(f"Socket Error: {msg}")
        def on_close(msg): logger.info("Socket Closed")
        def on_open():
            logger.info("Connected. Subscribing...")
            fyers_socket.subscribe(symbols=symbols, data_type="SymbolUpdate")
            fyers_socket.keep_running()

        fyers_socket = data_ws.FyersDataSocket(
            access_token=access_token,
            log_path="",
            litemode=False, # MUST BE FALSE to get Volume data
            write_to_file=False,
            reconnect=True,
            on_connect=on_open, on_close=on_close, on_error=on_error, on_message=on_message
        )
        fyers_socket.connect()