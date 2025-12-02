import json
import time
import redis
import logging
from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from trading.models import FyersCredentials
from fyers_apiv3.FyersWebsocket import data_ws

logger = logging.getLogger('data_engine')
r = redis.from_url(settings.REDIS_URL)

class Command(BaseCommand):
    help = 'Runs Fyers V3 Data Socket'

    def handle(self, *args, **options):
        try:
            creds = FyersCredentials.objects.get(is_active=True)
            access_token = creds.access_token
        except FyersCredentials.DoesNotExist:
            logger.error("No active credentials.")
            return

        # Symbols to Subscribe (NSE format for Fyers)
        symbols = ["NSE:RELIANCE-EQ", "NSE:TCS-EQ", "NSE:HDFCBANK-EQ", "NSE:INFY-EQ", "NSE:SBIN-EQ"]
        
        # State for candle aggregation
        candle_map = {}

        def on_message(message):
            """
            Handle incoming ticks.
            V3 Message structure differs significantly.
            """
            # Validating message type
            if not isinstance(message, dict) or 'type' not in message:
                return

            # Handle Symbol Update
            # Note: message structure depends on 'litemode'. Assuming standard mode.
            if 'symbol' in message and 'ltp' in message:
                symbol = message['symbol']
                ltp = message['ltp']
                timestamp = time.time() # Use local server time for aggregation consistency

                # 1. Publish Tick (For instant LTP checks)
                r.xadd('market_ticks', {'symbol': symbol, 'ltp': ltp, 'ts': timestamp})

                # 2. Aggregate 1-Min Candle
                current_min = int(timestamp // 60)
                
                if symbol not in candle_map:
                    candle_map[symbol] = {
                        'minute': current_min,
                        'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp
                    }
                
                c = candle_map[symbol]
                
                if current_min > c['minute']:
                    # Close previous candle
                    final_candle = {
                        'symbol': symbol,
                        'open': c['open'], 'high': c['high'], 
                        'low': c['low'], 'close': c['close'], # Close is last tick of prev min
                        'ts': datetime.fromtimestamp(c['minute'] * 60).isoformat()
                    }
                    r.xadd('candle_stream_1m', {'data': json.dumps(final_candle)})
                    logger.info(f"Candle Closed: {symbol} @ {c['close']}")

                    # Start new candle
                    candle_map[symbol] = {
                        'minute': current_min,
                        'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp
                    }
                else:
                    # Update current
                    c['high'] = max(c['high'], ltp)
                    c['low'] = min(c['low'], ltp)
                    c['close'] = ltp

        def on_error(message):
            logger.error(f"Data Socket Error: {message}")

        def on_close(message):
            logger.info("Data Socket Closed")

        def on_open():
            logger.info("Data Socket Connected. Subscribing...")
            fyers_socket.subscribe(symbols=symbols, data_type="SymbolUpdate")
            fyers_socket.keep_running()

        # Connect
        fyers_socket = data_ws.FyersDataSocket(
            access_token=access_token,
            log_path="",
            litemode=True, # Litemode is faster, provides essential LTP data
            write_to_file=False,
            reconnect=True,
            on_connect=on_open,
            on_close=on_close,
            on_error=on_error,
            on_message=on_message
        )
        fyers_socket.connect()