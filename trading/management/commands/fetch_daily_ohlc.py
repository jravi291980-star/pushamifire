import json
import redis
import logging
import time
import ssl
from datetime import datetime, timedelta
from django.conf import settings
from django.core.management.base import BaseCommand
from trading.models import FyersCredentials
from trading.fyers_auth_util import get_fyers_client
from trading.constants import get_strategy_symbols # Import List

logger = logging.getLogger('data_engine')

if settings.REDIS_URL.startswith('rediss://'):
    r = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
else:
    r = redis.from_url(settings.REDIS_URL)

class Command(BaseCommand):
    help = 'Fetches previous day OHLC for strategy symbols'

    def handle(self, *args, **options):
        try:
            creds = FyersCredentials.objects.get(is_active=True)
            fyers = get_fyers_client(creds.access_token)
        except Exception as e:
            logger.error(f"Auth failed: {e}")
            return

        symbols = get_strategy_symbols()
        
        # Date Setup
        today = datetime.now().date()
        from_date = today - timedelta(days=5) # Look back 5 days to handle weekends
        
        range_from = from_date.strftime('%Y-%m-%d')
        range_to = today.strftime('%Y-%m-%d')

        logger.info(f"Fetching History for {len(symbols)} symbols ({range_from} -> {range_to})")

        cached_count = 0

        for symbol in symbols:
            try:
                # Rate Limit Protection
                time.sleep(0.1) 
                
                data = {
                    "symbol": symbol,
                    "resolution": "D",
                    "date_format": "1",
                    "range_from": range_from,
                    "range_to": range_to,
                    "cont_flag": "1"
                }

                response = fyers.history(data)

                if response.get('s') != 'ok':
                    logger.warning(f"Failed {symbol}: {response.get('message')}")
                    continue

                candles = response.get('candles', [])
                if not candles:
                    continue

                # Get Last Completed Candle
                # candles = [[ts, o, h, l, c, v], ...]
                last_candle = candles[-1]
                candle_ts = datetime.fromtimestamp(last_candle[0])
                
                # If fetching after market open, ignore today's forming candle
                if candle_ts.date() == today:
                    if len(candles) > 1:
                        prev_day_candle = candles[-2]
                    else:
                        continue
                else:
                    prev_day_candle = last_candle

                ohlc_data = {
                    "ts": prev_day_candle[0],
                    "open": prev_day_candle[1],
                    "high": prev_day_candle[2],
                    "low": prev_day_candle[3],
                    "close": prev_day_candle[4],
                    "volume": prev_day_candle[5]
                }

                # Store in Redis Hash
                r.hset("prev_day_ohlc", symbol, json.dumps(ohlc_data))
                cached_count += 1
                
                if cached_count % 50 == 0:
                    logger.info(f"Progress: {cached_count}/{len(symbols)} cached.")

            except Exception as e:
                logger.error(f"Error {symbol}: {e}")

        logger.info(f"DONE. Cached Previous Day Data for {cached_count} symbols.")