import json
import redis
import logging
from datetime import datetime, timedelta
from django.conf import settings
from django.core.management.base import BaseCommand
from trading.models import FyersCredentials
from trading.fyers_auth_util import get_fyers_client

logger = logging.getLogger('data_engine')

class Command(BaseCommand):
    help = 'Fetches previous day OHLC data from Fyers History API and caches it in Redis'

    def handle(self, *args, **options):
        r = redis.from_url(settings.REDIS_URL)
        
        # 1. Authenticate
        try:
            creds = FyersCredentials.objects.get(is_active=True)
            fyers = get_fyers_client(creds.access_token)
        except Exception as e:
            logger.error(f"Auth failed: {e}")
            return

        # 2. Define Watchlist 
        # In production, fetch this from a 'Watchlist' model or your StrategyTrade model
        # You can also pass a CSV file path as an argument
        symbols = [
            "NSE:RELIANCE-EQ", "NSE:TCS-EQ", "NSE:HDFCBANK-EQ", 
            "NSE:INFY-EQ", "NSE:SBIN-EQ", "NSE:ICICIBANK-EQ",
            "NSE:AXISBANK-EQ", "NSE:KOTAKBANK-EQ", "NSE:LT-EQ"
        ]

        # 3. Date Calculation (Last 5 days to cover weekends/holidays)
        today = datetime.now().date()
        from_date = today - timedelta(days=5)
        
        range_from = from_date.strftime('%Y-%m-%d')
        range_to = today.strftime('%Y-%m-%d')

        logger.info(f"Fetching History from {range_from} to {range_to}")

        cached_count = 0

        for symbol in symbols:
            try:
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
                    logger.warning(f"Failed to fetch {symbol}: {response.get('message')}")
                    continue

                candles = response.get('candles', [])
                if not candles:
                    logger.warning(f"No candles found for {symbol}")
                    continue

                # candles format: [[timestamp, open, high, low, close, volume], ...]
                # Logic: We want the LAST COMPLETED candle.
                
                last_candle = candles[-1]
                candle_ts = datetime.fromtimestamp(last_candle[0])
                
                # If script runs AFTER market open today, the last candle might be "today's" forming candle.
                # We want previous day.
                if candle_ts.date() == today:
                    if len(candles) > 1:
                        prev_day_candle = candles[-2]
                    else:
                        logger.warning(f"Not enough history for {symbol}")
                        continue
                else:
                    prev_day_candle = last_candle

                # Structure for Redis
                # Fyers History Response Index: 0=ts, 1=o, 2=h, 3=l, 4=c, 5=v
                ohlc_data = {
                    "ts": prev_day_candle[0],
                    "open": prev_day_candle[1],
                    "high": prev_day_candle[2],
                    "low": prev_day_candle[3],
                    "close": prev_day_candle[4],
                    "volume": prev_day_candle[5]
                }

                # Store in Redis Hash: 'prev_day_ohlc'
                # Key: Symbol, Value: JSON String
                r.hset("prev_day_ohlc", symbol, json.dumps(ohlc_data))
                cached_count += 1
                logger.info(f"Cached {symbol} | PDL: {ohlc_data['low']}")

            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")

        logger.info(f"Successfully cached previous day data for {cached_count} symbols.")