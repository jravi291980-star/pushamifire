import json
import redis
import logging
import ssl
import time
from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from trading.models import LiveScanResult

# Logging Setup
logger = logging.getLogger('scanner_worker')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- SSL FIX FOR HEROKU REDIS ---
# Essential for connecting to Heroku's secure Redis instance
if settings.REDIS_URL.startswith('rediss://'):
    r = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
else:
    r = redis.from_url(settings.REDIS_URL)

# Constants
GROUP_NAME = "SCANNER_GROUP"
CONSUMER_NAME = "SCANNER_1"
STREAM_CANDLE = "candle_stream_1m"
REDIS_PDL_KEY = "prev_day_ohlc"

class Command(BaseCommand):
    help = 'Runs the Live Scanner for Cash Breakdown Strategy on all tracked symbols'

    def handle(self, *args, **options):
        logger.info("--- Initializing Scanner Worker ---")

        # 1. Initialize Redis Consumer Group
        # We use a distinct group name so we consume a COPY of the data independently of the Algo Worker
        try:
            r.xgroup_create(STREAM_CANDLE, GROUP_NAME, id='0', mkstream=True)
        except redis.exceptions.ResponseError:
            pass # Group already exists

        # 2. Load Previous Day Low (PDL) Cache
        # This allows checking "Open > PDL > Close" instantly for 500+ stocks without DB hits
        try:
            cached_pdl_data = r.hgetall(REDIS_PDL_KEY)
            prev_day_data_map = {}
            for k, v in cached_pdl_data.items():
                sym = k.decode('utf-8')
                val = json.loads(v.decode('utf-8'))
                prev_day_data_map[sym] = val
            logger.info(f"Scanner Loaded PDL Data for {len(prev_day_data_map)} symbols.")
        except Exception as e:
            logger.error(f"Failed to load PDL Cache: {e}")
            prev_day_data_map = {}

        logger.info(">>> Scanner Loop Started <<<")

        while True:
            try:
                # Read from Candle Stream (Non-blocking look or block for 2s)
                events = r.xreadgroup(
                    groupname=GROUP_NAME, 
                    consumername=CONSUMER_NAME, 
                    streams={STREAM_CANDLE: '>'}, 
                    count=100, # Batch process high volume of candles
                    block=2000
                )
                
                if not events:
                    continue

                for stream, messages in events:
                    for msg_id, data in messages:
                        try:
                            self.scan_candle(data, prev_day_data_map)
                            # Acknowledge immediately (we don't need strict retry logic for scanner)
                            r.xack(stream, GROUP_NAME, msg_id)
                        except Exception as e:
                            logger.error(f"Scanner Error processing MsgID {msg_id}: {e}")

            except redis.exceptions.ConnectionError:
                logger.error("Redis Connection Lost. Retrying...")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Unhandled Exception in Loop: {e}")

    def scan_candle(self, data, prev_day_data_map):
        """
        Check Strategy Condition:
        1. Open > PDL AND Close < PDL
        2. Turnover (Volume * Close) > 1 Crore
        """
        try:
            payload_str = data[b'data'].decode('utf-8') if b'data' in data else data['data']
            payload = json.loads(payload_str)
        except Exception:
            return

        symbol = payload['symbol']
        pdl_info = prev_day_data_map.get(symbol)
        
        # If we don't have PDL data, we can't scan this symbol
        if not pdl_info: 
            return

        pdl = float(pdl_info['low'])
        open_p = float(payload['open'])
        close_p = float(payload['close'])
        volume = float(payload.get('volume', 0))
        
        # --- 1. PRICE LOGIC ---
        # Breakdown Pattern
        if open_p > pdl and close_p < pdl:
            
            # --- 2. VOLUME LOGIC ---
            # Filter out illiquid stocks (Turnover > 10,000,000)
            turnover = volume * close_p
            if turnover <= 10000000:
                return

            # Construct Pattern Description
            pattern_desc = (
                f"Breakdown: O:{open_p} > PDL:{pdl} > C:{close_p} | "
                f"Vol: {volume:,.0f} | Val: {turnover/10000000:.2f}Cr"
            )
            
            # Save to Database for Dashboard Display
            LiveScanResult.objects.create(
                symbol=symbol,
                pattern=pattern_desc
            )
            
            # Database Hygiene: Keep only the last 50 scans to prevent DB bloat
            if LiveScanResult.objects.count() > 50:
                oldest = LiveScanResult.objects.order_by('scan_time').first()
                if oldest:
                    oldest.delete()

            logger.info(f"SCAN MATCH: {symbol} | {pattern_desc}")