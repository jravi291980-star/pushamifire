import json
import redis
import logging
import ssl
from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction

# Project Imports
from trading.models import FyersCredentials, GlobalTradingSettings, StrategyTrade
from trading.fyers_auth_util import get_fyers_client

logger = logging.getLogger('algo_worker')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- SSL FIX FOR HEROKU REDIS ---
if settings.REDIS_URL.startswith('rediss://'):
    r = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
else:
    r = redis.from_url(settings.REDIS_URL)

GROUP_NAME = "ALGO_GROUP"
CONSUMER_NAME = "WORKER_1"
STREAM_CANDLE = "candle_stream_1m"
STREAM_TICK = "market_ticks"
REDIS_PDL_KEY = "prev_day_ohlc"

class Command(BaseCommand):
    help = 'Runs the Fyers V3 Algo Strategy Worker (Cash Breakdown)'

    # --- LUA SCRIPTS FOR ATOMIC LIMIT ENFORCEMENT ---
    
    # 1. Check Limits & Increment (Atomic)
    # KEYS[1]: Global Counter Key (e.g., 'daily_count:2023-10-27')
    # KEYS[2]: Symbol Counter Key (e.g., 'symbol_count:2023-10-27:NSE:SBIN-EQ')
    # ARGV[1]: Global Limit
    # ARGV[2]: Symbol Limit
    # ARGV[3]: Expiry in seconds (86400)
    CHECK_AND_INCR_LUA = """
    local global_count = tonumber(redis.call('GET', KEYS[1]) or 0)
    local symbol_count = tonumber(redis.call('GET', KEYS[2]) or 0)
    local global_limit = tonumber(ARGV[1])
    local symbol_limit = tonumber(ARGV[2])

    if global_count >= global_limit then
        return -1 -- Global Limit Reached
    end
    
    if symbol_count >= symbol_limit then
        return -2 -- Symbol Limit Reached
    end

    redis.call('INCR', KEYS[1])
    redis.call('INCR', KEYS[2])
    redis.call('EXPIRE', KEYS[1], ARGV[3])
    redis.call('EXPIRE', KEYS[2], ARGV[3])
    
    return 1 -- Success
    """

    # 2. Rollback (Decrement) on Failure
    ROLLBACK_LUA = """
    local global_val = tonumber(redis.call('GET', KEYS[1]) or 0)
    local symbol_val = tonumber(redis.call('GET', KEYS[2]) or 0)
    
    if global_val > 0 then redis.call('DECR', KEYS[1]) end
    if symbol_val > 0 then redis.call('DECR', KEYS[2]) end
    return 1
    """

    def handle(self, *args, **options):
        logger.info("--- Initializing Algo Worker V3 (Strict Limits) ---")

        # 1. Initialize Redis Consumer Group
        try:
            r.xgroup_create(STREAM_CANDLE, GROUP_NAME, id='0', mkstream=True)
            r.xgroup_create(STREAM_TICK, GROUP_NAME, id='$', mkstream=True)
        except redis.exceptions.ResponseError:
            pass 

        # 2. Authenticate
        try:
            creds = FyersCredentials.objects.get(is_active=True)
            fyers = get_fyers_client(creds.access_token)
            settings_db, _ = GlobalTradingSettings.objects.get_or_create(user=creds.user)
            logger.info(f"Authenticated as: {creds.app_id}")
        except Exception as e:
            logger.error(f"CRITICAL: Initialization Error: {e}")
            return

        # 3. Load Previous Day Low (PDL) Cache
        try:
            cached_pdl_data = r.hgetall(REDIS_PDL_KEY)
            prev_day_data_map = {}
            for k, v in cached_pdl_data.items():
                sym = k.decode('utf-8')
                val = json.loads(v.decode('utf-8'))
                prev_day_data_map[sym] = val
            logger.info(f"Loaded PDL Data for {len(prev_day_data_map)} symbols.")
        except Exception as e:
            logger.error(f"Failed to load PDL Cache: {e}")
            prev_day_data_map = {}

        logger.info(">>> Algo Worker Loop Started <<<")

        while True:
            try:
                events = r.xreadgroup(
                    groupname=GROUP_NAME, 
                    consumername=CONSUMER_NAME, 
                    streams={STREAM_CANDLE: '>', STREAM_TICK: '>'}, 
                    count=10, 
                    block=1000
                )
                
                if not events:
                    continue

                for stream, messages in events:
                    for msg_id, data in messages:
                        try:
                            if stream == STREAM_CANDLE:
                                self.process_candle(data, settings_db, prev_day_data_map)
                            elif stream == STREAM_TICK:
                                self.process_tick(data, fyers, settings_db)
                            
                            r.xack(stream, GROUP_NAME, msg_id)
                        except Exception as e:
                            logger.error(f"Error processing MsgID {msg_id}: {e}")

            except redis.exceptions.ConnectionError:
                logger.error("Redis Connection Lost. Retrying...")
                import time; time.sleep(5)
            except Exception as e:
                logger.error(f"Unhandled Exception in Loop: {e}")

    # --- STRATEGY LOGIC ---
    def process_candle(self, data, settings_db, prev_day_data_map):
        try:
            payload_str = data[b'data'].decode('utf-8') if b'data' in data else data['data']
            payload = json.loads(payload_str)
        except Exception:
            return

        symbol = payload['symbol']
        pdl_info = prev_day_data_map.get(symbol)
        if not pdl_info: return

        pdl = float(pdl_info['low'])
        open_p = float(payload['open'])
        close_p = float(payload['close'])
        
        # Breakdown Condition: Open > PDL and Close < PDL
        if open_p > pdl and close_p < pdl:
            today = timezone.now().date()
            
            # Simple DB Check to avoid spamming PENDINGs (Not atomic, optimization only)
            if StrategyTrade.objects.filter(symbol=symbol, created_at__date=today).count() >= settings_db.max_trades_per_symbol:
                return

            entry_level = float(payload['low']) * 0.9998
            stop_loss = float(payload['high']) * 1.0002
            risk = stop_loss - entry_level
            if risk <= 0: return

            qty = int(float(settings_db.risk_per_trade_amount) / risk)
            if qty < 1: qty = 1
            target = entry_level - (risk * float(settings_db.risk_reward_ratio))

            # Create PENDING trade (Monitoring)
            # We do NOT increment limits here yet. Limits are enforced at ENTRY Trigger.
            StrategyTrade.objects.create(
                symbol=symbol, status='PENDING', candle_timestamp=datetime.fromisoformat(payload['ts']),
                candle_open=open_p, candle_high=payload['high'], candle_low=payload['low'],
                candle_close=close_p, prev_day_low=pdl, entry_level=entry_level,
                stop_loss=stop_loss, target_price=target, quantity=qty
            )
            logger.info(f"SIGNAL: {symbol} Breakdown < {pdl} | Monitoring for Entry")

    def process_tick(self, data, fyers, settings_db):
        try:
            symbol = data[b'symbol'].decode('utf-8')
            ltp = float(data[b'ltp'])
        except KeyError: return

        # 1. ENTRY LOGIC with ATOMIC LIMITS
        # Fetch ID list first to minimize locking time
        pending_ids = list(StrategyTrade.objects.filter(symbol=symbol, status='PENDING').values_list('id', flat=True))
        
        for trade_id in pending_ids:
            # Use Transaction + Select For Update to prevent Race Conditions
            with transaction.atomic():
                try:
                    trade = StrategyTrade.objects.select_for_update(skip_locked=True).get(id=trade_id)
                except StrategyTrade.DoesNotExist:
                    continue # Already processed by another worker

                # Double check status inside lock
                if trade.status != 'PENDING':
                    continue

                if ltp <= float(trade.entry_level):
                    # --- ATOMIC LIMIT CHECK START ---
                    today_str = timezone.now().strftime('%Y-%m-%d')
                    global_key = f"daily_count:{today_str}"
                    symbol_key = f"symbol_count:{today_str}:{symbol}"
                    
                    # Run Lua Script
                    limit_result = r.eval(
                        self.CHECK_AND_INCR_LUA, 2, global_key, symbol_key,
                        settings_db.max_trades_per_day, settings_db.max_trades_per_symbol, 86400
                    )

                    if limit_result == -1:
                        logger.warning(f"Global Limit Reached. Skipping {symbol}.")
                        trade.status = 'EXPIRED' # Optionally mark expired so we stop checking
                        trade.exit_reason = "Global Limit Reached"
                        trade.save()
                        continue
                    elif limit_result == -2:
                        logger.warning(f"Symbol Limit Reached for {symbol}.")
                        trade.status = 'EXPIRED'
                        trade.exit_reason = "Symbol Limit Reached"
                        trade.save()
                        continue
                    # --- ATOMIC LIMIT CHECK END ---

                    logger.info(f"ENTRY TRIGGER: {symbol} @ {ltp} | Placing Order...")
                    
                    # Place Order
                    oid = self.place_fyers_order(fyers, symbol, trade.quantity, -1, 2)
                    
                    if oid:
                        trade.status = 'PENDING_ENTRY'
                        trade.entry_order_id = oid
                        trade.save()
                        logger.info(f"Entry Order Placed: {oid}")
                    else:
                        # ROLLBACK LIMITS if API Call Fails
                        r.eval(self.ROLLBACK_LUA, 2, global_key, symbol_key)
                        trade.status = 'FAILED'
                        trade.save()
                        logger.error(f"Order Placement Failed. Limits Rolled Back.")

        # 2. EXIT & TSL LOGIC
        # Note: 'OPEN' status is set by Order Socket when fill is confirmed.
        open_ids = list(StrategyTrade.objects.filter(symbol=symbol, status='OPEN').values_list('id', flat=True))
        
        for trade_id in open_ids:
            with transaction.atomic():
                try:
                    trade = StrategyTrade.objects.select_for_update(skip_locked=True).get(id=trade_id)
                except StrategyTrade.DoesNotExist:
                    continue

                if trade.status != 'OPEN': continue

                sl = float(trade.stop_loss)
                tgt = float(trade.target_price)
                
                # Exit Condition
                if ltp >= sl or ltp <= tgt:
                    reason = "Stop Loss" if ltp >= sl else "Target"
                    oid = self.place_fyers_order(fyers, symbol, trade.quantity, 1, 2)
                    if oid:
                        trade.status = 'PENDING_EXIT'
                        trade.exit_order_id = oid
                        trade.exit_reason = reason
                        trade.save()
                        logger.info(f"EXIT TRIGGER: {symbol} ({reason}) | Order: {oid}")
                    else:
                        logger.error(f"Exit Order Failed for {symbol}")
                
                # TSL Logic (Breakeven)
                elif not trade.is_breakeven_moved:
                    entry = float(trade.actual_entry_price or trade.entry_level)
                    risk = sl - entry
                    if (entry - ltp) >= (risk * float(settings_db.breakeven_trigger_r)):
                        trade.stop_loss = entry
                        trade.is_breakeven_moved = True
                        trade.save()
                        logger.info(f"TSL UPDATE: {symbol} Profit Locked (Breakeven)")

    def place_fyers_order(self, fyers, symbol, qty, side, type):
        try:
            resp = fyers.place_order(data={"symbol":symbol, "qty":int(qty), "type":type, "side":side, "productType":"INTRADAY", "validity":"DAY", "limitPrice":0, "stopPrice":0, "disclosedQty":0, "offlineOrder":False})
            if isinstance(resp, dict) and resp.get('s') == 'ok': return resp.get('id')
        except Exception as e: logger.error(f"API Error: {e}")
        return None