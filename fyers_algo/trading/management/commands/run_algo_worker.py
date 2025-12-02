import json
import redis
import logging
from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction

# Project Imports
from trading.models import FyersCredentials, GlobalTradingSettings, StrategyTrade
from trading.fyers_auth_util import get_fyers_client

# Logging Setup
logger = logging.getLogger('algo_worker')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Redis Configuration
r = redis.from_url(settings.REDIS_URL)

GROUP_NAME = "ALGO_GROUP"
CONSUMER_NAME = "WORKER_1"
STREAM_CANDLE = "candle_stream_1m"
STREAM_TICK = "market_ticks"
REDIS_PDL_KEY = "prev_day_ohlc"

class Command(BaseCommand):
    help = 'Runs the Fyers V3 Algo Strategy Worker (Cash Breakdown)'

    def handle(self, *args, **options):
        logger.info("--- Initializing Algo Worker V3 ---")

        # 1. Initialize Redis Consumer Group (Idempotent)
        try:
            r.xgroup_create(STREAM_CANDLE, GROUP_NAME, id='0', mkstream=True)
            r.xgroup_create(STREAM_TICK, GROUP_NAME, id='$', mkstream=True) # Start reading ticks from NOW
        except redis.exceptions.ResponseError:
            pass # Group already exists

        # 2. Authenticate & Load Settings
        try:
            creds = FyersCredentials.objects.get(is_active=True)
            fyers = get_fyers_client(creds.access_token)
            
            # Get or Create Global Settings
            settings_db, created = GlobalTradingSettings.objects.get_or_create(user=creds.user)
            if created:
                logger.warning("Global Settings created with defaults. Please configure in Dashboard.")
                
            logger.info(f"Authenticated as: {creds.app_id}")
            logger.info(f"Risk Per Trade: {settings_db.risk_per_trade_amount}")
            
        except FyersCredentials.DoesNotExist:
            logger.error("CRITICAL: No active Fyers Credentials found. Exiting.")
            return
        except Exception as e:
            logger.error(f"CRITICAL: Initialization Error: {e}")
            return

        # 3. Load Previous Day Low (PDL) Cache
        # This hash should be populated by 'fetch_daily_ohlc.py' before market open
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

        logger.info(">>> Algo Worker Loop Started. Waiting for Market Data... <<<")

        # 4. Main Event Loop
        while True:
            try:
                # Blocking read for new messages on both streams
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
                            
                            # Acknowledge processed message
                            r.xack(stream, GROUP_NAME, msg_id)
                            
                        except Exception as e:
                            logger.error(f"Error processing MsgID {msg_id}: {e}")
                            # Optionally: Don't ACK if critical failure, so another worker can retry

            except redis.exceptions.ConnectionError:
                logger.error("Redis Connection Lost. Retrying...")
                import time; time.sleep(5)
            except Exception as e:
                logger.error(f"Unhandled Exception in Loop: {e}")


    # =========================================================================
    # LOGIC 1: PATTERN RECOGNITION (Runs on Candle Close)
    # =========================================================================
    def process_candle(self, data, settings_db, prev_day_data_map):
        """
        Logic:
        1. Parse 1-min Candle.
        2. Check Strategy: OPEN > PDL and CLOSE < PDL (Breakdown).
        3. Check Limits (Max trades).
        4. Calculate Entry/SL/Target/Qty.
        5. Save to DB as 'PENDING'.
        """
        try:
            # Handle Redis byte data
            payload_str = data[b'data'].decode('utf-8') if b'data' in data else data['data']
            payload = json.loads(payload_str)
        except Exception as e:
            logger.error(f"Candle Data Parse Error: {e}")
            return

        symbol = payload['symbol']
        
        # 1. Get Context (PDL)
        pdl_info = prev_day_data_map.get(symbol)
        if not pdl_info:
            return # No historical data for this symbol, cannot trade

        pdl = float(pdl_info['low'])
        open_p = float(payload['open'])
        high_p = float(payload['high'])
        low_p = float(payload['low'])
        close_p = float(payload['close'])
        ts_str = payload['ts']

        # 2. Strategy Condition: Cash Breakdown
        # Candle opened above PDL and closed below PDL
        if open_p > pdl and close_p < pdl:
            
            # 3. Check Constraints
            today = timezone.now().date()
            
            # Check A: Already a trade for this symbol today?
            if StrategyTrade.objects.filter(symbol=symbol, created_at__date=today).exists():
                return

            # Check B: Max Trades per day globally?
            daily_count = StrategyTrade.objects.filter(created_at__date=today).count()
            if daily_count >= settings_db.max_trades_per_day:
                return

            # 4. Calculations
            # Entry: Slightly below the candle low
            entry_level = low_p * 0.9998 
            # Stop Loss: Slightly above the candle high
            stop_loss = high_p * 1.0002 
            
            risk_per_share = stop_loss - entry_level
            if risk_per_share <= 0: return # Invalid candle

            # Qty Calculation based on Risk Amount
            risk_amount = float(settings_db.risk_per_trade_amount)
            qty = int(risk_amount / risk_per_share)
            if qty < 1: qty = 1
            
            # Target Calculation (Risk:Reward)
            rr_ratio = float(settings_db.risk_reward_ratio) # e.g., 2.5
            target_price = entry_level - (risk_per_share * rr_ratio)

            # 5. Commit to Database
            # Status is 'PENDING'. It waits for a Tick to cross 'entry_level'
            StrategyTrade.objects.create(
                symbol=symbol,
                status='PENDING',
                candle_timestamp=datetime.fromisoformat(ts_str),
                candle_open=open_p,
                candle_high=high_p,
                candle_low=low_p,
                candle_close=close_p,
                prev_day_low=pdl,
                entry_level=entry_level,
                stop_loss=stop_loss,
                target_price=target_price,
                quantity=qty
            )
            
            logger.info(f"Strategy SIGNAL: {symbol} Breakdown below {pdl}. Monitoring Entry < {entry_level}")


    # =========================================================================
    # LOGIC 2: EXECUTION & MANAGEMENT (Runs on Every Tick)
    # =========================================================================
    def process_tick(self, data, fyers, settings_db):
        """
        Logic:
        1. Check PENDING trades -> If LTP <= Entry -> Place SELL Order.
        2. Check OPEN trades -> If LTP >= SL or LTP <= Target -> Place BUY Order.
        3. TSL Logic -> If Profit > 1.25R -> Move SL to Entry.
        """
        try:
            symbol = data[b'symbol'].decode('utf-8')
            ltp = float(data[b'ltp'])
        except KeyError:
            return

        # --- A. ENTRY LOGIC (For PENDING Trades) ---
        # We fetch only 'PENDING' trades for this symbol
        pending_trades = StrategyTrade.objects.filter(symbol=symbol, status='PENDING')
        
        for trade in pending_trades:
            # Check Entry Condition (Short Sell)
            if ltp <= float(trade.entry_level):
                logger.info(f"ENTRY TRIGGER: {symbol} LTP {ltp} crossed {trade.entry_level}")
                
                # Place API Order
                order_id = self.place_fyers_order(
                    fyers=fyers,
                    symbol=symbol,
                    qty=trade.quantity,
                    side=-1, # SELL
                    type=2   # MARKET
                )

                if order_id:
                    # Update DB immediately to avoid double firing
                    trade.status = 'PENDING_ENTRY' 
                    trade.entry_order_id = order_id
                    trade.save()
                    logger.info(f"Placed Entry Order {order_id} for {symbol}")
                else:
                    trade.status = 'FAILED'
                    trade.save()
                    logger.error(f"Entry Order Failed for {symbol}")

        # --- B. EXIT & TSL LOGIC (For OPEN Trades) ---
        # Note: Trades become 'OPEN' only after 'run_order_socket' confirms the fill
        open_trades = StrategyTrade.objects.filter(symbol=symbol, status='OPEN')
        
        for trade in open_trades:
            sl = float(trade.stop_loss)
            target = float(trade.target_price)
            entry_price = float(trade.actual_entry_price or trade.entry_level)

            # 1. Exit Conditions
            exit_signal = False
            exit_reason = ""

            # Check Hard Stop Loss (Short: Price goes UP)
            if ltp >= sl:
                exit_signal = True
                exit_reason = "Stop Loss Hit"
            
            # Check Target (Short: Price goes DOWN)
            elif ltp <= target:
                exit_signal = True
                exit_reason = "Target Hit"

            if exit_signal:
                logger.info(f"EXIT TRIGGER: {symbol} ({exit_reason}) LTP: {ltp}")
                
                # Place API Order (Buy to Cover)
                order_id = self.place_fyers_order(
                    fyers=fyers,
                    symbol=symbol,
                    qty=trade.quantity,
                    side=1, # BUY
                    type=2  # MARKET
                )

                if order_id:
                    trade.status = 'PENDING_EXIT'
                    trade.exit_order_id = order_id
                    trade.exit_reason = exit_reason
                    trade.save()
                    logger.info(f"Placed Exit Order {order_id} for {symbol}")
                
                # Continue loop, don't process TSL if exiting
                continue 

            # 2. Breakeven TSL Logic
            # If we haven't moved SL yet, and profit is sufficient
            if not trade.is_breakeven_moved:
                risk_per_share = sl - entry_price
                current_profit_per_share = entry_price - ltp
                
                # Calculate required move (e.g., 1.25 * Risk)
                trigger_r = float(settings_db.breakeven_trigger_r) # 1.25
                required_move = risk_per_share * trigger_r

                if current_profit_per_share >= required_move:
                    # Move SL to Entry Price
                    trade.stop_loss = entry_price
                    trade.is_breakeven_moved = True
                    trade.save()
                    logger.info(f"TSL UPDATE: {symbol} Profit > {trigger_r}R. SL moved to Breakeven ({entry_price})")


    # =========================================================================
    # HELPER: API WRAPPER
    # =========================================================================
    def place_fyers_order(self, fyers, symbol, qty, side, type, limit_price=0, stop_price=0):
        """
        Wrapper for Fyers V3 'place_order'.
        Side: 1=Buy, -1=Sell
        Type: 1=Limit, 2=Market, 3=Stop, 4=StopLimit
        """
        data = {
            "symbol": symbol,
            "qty": int(qty),
            "type": type,
            "side": side,
            "productType": "INTRADAY",
            "limitPrice": float(limit_price),
            "stopPrice": float(stop_price),
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False
        }

        try:
            response = fyers.place_order(data=data)
            
            # Fyers V3 Success Response:
            # {'s': 'ok', 'code': 1101, 'message': 'Order submitted', 'id': '123456'}
            if isinstance(response, dict) and response.get('s') == 'ok':
                return response.get('id')
            else:
                logger.error(f"Fyers API Error: {response}")
                return None
        except Exception as e:
            logger.error(f"Fyers API Exception: {e}")
            return None