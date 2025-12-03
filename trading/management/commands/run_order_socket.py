# import redis
# import logging
# import ssl
# import time
# import os 
# import threading # Added for Redis Listener
# from django.conf import settings
# from django.core.management.base import BaseCommand
# from trading.models import FyersCredentials, StrategyTrade
# from fyers_apiv3.FyersWebsocket import order_ws

# logger = logging.getLogger('order_socket')

# # --- SSL FIX ---
# if settings.REDIS_URL.startswith('rediss://'):
#     r = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
# else:
#     r = redis.from_url(settings.REDIS_URL)

# class Command(BaseCommand):
#     help = 'Runs Fyers V3 Order Socket with Instant Token Reload'

#     def handle(self, *args, **options):
#         # --- 1. Background Listener for New Tokens ---
#         # This thread waits for the Dashboard to say "Token Updated!"
#         # When that happens, it restarts this worker immediately.
#         def listen_for_token_update():
#             try:
#                 # Create a dedicated connection for subscription
#                 if settings.REDIS_URL.startswith('rediss://'):
#                     r_sub = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
#                 else:
#                     r_sub = redis.from_url(settings.REDIS_URL)
                
#                 pubsub = r_sub.pubsub()
#                 pubsub.subscribe('fyers_token_update')
#                 logger.info("🎧 Listening for Token Updates from Redis...")

#                 for message in pubsub.listen():
#                     if message['type'] == 'message':
#                         logger.info("✅ New Token Signal Received! Restarting Worker to apply changes...")
#                         os._exit(0) # Clean exit to trigger restart & DB fetch
#             except Exception as e:
#                 logger.error(f"Redis Listener Error: {e}")

#         # Start the listener thread (Daemon dies when main process dies)
#         threading.Thread(target=listen_for_token_update, daemon=True).start()

#         # --- 2. Main Socket Loop ---
#         while True:
#             logger.info(">>> Initializing Order Socket...")
            
#             # Fetch Latest Token from Database
#             try:
#                 creds = FyersCredentials.objects.get(is_active=True)
#                 token = creds.access_token
#                 logger.info(f"Using Token: {token[:10]}...") 
#             except FyersCredentials.DoesNotExist:
#                 logger.error("No active credentials. Retrying in 10s...")
#                 time.sleep(10)
#                 continue

#             def on_order(message):
#                 order_id = message.get('id')
#                 status = message.get('status')
#                 price = float(message.get('tradedPrice', 0) or 0)
                
#                 if not order_id: return
#                 logger.info(f"Order Update: ID={order_id} Status={status}")

#                 try:
#                     # Entry Reconcile
#                     t_entry = StrategyTrade.objects.filter(entry_order_id=order_id).first()
#                     if t_entry:
#                         if status == 2:
#                             t_entry.status = 'OPEN'; t_entry.actual_entry_price = price; t_entry.save()
#                             logger.info(f"ENTRY CONFIRMED: {t_entry.symbol}")
#                         elif status in [1, 5]:
#                             t_entry.status = 'FAILED'; t_entry.save()
#                         return

#                     # Exit Reconcile
#                     t_exit = StrategyTrade.objects.filter(exit_order_id=order_id).first()
#                     if t_exit:
#                         if status == 2:
#                             t_exit.status = 'CLOSED'; t_exit.actual_exit_price = price
#                             entry_p = float(t_exit.actual_entry_price or t_exit.entry_level)
#                             t_exit.pnl = (entry_p - price) * t_exit.quantity
#                             t_exit.save()
#                             logger.info(f"EXIT CONFIRMED: {t_exit.symbol}")
#                         elif status in [1, 5]:
#                             t_exit.status = 'OPEN'; t_exit.exit_order_id = None; t_exit.save()
#                             logger.warning(f"Exit Failed for {t_exit.symbol}. Reverted to OPEN.")
#                 except Exception as e:
#                     logger.error(f"DB Error: {e}")

#             def on_error(msg):
#                 logger.error(f"Socket Error: {msg}")
#                 # If Forbidden (403), the token is dead. 
#                 # Kill process to force reload from DB.
#                 if '403' in str(msg) or 'Forbidden' in str(msg):
#                     logger.critical("TOKEN EXPIRED. Restarting...")
#                     os._exit(1) 

#             def on_open():
#                 logger.info("Socket Connected. Subscribing...")
#                 fyers_socket.subscribe(data_type="OnOrders")
#                 fyers_socket.keep_running()

#             try:
#                 fyers_socket = order_ws.FyersOrderSocket(
#                     access_token=token, 
#                     write_to_file=False, 
#                     log_path="",
#                     on_connect=on_open, 
#                     on_error=on_error, 
#                     on_orders=on_order
#                 )
#                 fyers_socket.connect()
#             except Exception as e:
#                 logger.error(f"Socket Exception: {e}")
#                 time.sleep(5)
import redis
import logging
import ssl
import time
import os
import threading
from multiprocessing import Process
from django.conf import settings
from django.core.management.base import BaseCommand
from trading.models import FyersCredentials, StrategyTrade
from fyers_apiv3.FyersWebsocket import order_ws

logger = logging.getLogger('order_socket')

# --- SSL FIX ---
def get_redis():
    if settings.REDIS_URL.startswith('rediss://'):
        return redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
    return redis.from_url(settings.REDIS_URL)

class Command(BaseCommand):
    help = 'Runs Fyers Order Socket with Supervisor Process'

    def handle(self, *args, **options):
        # --- SUPERVISOR LOOP ---
        # This loop runs in the Main Process. It spawns the socket in a Child Process.
        while True:
            logger.info("[Supervisor] Starting Order Socket Child Process...")
            p = Process(target=self.run_socket_process)
            p.start()
            p.join() # Wait for child to exit

            if p.exitcode == 0:
                logger.info("[Supervisor] Socket requested restart (Token Update). Restarting immediately.")
            else:
                logger.error("[Supervisor] Socket crashed. Restarting in 5s...")
                time.sleep(5)

    def run_socket_process(self):
        # --- CHILD PROCESS LOGIC ---
        # 1. Redis Listener Thread (For Hot Reload)
        def listen_for_token_update():
            try:
                r_sub = get_redis()
                pubsub = r_sub.pubsub()
                pubsub.subscribe('fyers_token_update')
                logger.info("🎧 Listening for Token Updates...")
                for message in pubsub.listen():
                    if message['type'] == 'message':
                        logger.info("✅ New Token Signal! Exiting to reload...")
                        os._exit(0) # Exit with 0 tells Supervisor to restart instantly
            except Exception as e:
                logger.error(f"Listener Error: {e}")

        threading.Thread(target=listen_for_token_update, daemon=True).start()

        # 2. Main Socket Logic
        try:
            # Need to re-import Django models inside process if needed, 
            # but since we are in a management command class, DB connections should be handled.
            # We explicitly close old connections to be safe in a new process.
            from django.db import connections
            connections.close_all()
            
            try:
                creds = FyersCredentials.objects.get(is_active=True)
                token = creds.access_token
                logger.info(f"🔑 Loaded Token: {token[:10]}...")
            except Exception:
                logger.error("No Credentials. Sleeping...")
                time.sleep(10)
                os._exit(1)

            def on_order(message):
                order_id = message.get('id')
                status = message.get('status')
                price = float(message.get('tradedPrice', 0) or 0)
                if not order_id: return
                logger.info(f"Order Update: ID={order_id} Status={status}")
                try:
                    t_entry = StrategyTrade.objects.filter(entry_order_id=order_id).first()
                    if t_entry:
                        if status == 2: t_entry.status = 'OPEN'; t_entry.actual_entry_price = price; t_entry.save()
                        elif status in [1, 5]: t_entry.status = 'FAILED'; t_entry.save()
                        return
                    t_exit = StrategyTrade.objects.filter(exit_order_id=order_id).first()
                    if t_exit:
                        if status == 2: 
                            t_exit.status = 'CLOSED'; t_exit.actual_exit_price = price
                            entry = float(t_exit.actual_entry_price or t_exit.entry_level)
                            t_exit.pnl = (entry - price) * t_exit.quantity
                            t_exit.save()
                        elif status in [1, 5]: 
                            t_exit.status = 'OPEN'; t_exit.exit_order_id = None; t_exit.save()
                except Exception as e: logger.error(f"DB Error: {e}")

            def on_error(msg):
                logger.error(f"Socket Error: {msg}")
                if '403' in str(msg) or 'Forbidden' in str(msg):
                    logger.critical("⛔ Token Expired (403). Exiting...")
                    os._exit(0) # Exit 0 to force immediate reload

            def on_open():
                logger.info("Socket Connected. Subscribing...")
                fyers_socket.subscribe(data_type="OnOrders")
                fyers_socket.keep_running()

            fyers_socket = order_ws.FyersOrderSocket(
                access_token=token, write_to_file=False, log_path="",
                on_connect=on_open, on_error=on_error, on_orders=on_order
            )
            fyers_socket.connect()

        except Exception as e:
            logger.error(f"Process Exception: {e}")
            os._exit(1)