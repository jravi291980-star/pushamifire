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

def get_redis():
    if settings.REDIS_URL.startswith('rediss://'):
        return redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
    return redis.from_url(settings.REDIS_URL)

class Command(BaseCommand):
    help = 'Runs Fyers Order Socket with Supervisor Process'

    def handle(self, *args, **options):
        # Supervisor Loop
        while True:
            logger.info("[Supervisor] Starting Order Socket...")
            p = Process(target=self.run_socket_process)
            p.start()
            p.join()

            if p.exitcode == 0:
                logger.info("[Supervisor] Token Update Detected. Restarting...")
            else:
                logger.error("[Supervisor] Crashed. Retrying in 5s...")
                time.sleep(5)

    def run_socket_process(self):
        # Redis Listener for Hot Reload
        def listen_for_token_update():
            try:
                r_sub = get_redis()
                pubsub = r_sub.pubsub()
                pubsub.subscribe('fyers_token_update')
                for message in pubsub.listen():
                    if message['type'] == 'message':
                        os._exit(0)
            except Exception: pass

        threading.Thread(target=listen_for_token_update, daemon=True).start()

        try:
            from django.db import connections
            connections.close_all()
            
            # 1. FETCH CREDENTIALS
            try:
                creds = FyersCredentials.objects.get(is_active=True)
                raw_token = creds.access_token
                app_id = creds.app_id
                
                # --- CRITICAL FIX: FORMAT TOKEN ---
                # Fyers WebSockets require "APP_ID:ACCESS_TOKEN"
                if ":" not in raw_token:
                    full_token = f"{app_id}:{raw_token}"
                else:
                    full_token = raw_token
                
                logger.info(f"🔑 Connecting with AppID: {app_id}...")

            except Exception:
                logger.error("No Credentials. Sleeping...")
                time.sleep(10)
                os._exit(1)

            def on_order(message):
                # Standard Order Processing Logic...
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
                # 403 means Forbidden (Wrong Token Format or Expired)
                if '403' in str(msg) or 'Forbidden' in str(msg):
                    logger.critical("⛔ 403 FORBIDDEN. Exiting to reload...")
                    os._exit(0)

            def on_open():
                logger.info("Socket Connected. Subscribing...")
                fyers_socket.subscribe(data_type="OnOrders")
                fyers_socket.keep_running()

            # Connect using the CORRECT full_token
            fyers_socket = order_ws.FyersOrderSocket(
                access_token=full_token, 
                write_to_file=False, 
                log_path="",
                on_connect=on_open, 
                on_error=on_error, 
                on_orders=on_order
            )
            fyers_socket.connect()

        except Exception as e:
            logger.error(f"Process Exception: {e}")
            os._exit(1)