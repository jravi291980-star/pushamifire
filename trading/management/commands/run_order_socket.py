import redis
import logging
import ssl
from django.conf import settings
from django.core.management.base import BaseCommand
from trading.models import FyersCredentials, StrategyTrade
from fyers_apiv3.FyersWebsocket import order_ws

logger = logging.getLogger('order_socket')

# --- SSL FIX ---
if settings.REDIS_URL.startswith('rediss://'):
    r = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE)
else:
    r = redis.from_url(settings.REDIS_URL)

class Command(BaseCommand):
    help = 'Runs Fyers V3 Order Socket for Real-time Reconciliation'

    def handle(self, *args, **options):
        logger.info("Starting Order Socket...")
        try:
            creds = FyersCredentials.objects.get(is_active=True)
            token = creds.access_token
        except FyersCredentials.DoesNotExist:
            logger.error("No active credentials found.")
            return

        def on_order(message):
            """
            Handles Order Updates.
            Prevents double-updates by checking current DB status.
            """
            # Fyers V3 Message Format:
            # {'id': '12345', 'status': 2, 'tradedPrice': 500.0, 'qty': 10, ...}
            
            order_id = message.get('id')
            status = message.get('status') 
            # Status Codes: 
            # 1: Cancelled, 2: Traded/Filled, 4: Transit, 5: Rejected, 6: Pending
            
            price = float(message.get('tradedPrice', 0) or 0)
            
            if not order_id: return

            logger.info(f"Order Update: ID={order_id} Status={status} Price={price}")

            try:
                # ---------------------------------------------------------
                # 1. RECONCILE ENTRY
                # ---------------------------------------------------------
                t_entry = StrategyTrade.objects.filter(entry_order_id=order_id).first()
                if t_entry:
                    # Idempotency Check: If already OPEN, ignore "Filled" msg again
                    if t_entry.status == 'OPEN' and status == 2:
                        return 

                    if status == 2: # FILLED
                        t_entry.status = 'OPEN'
                        t_entry.actual_entry_price = price
                        t_entry.save()
                        logger.info(f"CONFIRMED ENTRY: {t_entry.symbol} @ {price}")
                    
                    elif status in [1, 5]: # CANCELLED / REJECTED
                        t_entry.status = 'FAILED'
                        t_entry.save()
                        logger.error(f"ENTRY FAILED: {t_entry.symbol} (Status {status})")
                    return

                # ---------------------------------------------------------
                # 2. RECONCILE EXIT
                # ---------------------------------------------------------
                t_exit = StrategyTrade.objects.filter(exit_order_id=order_id).first()
                if t_exit:
                    # Idempotency Check: If already CLOSED, ignore
                    if t_exit.status == 'CLOSED' and status == 2:
                        return

                    if status == 2: # FILLED
                        t_exit.status = 'CLOSED'
                        t_exit.actual_exit_price = price
                        
                        # Calculate Final PnL (Short: Entry - Exit)
                        # Use actual entry price if available, else fallback to planned level
                        entry_p = float(t_exit.actual_entry_price or t_exit.entry_level)
                        t_exit.pnl = (entry_p - price) * t_exit.quantity
                        t_exit.save()
                        logger.info(f"CONFIRMED EXIT: {t_exit.symbol} @ {price} | PnL: {t_exit.pnl}")

                    elif status in [1, 5]: # CANCELLED / REJECTED
                        # CRITICAL: If exit fails, revert to OPEN so Algo can retry!
                        t_exit.status = 'OPEN'
                        t_exit.exit_order_id = None # Reset ID to allow retry
                        t_exit.exit_reason = f"Order Failed (Status {status})"
                        t_exit.save()
                        logger.error(f"EXIT FAILED: {t_exit.symbol}. Reverting to OPEN.")

            except Exception as e:
                logger.error(f"DB Reconciliation Error: {e}")

        def on_error(msg): 
            logger.error(f"Socket Error: {msg}")
        
        def on_open():
            logger.info("Connected to Order Socket. Listening...")
            fyers_socket.subscribe(data_type="OnOrders")
            fyers_socket.keep_running()

        fyers_socket = order_ws.FyersOrderSocket(
            access_token=token, 
            write_to_file=False, 
            log_path="",
            on_connect=on_open, 
            on_error=on_error, 
            on_orders=on_order
        )
        fyers_socket.connect()