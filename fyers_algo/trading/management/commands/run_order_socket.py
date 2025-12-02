import redis
import logging
from django.conf import settings
from django.core.management.base import BaseCommand
from trading.models import FyersCredentials, StrategyTrade
from fyers_apiv3.FyersWebsocket import order_ws

logger = logging.getLogger('order_socket')
r = redis.from_url(settings.REDIS_URL)

class Command(BaseCommand):
    help = 'Runs Fyers V3 Order Socket for Real-time Trade Updates'

    def handle(self, *args, **options):
        try:
            creds = FyersCredentials.objects.get(is_active=True)
            access_token = creds.access_token
        except FyersCredentials.DoesNotExist:
            logger.error("No active credentials.")
            return

        def on_trade(message):
            """
            Triggered when a trade occurs (Fill/Partial Fill).
            """
            logger.info(f"Trade Update: {message}")
            # Use this to calculate precise PnL logs if needed
            pass

        def on_order(message):
            """
            Triggered on order status change (PENDING, FILLED, CANCELLED, REJECTED).
            
            Sample Message:
            {
                'id': '123456', 'symbol': 'NSE:SBIN-EQ', 'qty': 10, 
                'tradedPrice': 500.50, 'status': 2 (Filled), 'limitPrice': 0, ...
            }
            Status Codes: 1: Cancelled, 2: Traded/Filled, 5: Rejected, 6: Pending
            """
            logger.info(f"Order Update: {message}")
            
            order_id = message.get('id')
            status_code = message.get('status')
            traded_price = float(message.get('tradedPrice', 0))
            
            if not order_id: return

            try:
                # 1. Check if this is an ENTRY order
                trade = StrategyTrade.objects.filter(entry_order_id=order_id).first()
                if trade:
                    if status_code == 2: # FILLED
                        trade.status = 'OPEN'
                        trade.actual_entry_price = traded_price
                        trade.save()
                        logger.info(f"Trade {trade.symbol} is now OPEN @ {traded_price}")
                    elif status_code in [1, 5]: # CANCELLED / REJECTED
                        trade.status = 'FAILED'
                        trade.save()
                    return

                # 2. Check if this is an EXIT order
                trade = StrategyTrade.objects.filter(exit_order_id=order_id).first()
                if trade:
                    if status_code == 2: # FILLED
                        trade.status = 'CLOSED'
                        trade.actual_exit_price = traded_price
                        # Calculate Final PnL
                        if trade.actual_entry_price:
                            # Short Strategy: Entry - Exit
                            trade.pnl = (trade.actual_entry_price - traded_price) * trade.quantity
                        trade.save()
                        logger.info(f"Trade {trade.symbol} CLOSED @ {traded_price}. PnL: {trade.pnl}")
                    elif status_code in [1, 5]: # CANCELLED / REJECTED
                        # Revert to OPEN so Algo Worker can retry exit?
                        trade.status = 'OPEN' 
                        trade.exit_order_id = None # Clear failed ID
                        trade.save()
                        logger.error(f"Exit Order for {trade.symbol} failed! Reverting to OPEN.")

            except Exception as e:
                logger.error(f"Error updating DB from Order Socket: {e}")

        def on_error(message):
            logger.error(f"Order Socket Error: {message}")

        def on_open():
            logger.info("Order Socket Connected. Subscribing...")
            fyers_socket.subscribe(data_type="OnOrders,OnTrades")
            fyers_socket.keep_running()

        fyers_socket = order_ws.FyersOrderSocket(
            access_token=access_token,
            write_to_file=False,
            log_path="",
            on_connect=on_open,
            on_error=on_error,
            on_orders=on_order,
            on_trades=on_trade
        )
        fyers_socket.connect()