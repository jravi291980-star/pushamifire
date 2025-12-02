import json
import redis
from django.conf import settings
from django.core.management.base import BaseCommand
from trading.models import LiveScanResult

r = redis.from_url(settings.REDIS_URL)

class Command(BaseCommand):
    help = 'Runs the Live Scanner'

    def handle(self, *args, **options):
        # Uses a separate group to read same data independently
        group = "SCANNER_GROUP"
        stream = "candle_stream_1m"
        
        try:
            r.xgroup_create(stream, group, id='0', mkstream=True)
        except: pass
        
        while True:
            events = r.xreadgroup(group, "SCANNER_1", {stream: '>'}, count=5, block=2000)
            if not events: continue
            
            for _, messages in events:
                for msg_id, data in messages:
                    payload = json.loads(data['data'])
                    symbol = payload['symbol']
                    
                    # Logic: Just identifying the pattern
                    # Simulating PDL check
                    pdl = 2000 # Mock
                    open_p = float(payload['open'])
                    close_p = float(payload['close'])
                    
                    if open_p > pdl and close_p < pdl:
                        LiveScanResult.objects.create(
                            symbol=symbol,
                            pattern="Cash Breakdown (Open > PDL > Close)"
                        )
                        # Keep DB small
                        if LiveScanResult.objects.count() > 100:
                            LiveScanResult.objects.first().delete()
                            
                    r.xack(stream, group, msg_id)