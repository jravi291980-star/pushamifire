from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone

class FyersCredentials(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    app_id = models.CharField(max_length=255)
    secret_key = models.CharField(max_length=255)
    access_token = models.TextField(blank=True, null=True)
    is_active = models.BooleanField(default=False)
    updated_at = models.DateTimeField(auto_now=True)

class GlobalTradingSettings(models.Model):
    """Controls global risk parameters for the strategy."""
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    max_trades_per_day = models.IntegerField(default=10)
    max_trades_per_symbol = models.IntegerField(default=2)
    risk_per_trade_amount = models.DecimalField(max_digits=10, decimal_places=2, default=500.0)
    volume_threshold = models.BigIntegerField(default=500000, help_text="Min volume * price to trade")
    
    # Strategy specific hardcodes (made editable here)
    risk_reward_ratio = models.DecimalField(max_digits=4, decimal_places=2, default=2.5) # 1:2.5
    breakeven_trigger_r = models.DecimalField(max_digits=4, decimal_places=2, default=1.25) # 1.25 R
    
    def __str__(self):
        return f"Settings for {self.user.username}"

class StrategyTrade(models.Model):
    """
    The specific model for Cash Breakdown Strategy.
    """
    STATUS_CHOICES = (
        ('PENDING', 'Monitoring (Wait for Break)'),
        ('PENDING_ENTRY', 'Order Placed'),
        ('OPEN', 'Position Open'),
        ('PENDING_EXIT', 'Exit Order Placed'),
        ('CLOSED', 'Closed'),
        ('EXPIRED', 'Setup Expired'),
        ('FAILED', 'Failed'),
    )

    symbol = models.CharField(max_length=50)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    
    # Candle Data that triggered the setup
    candle_timestamp = models.DateTimeField()
    candle_open = models.DecimalField(max_digits=10, decimal_places=2)
    candle_high = models.DecimalField(max_digits=10, decimal_places=2)
    candle_low = models.DecimalField(max_digits=10, decimal_places=2)
    candle_close = models.DecimalField(max_digits=10, decimal_places=2)
    prev_day_low = models.DecimalField(max_digits=10, decimal_places=2)
    
    # Trade Parameters
    entry_level = models.DecimalField(max_digits=10, decimal_places=2) # Trigger price
    stop_loss = models.DecimalField(max_digits=10, decimal_places=2)
    target_price = models.DecimalField(max_digits=10, decimal_places=2)
    quantity = models.IntegerField(default=0)
    
    # Execution Details
    entry_order_id = models.CharField(max_length=50, blank=True, null=True)
    exit_order_id = models.CharField(max_length=50, blank=True, null=True)
    actual_entry_price = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    actual_exit_price = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    
    # Management
    is_breakeven_moved = models.BooleanField(default=False)
    pnl = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    exit_reason = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return f"{self.symbol} - {self.status}"

class LiveScanResult(models.Model):
    symbol = models.CharField(max_length=50)
    scan_time = models.DateTimeField(auto_now_add=True)
    pattern = models.CharField(max_length=255)
    
    class Meta:
        ordering = ['-scan_time']