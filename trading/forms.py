from django import forms
from .models import GlobalTradingSettings

class GlobalSettingsForm(forms.ModelForm):
    class Meta:
        model = GlobalTradingSettings
        exclude = ['user']
        widgets = {
            'max_trades_per_day': forms.NumberInput(attrs={'class': 'input-field w-full p-2 rounded'}),
            'risk_per_trade_amount': forms.NumberInput(attrs={'class': 'input-field w-full p-2 rounded'}),
            'risk_reward_ratio': forms.NumberInput(attrs={'class': 'input-field w-full p-2 rounded'}),
        }