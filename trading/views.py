# from django.shortcuts import render, redirect, get_object_or_404
# from django.contrib.auth.decorators import login_required, user_passes_test
# from django.contrib import messages
# from django.conf import settings
# from .models import FyersCredentials, StrategyTrade, LiveScanResult, GlobalTradingSettings
# from .forms import GlobalSettingsForm
# # We don't strictly need fyers_auth_util for generating the URL if we construct it manually, 
# # but we keep it for token exchange.
# from .fyers_auth_util import exchange_auth_code_for_token

# def superuser_required(function=None):
#     return user_passes_test(lambda u: u.is_active and u.is_superuser)(function)

# @login_required
# @superuser_required
# def dashboard_view(request):
#     creds, _ = FyersCredentials.objects.get_or_create(user=request.user)
#     settings_obj, _ = GlobalTradingSettings.objects.get_or_create(user=request.user)
    
#     # Use the Callback URL from settings (Heroku Config)
#     callback_url = settings.FYERS_CALLBACK_URL

#     if request.method == 'POST':
#         # --- 1. Handle API Credentials Save ---
#         if 'save_credentials' in request.POST:
#             app_id = request.POST.get('app_id')
#             secret_key = request.POST.get('secret_key')
            
#             if app_id and secret_key:
#                 creds.app_id = app_id
#                 creds.secret_key = secret_key
#                 creds.save()
#                 messages.success(request, "API Credentials Saved Successfully.")
#             else:
#                 messages.error(request, "App ID and Secret Key cannot be empty.")
#             return redirect('trading:dashboard')

#         # --- 2. Handle Strategy Settings Update ---
#         elif 'update_settings' in request.POST:
#             form = GlobalSettingsForm(request.POST, instance=settings_obj)
#             if form.is_valid():
#                 form.save()
#                 messages.success(request, "Strategy Risk Settings Updated")
#             return redirect('trading:dashboard')

#         # --- 3. Handle Manual Square Off ---
#         elif 'square_off' in request.POST:
#             trade_id = request.POST.get('trade_id')
#             try:
#                 trade = StrategyTrade.objects.get(id=trade_id)
#                 trade.status = 'PENDING_EXIT'
#                 trade.exit_reason = 'Manual Square Off'
#                 trade.save()
#                 messages.warning(request, f"Square Off Triggered for {trade.symbol}")
#             except StrategyTrade.DoesNotExist:
#                 messages.error(request, "Trade not found.")
#             return redirect('trading:dashboard')

#     else:
#         form = GlobalSettingsForm(instance=settings_obj)

#     # --- GENERATE AUTH URL (V3 FIX) ---
#     # The V2 endpoint is deprecated. We must use /api/v3/generate-authcode
#     auth_url = "#"
#     if creds.app_id:
#         auth_url = f"https://api.fyers.in/api/v3/generate-authcode?client_id={creds.app_id}&redirect_uri={callback_url}&response_type=code&state=sample_state"

#     trades = StrategyTrade.objects.all().order_by('-created_at')[:20]
#     scans = LiveScanResult.objects.all()[:10]

#     context = {
#         'credentials': creds,
#         'auth_url': auth_url,
#         'callback_url': callback_url,
#         'settings_form': form,
#         'trades': trades,
#         'scans': scans,
#     }
#     return render(request, 'trading/dashboard.html', context)

# @login_required
# @superuser_required
# def fyers_callback_view(request):
#     """
#     Handles the redirect from Fyers after login.
#     """
#     auth_code = request.GET.get('auth_code')
    
#     if not auth_code:
#         # Sometimes Fyers returns errors in the URL parameters
#         error_msg = request.GET.get('message') or "No Auth Code received."
#         messages.error(request, f"Authentication Failed: {error_msg}")
#         return redirect('trading:dashboard')

#     try:
#         creds = FyersCredentials.objects.get(user=request.user)
#     except FyersCredentials.DoesNotExist:
#         messages.error(request, "Please save your App ID and Secret in the dashboard first.")
#         return redirect('trading:dashboard')

#     # Exchange Code for Token
#     access_token = exchange_auth_code_for_token(
#         auth_code=auth_code,
#         app_id=creds.app_id,
#         secret_key=creds.secret_key,
#         callback_url=settings.FYERS_CALLBACK_URL
#     )

#     if access_token:
#         creds.access_token = access_token
#         creds.is_active = True
#         creds.save()
#         messages.success(request, "Fyers Connected! Token Generated.")
#     else:
#         messages.error(request, "Failed to generate Access Token. Check App ID/Secret.")

#     return redirect('trading:dashboard')
import urllib.parse
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.conf import settings
from .models import FyersCredentials, StrategyTrade, LiveScanResult, GlobalTradingSettings
from .forms import GlobalSettingsForm
from .fyers_auth_util import generate_auth_url, exchange_auth_code_for_token

def superuser_required(function=None):
    return user_passes_test(lambda u: u.is_active and u.is_superuser)(function)

@login_required
@superuser_required
def dashboard_view(request):
    creds, _ = FyersCredentials.objects.get_or_create(user=request.user)
    settings_obj, _ = GlobalTradingSettings.objects.get_or_create(user=request.user)
    callback_url = settings.FYERS_CALLBACK_URL

    if request.method == 'POST':
        if 'save_credentials' in request.POST:
            app_id = request.POST.get('app_id')
            secret_key = request.POST.get('secret_key')
            if app_id and secret_key:
                creds.app_id = app_id
                creds.secret_key = secret_key
                creds.save()
                messages.success(request, "API Credentials Saved.")
            return redirect('trading:dashboard')

        elif 'update_settings' in request.POST:
            form = GlobalSettingsForm(request.POST, instance=settings_obj)
            if form.is_valid():
                form.save()
                messages.success(request, "Risk Settings Updated")
            return redirect('trading:dashboard')

        elif 'square_off' in request.POST:
            trade_id = request.POST.get('trade_id')
            try:
                trade = StrategyTrade.objects.get(id=trade_id)
                trade.status = 'PENDING_EXIT'
                trade.exit_reason = 'Manual Square Off'
                trade.save()
                messages.warning(request, f"Square Off Triggered: {trade.symbol}")
            except StrategyTrade.DoesNotExist:
                pass
            return redirect('trading:dashboard')
    else:
        form = GlobalSettingsForm(instance=settings_obj)

    auth_url = "#"
    if creds.app_id:
        encoded_callback = urllib.parse.quote(callback_url, safe='')
        auth_url = f"https://api.fyers.in/api/v3/generate-authcode?client_id={creds.app_id}&redirect_uri={encoded_callback}&response_type=code&state=sample_state"

    trades = StrategyTrade.objects.all().order_by('-created_at')[:20]
    scans = LiveScanResult.objects.all()[:10]

    context = { 'credentials': creds, 'auth_url': auth_url, 'callback_url': callback_url, 'settings_form': form, 'trades': trades, 'scans': scans }
    return render(request, 'trading/dashboard.html', context)

@login_required
@superuser_required
def fyers_callback_view(request):
    auth_code = request.GET.get('auth_code')
    if not auth_code:
        messages.error(request, "Auth Failed: No Code Received.")
        return redirect('trading:dashboard')

    try:
        creds = FyersCredentials.objects.get(user=request.user)
    except FyersCredentials.DoesNotExist:
        return redirect('trading:dashboard')

    # Pass the 'creds' OBJECT here, so the utility can save the token BEFORE publishing
    token = exchange_auth_code_for_token(auth_code, creds, settings.FYERS_CALLBACK_URL)

    if token:
        messages.success(request, "Connected Successfully!")
    else:
        messages.error(request, "Token Exchange Failed.")

    return redirect('trading:dashboard')