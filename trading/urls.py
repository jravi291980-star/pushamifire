from django.urls import path
from django.contrib.auth import views as auth_views
from . import views

app_name = 'trading'

urlpatterns = [
    # --- Authentication ---
    # Login view expects a template at 'trading/templates/registration/login.html'
    path('login/', auth_views.LoginView.as_view(template_name='registration/login.html'), name='login'),
    
    # Logout redirects the user back to the login page
    path('logout/', auth_views.LogoutView.as_view(next_page='/trading/login/'), name='logout'),

    # --- Main Interface ---
    # The dashboard view handles both displaying data and processing manual commands (like Square Off)
    path('dashboard/', views.dashboard_view, name='dashboard'),

    # --- Fyers OAuth Flow ---
    # This URL receives the 'auth_code' from Fyers after the user logs in
    # Make sure this matches the Redirect URI in your Fyers App Dashboard
    path('auth/fyers/callback/', views.fyers_callback_view, name='fyers_callback'),
]