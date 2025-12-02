from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect

urlpatterns = [
    # 1. Admin Panel
    path('admin/', admin.site.urls),
    
    # 2. Connect the Trading App URLs
    path('trading/', include('trading.urls')),
    
    # 3. Root Redirect (Critical Fix)
    # When user hits "https://yourapp.herokuapp.com/", redirect them to ".../trading/dashboard/"
    path('', lambda request: redirect('trading:dashboard', permanent=False)),
]