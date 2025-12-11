from django.urls import path
from .views import chart_today, dashboard

urlpatterns = [
    path('chart/today', chart_today, name='chart_today'),
]

# Dashboard page URLs (served from main urls.py)
dashboard_urlpatterns = [
    path('', dashboard, name='dashboard'),
]
