from django.urls import path
from .views import chart_today, dashboard, chart_archives, chart_detail, stats_overview

urlpatterns = [
    path('chart/today', chart_today, name='chart_today'),
    path('chart/archives', chart_archives, name='chart_archives'),
    path('chart/<int:chart_id>', chart_detail, name='chart_detail'),
    path('stats', stats_overview, name='stats_overview'),
]

# Dashboard page URLs (served from main urls.py)
dashboard_urlpatterns = [
    path('', dashboard, name='dashboard'),
]
