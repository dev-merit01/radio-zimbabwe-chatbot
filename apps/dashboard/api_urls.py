from django.urls import path
from .views import chart_today, dashboard, chart_archives, chart_detail, stats_overview

from . import desktop, workspace

urlpatterns = [
    path("desktop/status", desktop.status),
    path("workspace/overview", workspace.overview),
    path("workspace/songs", workspace.songs),
    path("workspace/songs/add", workspace.add_song),
    path("workspace/songs/<int:song_id>/review", workspace.review),
    path("workspace/incoming", workspace.incoming),
    path("workspace/health", workspace.health),
    path("workspace/retry", workspace.retry_failed),
    path("workspace/audit", workspace.audit),
    path("workspace/publish", workspace.publish),
    path("workspace/export", workspace.export_chart),
    path('chart/today', chart_today, name='chart_today'),
    path('chart/archives', chart_archives, name='chart_archives'),
    path('chart/<int:chart_id>', chart_detail, name='chart_detail'),
    path('stats', stats_overview, name='stats_overview'),
]

dashboard_urlpatterns = [path('', dashboard, name='dashboard')]
