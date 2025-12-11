from django.contrib import admin
from django.urls import path, include
from apps.dashboard.api_urls import dashboard_urlpatterns

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include('apps.dashboard.api_urls')),
    path('webhook/', include('apps.bot.webhook_urls')),
    path('', include(dashboard_urlpatterns)),
]
