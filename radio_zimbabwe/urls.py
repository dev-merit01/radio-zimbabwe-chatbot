from django.contrib import admin
from django.urls import path, include
from apps.dashboard.api_urls import dashboard_urlpatterns
from apps.dashboard.readiness import readiness

admin.site.site_header = "AirVote administration"
admin.site.site_title = "AirVote"
admin.site.index_title = "Administration"

urlpatterns = [
    path("healthz/", readiness),
    path("api/voting-bridge/", include("apps.public_voting.urls")),
    path('admin/', admin.site.urls),
    path("licensing/", include("apps.licensing.urls")),
    path('accounts/', include('apps.accounts.urls')),
    path('api/', include('apps.dashboard.api_urls')),
    path('webhook/', include('apps.bot.webhook_urls')),
    path('', include(dashboard_urlpatterns)),
]
