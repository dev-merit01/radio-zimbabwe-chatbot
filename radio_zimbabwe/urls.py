from django.contrib import admin
from django.urls import path, include
from apps.dashboard.api_urls import dashboard_urlpatterns
from apps.dashboard.readiness import readiness
from apps.voting import portal

admin.site.site_header = "AirVote administration"
admin.site.site_title = "AirVote"
admin.site.index_title = "Administration"

urlpatterns = [
    path("healthz/", readiness),
    # Public listener portal, separate from private presenter/admin dashboards.
    path("vote/", portal.home, name="public_vote_portal"),
    path("vote/api/stations/", portal.stations, name="public_vote_stations"),
    path("vote/api/shows/<int:show_id>/", portal.show_detail, name="public_vote_show"),
    path("vote/api/verify/start/", portal.start_verification, name="public_verify_start"),
    path("vote/api/verify/check/", portal.check_verification, name="public_verify_check"),
    path("vote/api/cast/", portal.vote, name="public_cast_vote"),
    path('admin/', admin.site.urls),
    path('accounts/', include('apps.accounts.urls')),
    path('api/', include('apps.dashboard.api_urls')),
    path('webhook/', include('apps.bot.webhook_urls')),
    path('', include(dashboard_urlpatterns)),
]
