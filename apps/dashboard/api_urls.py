from django.urls import path
from .views import chart_today, dashboard, chart_archives, chart_detail, stats_overview

from . import desktop, workspace, show_management, eligibility, presenter_management

urlpatterns = [
    path("desktop/status", desktop.status),
    path("workspace/eligibility", eligibility.list_editions),
    path("workspace/eligibility/create", eligibility.create_edition),
    path("workspace/eligibility/candidate", eligibility.edit_candidate),
    path("workspace/eligibility/suggest", eligibility.suggest_song),
    path("workspace/eligibility/submit", eligibility.submit_edition),
    path("workspace/eligibility/review", eligibility.review_edition),
    path("workspace/eligibility/suggestion/review", eligibility.review_suggestion),
    path("workspace/presenters", presenter_management.list_presenters),
    path("workspace/presenters/create", presenter_management.create_presenter),
    path("workspace/presenters/assign", presenter_management.assign_presenter),
    path("workspace/shows", show_management.list_shows),
    path("workspace/shows/save", show_management.save_show),
    path("workspace/shows/archive", show_management.archive_show),
    path("workspace/shows/routing", show_management.set_routing_policy),
    path("workspace/shows/multiplier", show_management.set_station_multiplier),
    path("workspace/unassigned", show_management.unassigned_list),
    path("workspace/unassigned/assign", show_management.assign_unassigned),
    path("workspace/unassigned/dismiss", show_management.dismiss_unassigned),
    path("workspace/spam", show_management.suspicious_votes),
    path("workspace/spam/review", show_management.review_suspicious_vote),
    path("workspace/administration", workspace.administration),
    path("workspace/accounts/<int:user_id>/approve", workspace.approve_account),
    path("workspace/station-password", workspace.station_password),
    path("workspace/votes", workspace.submit_vote),
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
