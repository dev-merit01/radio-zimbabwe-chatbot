"""Maintenance admin. Day-to-day changes go through the audited v2 workspace."""

from django.contrib import admin
from apps.accounts.context_processors import get_active_station
from .models import (
    User,
    RawVote,
    RawSongTally,
    CleanedSong,
    SongAlias,
    MatchKeyMapping,
    CleanedSongTally,
    VerifiedArtist,
    LLMDecisionLog,
    WeeklyChart,
    WeeklyChartEntry,
    ReviewAudit,
)


class StationReadOnly(admin.ModelAdmin):
    actions = None
    list_per_page = 50

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        if any(f.name == "station" for f in self.model._meta.fields):
            return qs.filter(station=get_active_station(request))
        if self.model is WeeklyChartEntry:
            return qs.filter(chart__station=get_active_station(request))
        return qs

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


for model in (
    User,
    RawVote,
    RawSongTally,
    CleanedSong,
    MatchKeyMapping,
    CleanedSongTally,
    LLMDecisionLog,
    WeeklyChart,
    WeeklyChartEntry,
    ReviewAudit,
):
    admin.site.register(model, StationReadOnly)


@admin.register(VerifiedArtist)
class ArtistAdmin(admin.ModelAdmin):
    list_display = ("name", "genre", "is_active")
    search_fields = ("name",)

    def has_module_permission(self, request):
        return request.user.is_superuser

    def has_view_permission(self, request, obj=None):
        return request.user.is_superuser

    def has_add_permission(self, request):
        return request.user.is_superuser

    def has_change_permission(self, request, obj=None):
        return request.user.is_superuser

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        from .matching import clear_artist_cache

        clear_artist_cache()


@admin.register(SongAlias)
class SongAliasAdmin(admin.ModelAdmin):
    """Only superusers may approve catalogue aliases; never trust AI guesses."""
    list_display = ("value", "kind", "station", "song", "is_approved", "source")
    list_filter = ("station", "kind", "is_approved")
    search_fields = ("value", "song__canonical_name")
    readonly_fields = ("normalised", "created_at")

    def has_module_permission(self, request):
        return request.user.is_superuser

    def has_view_permission(self, request, obj=None):
        return request.user.is_superuser

    def has_add_permission(self, request):
        return request.user.is_superuser

    def has_change_permission(self, request, obj=None):
        return request.user.is_superuser

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser

    def save_model(self, request, obj, form, change):
        # The song is authoritative; prevent an alias crossing station IDs.
        if obj.song.status != "verified":
            raise ValueError("Only verified catalogue songs can receive aliases.")
        obj.station = obj.song.station
        obj.source = "reviewer"
        super().save_model(request, obj, form, change)
