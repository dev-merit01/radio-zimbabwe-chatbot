"""Kuiper licensing data is visible to AirVote administrators, never editable as grants."""
from django.contrib import admin

from .models import BroadcastCompany, CompanyLicence, CompanyStation, StationLicence


class SuperuserOnlyAdmin(admin.ModelAdmin):
    def has_module_permission(self, request):
        return bool(request.user.is_superuser)

    def has_view_permission(self, request, obj=None):
        return bool(request.user.is_superuser)

    def has_add_permission(self, request):
        return bool(request.user.is_superuser)

    def has_change_permission(self, request, obj=None):
        return bool(request.user.is_superuser)

    def has_delete_permission(self, request, obj=None):
        return bool(request.user.is_superuser)


class StationOwnershipInline(admin.TabularInline):
    model = CompanyStation
    extra = 0
    fields = ("station_slug",)
    verbose_name = "Owned station"
    verbose_name_plural = "Owned stations"


@admin.register(BroadcastCompany)
class BroadcastCompanyAdmin(SuperuserOnlyAdmin):
    list_display = ("name", "slug", "is_active", "station_total")
    list_filter = ("is_active",)
    search_fields = ("name", "slug")
    inlines = [StationOwnershipInline]

    def station_total(self, obj):
        return obj.station_ownerships.count()

    def get_readonly_fields(self, request, obj=None):
        return ("slug", "created_at", "updated_at") if obj else ("created_at", "updated_at")


@admin.register(CompanyLicence)
class CompanyLicenceAdmin(SuperuserOnlyAdmin):
    list_display = ("company", "verified_status", "installed_at", "revoked_at")
    readonly_fields = ("company", "verified_status", "installed_at", "updated_at",
                       "revoked_at", "revocation_reason")
    exclude = ("signed_token",)

    def verified_status(self, obj):
        from .company_verification import company_licence_status
        return company_licence_status(obj.company_id)["state"]

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(StationLicence)
class LegacyStationLicenceAdmin(SuperuserOnlyAdmin):
    list_display = ("station", "installed_at", "revoked_at")
    readonly_fields = ("station", "installed_at", "updated_at", "revoked_at", "revocation_reason")
    exclude = ("signed_token",)
    def has_add_permission(self, request):
        return False
    def has_change_permission(self, request, obj=None):
        return False
    def has_delete_permission(self, request, obj=None):
        return False
