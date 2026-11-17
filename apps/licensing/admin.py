from django.contrib import admin
from .models import StationLicence


@admin.register(StationLicence)
class StationLicenceAdmin(admin.ModelAdmin):
    list_display = ("station", "installed_at", "updated_at", "revoked_at")
    readonly_fields = ("station", "installed_at", "updated_at", "revoked_at",
                       "revocation_reason", "verified_status")
    exclude = ("signed_token",)
    search_fields = ("station",)

    def verified_status(self, obj):
        from .verification import licence_status
        return licence_status(obj.station)["state"]

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
