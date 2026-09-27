from django.contrib import admin
from django.contrib.auth import get_user_model
from django.contrib.auth.admin import UserAdmin
from .models import AccountProfile


class ProfileInline(admin.StackedInline):
    model = AccountProfile
    extra = 1
    max_num = 1
    can_delete = False


class AirVoteUserAdmin(UserAdmin):
    inlines = [ProfileInline]

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


admin.site.unregister(get_user_model())
admin.site.register(get_user_model(), AirVoteUserAdmin)
