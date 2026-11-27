from django.conf import settings
from django.core.exceptions import PermissionDenied

from .models import BroadcastStation, StaffAccess

Role = StaffAccess.Role
PRIVILEGES = {
    "export": "Export chart reports",
    "review": "Review flagged votes",
    "publish": "Publish final charts",
    "shows": "Edit programme details and daily limits",
    "presenters": "Manage presenter accounts",
    "staff": "Manage station manager accounts",
}
DEFAULTS = {
    Role.PRESENTER: (),
    Role.MANAGER: ("export", "review"),
    Role.DIRECTOR: ("export",),
    Role.ADMIN: tuple(PRIVILEGES),
}


def access_for(user):
    return getattr(user, "staff_access", None)


def permitted(user, privilege):
    grant = access_for(user)
    return user.is_superuser or bool(
        grant and grant.role != Role.PRESENTER and privilege in grant.privileges
    )


def stations_for(user):
    stations = BroadcastStation.objects.filter(is_active=True)
    grant = access_for(user)
    if not user.is_superuser and not (
        grant
        and (
            grant.role == Role.DIRECTOR
            or (grant.role == Role.ADMIN and grant.network_access)
        )
    ):
        stations = stations.filter(staffaccess=grant) if grant else stations.none()
    if (
        settings.AIRVOTE_COMPANY_LICENSING_ENFORCED
        or settings.AIRVOTE_LICENSING_ENFORCED
    ):
        from apps.licensing.verification import licence_status

        stations = stations.filter(
            pk__in=[
                slug
                for slug in stations.values_list("pk", flat=True)
                if licence_status(slug)["permitted"]
            ]
        )
    return stations


def grantable_roles(user):
    if user.is_superuser:
        return list(Role.values)
    roles = []
    if permitted(user, "presenters"):
        roles.append(Role.PRESENTER)
    if permitted(user, "staff") and access_for(user).role == Role.ADMIN:
        roles.append(Role.MANAGER)
    return roles


def require_privilege(user, name):
    if not permitted(user, name):
        raise PermissionDenied
