from django.conf import settings
from django.core.exceptions import PermissionDenied

from .models import BroadcastStation, StaffAccess

Role = StaffAccess.Role
PRIVILEGES = {
    "incoming": "View incoming votes",
    "archives": "View published chart archives",
    "eligible": "Prepare and publish eligible song lists",
    "export": "Export chart reports",
    "review": "Review flagged votes",
    "publish": "Publish final charts",
    "shows": "Edit programme details and daily limits",
    "presenters": "Manage presenter accounts",
    "staff": "Manage station manager accounts",
}
DEFAULTS = {
    Role.PRESENTER: ("eligible", "export", "archives"),
    Role.MANAGER: ("export", "review", "archives"),
    Role.DIRECTOR: ("incoming", "export", "archives"),
    Role.ADMIN: tuple(key for key in PRIVILEGES if key != "eligible"),
}


def access_for(user):
    return getattr(user, "staff_access", None)


def permitted(user, privilege):
    grant = access_for(user)
    return user.is_superuser or bool(grant and privilege in grant.privileges)


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


def shows_for(user, stations):
    from apps.voting.models import ChartShow

    shows = ChartShow.objects.filter(station__in=stations.values("slug")).exclude(
        kind="legacy"
    )
    grant = access_for(user)
    if not user.is_superuser and grant and grant.selected_shows_only:
        return shows.filter(pk__in=grant.shows.values("pk"))
    return shows


def can_delegate_scope(actor, target):
    grant = access_for(actor)
    if actor.is_superuser or not grant or not grant.selected_shows_only:
        return True
    allowed = grant.shows.values("pk")
    if target.role == Role.PRESENTER:
        assignment = getattr(target.user, "presenter_assignment", None)
        return bool(
            assignment and grant.shows.filter(pk=assignment.chart_show_id).exists()
        )
    return (
        target.selected_shows_only and not target.shows.exclude(pk__in=allowed).exists()
    )
