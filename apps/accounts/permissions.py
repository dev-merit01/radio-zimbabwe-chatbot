"""Server-side presenter scope, independent of browser navigation."""
from apps.accounts.models import PresenterAssignment


def presenter_show(user):
    if (not getattr(user, "is_authenticated", False)
            or getattr(user, "is_superuser", False)
            or not getattr(user, "pk", None)):
        # Static/anonymous request contexts have no saved user or assignment.
        # This also avoids unnecessary database queries for such contexts.
        return None
    assignment = PresenterAssignment.objects.select_related("chart_show").filter(
        user=user
    ).first()
    return assignment.chart_show if assignment else None


SESSION_CHART_SHOW_KEY = "airvote_chart_show_id"


def request_presenter_show(request):
    """The programme selected at login, or an explicitly assigned programme.

    Individual presenter assignments take priority. Shared station accounts
    may select an existing show at sign-in, but the resulting session is
    restricted to that station and show for its entire lifetime.
    """
    user = getattr(request, "user", None)
    assigned = presenter_show(user)
    if assigned:
        return assigned
    if (not getattr(user, "is_authenticated", False)
            or getattr(user, "is_superuser", False)):
        return None
    session = getattr(request, "session", None)
    show_id = session.get(SESSION_CHART_SHOW_KEY) if session else None
    if type(show_id) is not int or show_id <= 0:
        return None
    station = getattr(getattr(user, "profile", None), "station", None)
    if not station:
        return None
    from apps.voting.models import ChartShow
    return ChartShow.objects.filter(
        pk=show_id, station=station, kind__in=("weekly", "year_end"),
        is_archived=False,
    ).first()


def require_presenter_show(request, show):
    """Refuse show IDs outside the current authenticated login session."""
    if request.user.is_superuser:
        return show
    selected = request_presenter_show(request)
    if selected is None or selected.pk != show.pk or selected.station != show.station:
        raise ValueError("Your login cannot access this chart show.")
    return show
