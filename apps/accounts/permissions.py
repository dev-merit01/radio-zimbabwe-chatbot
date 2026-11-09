"""Server-side presenter scope, independent of browser navigation."""
from apps.accounts.models import PresenterAssignment


def presenter_show(user):
    if not getattr(user, "is_authenticated", False) or user.is_superuser:
        return None
    assignment = PresenterAssignment.objects.select_related("chart_show").filter(
        user=user
    ).first()
    return assignment.chart_show if assignment else None


def require_presenter_show(request, show):
    """Refuse arbitrary query/body show IDs from assigned presenters."""
    selected = presenter_show(request.user)
    if selected and (selected.pk != show.pk or selected.station != show.station):
        raise ValueError("Your presenter account cannot access this chart show.")
    return show
