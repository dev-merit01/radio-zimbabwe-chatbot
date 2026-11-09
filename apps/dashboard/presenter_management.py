"""Central administrator provisions presenters assigned to exactly one show."""
import json

from django.contrib.auth import get_user_model
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from django.db import transaction
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from apps.accounts.context_processors import get_active_station
from apps.accounts.models import AccountProfile, PresenterAssignment
from apps.charts.routing import station_show
from apps.voting.models import ReviewAudit
from apps.voting.pipeline import lock_station

from .workspace import api_access


@require_GET
@api_access("admin")
def list_presenters(request):
    station = get_active_station(request)
    assignments = PresenterAssignment.objects.filter(
        chart_show__station=station,
    ).select_related("user", "chart_show").order_by(
        "chart_show__name", "user__username",
    )[:200]
    available = get_user_model().objects.filter(
        is_superuser=False, profile__station=station,
        presenter_assignment__isnull=True,
    ).order_by("username")[:150]
    return JsonResponse({
        "station": station,
        "presenters": [
            {"user_id": assignment.user_id,
             "username": assignment.user.username,
             "active": assignment.user.is_active,
             "chart_show_id": assignment.chart_show_id,
             "chart_show": assignment.chart_show.name}
            for assignment in assignments
        ],
        "unassigned_accounts": [
            {"user_id": user.pk, "username": user.username,
             "active": user.is_active}
            for user in available
        ],
    })


@require_POST
@api_access("admin")
def create_presenter(request):
    data = json.loads(request.body)
    username, password, show_id = (
        data.get("username"), data.get("password"), data.get("chart_show_id")
    )
    if not isinstance(username, str) or not 3 <= len(username.strip()) <= 150:
        raise ValueError("Presenter username must contain 3–150 characters.")
    if not isinstance(password, str) or not 12 <= len(password) <= 128:
        raise ValueError("Choose a strong password of at least 12 characters.")
    if type(show_id) is not int:
        raise ValueError("Select a chart show.")
    try:
        validate_password(password)
    except ValidationError as error:
        raise ValueError(" ".join(error.messages)) from error
    station = get_active_station(request)
    with transaction.atomic():
        lock_station(station)
        show = station_show(station, show_id, user=request.user)
        if show.kind == "legacy" or show.is_archived:
            raise ValueError("Choose an active regular or year-end programme.")
        User = get_user_model()
        username = username.strip()
        if User.objects.filter(username__iexact=username).exists():
            raise ValueError("That presenter username already exists.")
        user = User(username=username, is_active=True, is_staff=False,
                    is_superuser=False)
        user.set_password(password)
        try:
            user.full_clean()
        except ValidationError as error:
            raise ValueError("Invalid presenter username or password.") from error
        user.save()
        AccountProfile.objects.update_or_create(
            user=user, defaults={"station": station},
        )
        PresenterAssignment.objects.create(user=user, chart_show=show)
        ReviewAudit.objects.create(
            station=station, actor=request.user, action="presenter_created",
            details={"user_id": user.pk, "chart_show_id": show.pk},
        )
    return JsonResponse({"ok": True, "username": user.username,
                         "chart_show_id": show.pk, "station": station})


@require_POST
@api_access("admin")
def assign_presenter(request):
    data = json.loads(request.body)
    user_id, show_id = data.get("user_id"), data.get("chart_show_id")
    if type(user_id) is not int or type(show_id) is not int:
        raise ValueError("Choose an account and a chart show.")
    station = get_active_station(request)
    with transaction.atomic():
        lock_station(station)
        show = station_show(station, show_id, user=request.user)
        if show.kind == "legacy" or show.is_archived:
            raise ValueError("Cannot assign presenters to historical or archived shows.")
        User = get_user_model()
        user = User.objects.select_for_update().filter(
            pk=user_id, is_superuser=False, is_staff=False,
            profile__station=station,
        ).first()
        if not user:
            return JsonResponse({"error": "Station account not found."}, status=404)
        existing = PresenterAssignment.objects.filter(user=user).first()
        previous = existing.chart_show_id if existing else None
        PresenterAssignment.objects.update_or_create(
            user=user, defaults={"chart_show": show},
        )
        ReviewAudit.objects.create(
            station=station, actor=request.user, action="presenter_assigned",
            details={"user_id": user.pk, "from": previous, "to": show.pk},
        )
    return JsonResponse({"ok": True, "chart_show_id": show.pk, "station": station})
