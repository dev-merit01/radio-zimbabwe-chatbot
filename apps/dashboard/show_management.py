"""Super-admin controls for the selected station's chart shows.

All writes operate in the session's active station, never a client-supplied station.
"""
import json
import re
from datetime import datetime, time
from zoneinfo import ZoneInfo

from django.conf import settings
from django.db import IntegrityError, transaction
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from apps.accounts.context_processors import get_active_station
from apps.voting.models import (
    ChartShow, InboundEvent, ReviewAudit, StationVotingConfig, UnassignedVote,
    SuspectedSpamVote, WeeklyChart,
)
from apps.voting.pipeline import lock_station
from apps.voting.services import validate_submission

from .workspace import api_access


def serialize(show):
    return {
        "id": show.pk, "station": show.station, "name": show.name,
        "code": show.code, "kind": show.kind,
        "schedule_mode": show.schedule_mode,
        "is_enabled": show.is_enabled, "manual_open": show.manual_open,
        "is_archived": show.is_archived,
        "chart_size": show.chart_size, "daily_limit": show.daily_limit,
        "broadcast_weekday": show.broadcast_weekday,
        "opens_weekday": show.opens_weekday,
        "closes_weekday": show.closes_weekday,
        "opens_time": show.opens_time.strftime("%H:%M"),
        "closes_time": show.closes_time.strftime("%H:%M"),
        "campaign_start": timezone.localtime(show.campaign_start).isoformat()
            if show.campaign_start else None,
        "campaign_end": timezone.localtime(show.campaign_end).isoformat()
            if show.campaign_end else None,
        "accepting": show.accepts_at(timezone.now()),
    }


@require_GET
@api_access()
def list_shows(request):
    station = get_active_station(request)
    config, _ = StationVotingConfig.objects.get_or_create(station=station)
    return JsonResponse({
        "station": station, "is_admin": request.user.is_superuser,
        "require_codes": config.require_codes,
        "vote_multiplier": config.vote_multiplier,
        "shows": [serialize(s) for s in
                  ChartShow.objects.filter(station=station).order_by("kind", "name", "id")],
        "unassigned_pending": UnassignedVote.objects.filter(
            station=station, state="pending"
        ).count() if request.user.is_superuser else 0,
    })


def parsed_bool(data, key, default=False):
    value = data.get(key, default)
    if type(value) is not bool:
        raise ValueError(f"{key} must be true or false.")
    return value


def parsed_int(data, key, default, lower, upper):
    value = data.get(key, default)
    if type(value) is not int or not lower <= value <= upper:
        raise ValueError(f"Invalid {key}.")
    return value


def parsed_time(data, key, default):
    value = data.get(key, default)
    if not isinstance(value, str):
        raise ValueError(f"Invalid {key}.")
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid {key}.") from exc


def parsed_datetime(data, key):
    value = data.get(key)
    if not value:
        return None
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError(f"Invalid {key}.")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid {key}.") from exc
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, ZoneInfo(settings.TIME_ZONE))
    return parsed


@require_POST
@api_access("admin")
def save_show(request):
    data = json.loads(request.body)
    if not isinstance(data, dict):
        raise ValueError("Expected show settings.")
    station = get_active_station(request)
    show_id = data.get("id")
    if show_id is not None and (type(show_id) is not int or show_id <= 0):
        raise ValueError("Invalid chart show ID.")
    name = data.get("name")
    code = data.get("code")
    if not isinstance(name, str) or not 2 <= len(name.strip()) <= 120:
        raise ValueError("Chart show name must contain 2–120 characters.")
    if not isinstance(code, str) or not re.fullmatch(
        r"[A-Z0-9]{2,16}", code.strip().upper()
    ):
        raise ValueError("Voting code must contain 2–16 letters or numbers.")
    name = name.strip()
    code = code.strip().upper()
    kind = data.get("kind", "weekly")
    mode = data.get("schedule_mode", "weekly")
    if kind not in {"weekly", "year_end"}:
        raise ValueError("Choose a regular or year-end chart.")
    if mode not in {"weekly", "manual", "campaign"}:
        raise ValueError("Invalid voting schedule mode.")
    if kind == "year_end" and mode != "campaign":
        raise ValueError("End-of-year Top 50 must use an exact-date voting campaign.")
    size = parsed_int(data, "chart_size", 50 if kind == "year_end" else 20, 20, 50)
    if size not in {20, 50} or (kind == "year_end" and size != 50):
        raise ValueError("Select Top 20 or Top 50; annual charts must be Top 50.")
    limit = parsed_int(data, "daily_limit", 5, 1, 100)
    broadcast = parsed_int(data, "broadcast_weekday", 5, 0, 6)
    start_day = parsed_int(data, "opens_weekday", 0, 0, 6)
    end_day = parsed_int(data, "closes_weekday", 5, 0, 6)
    start_time = parsed_time(data, "opens_time", "08:00")
    end_time = parsed_time(data, "closes_time", "18:00")
    enabled = parsed_bool(data, "is_enabled", False)
    manual_open = parsed_bool(data, "manual_open", False)
    start = parsed_datetime(data, "campaign_start")
    end = parsed_datetime(data, "campaign_end")
    if mode == "campaign" and enabled and (not start or not end or start >= end):
        raise ValueError("An enabled campaign requires a valid opening and closing date.")
    if mode == "campaign" and start and end and start >= end:
        raise ValueError("Campaign closing time must be after opening.")
    if (mode == "weekly" and start_day == end_day and
            end_time <= start_time):
        raise ValueError("A weekly voting window must have a non-zero duration.")

    with transaction.atomic():
        lock_station(station)
        show = (ChartShow.objects.select_for_update().filter(
            id=show_id, station=station
        ).first() if show_id is not None else None)
        if show_id is not None and show is None:
            return JsonResponse({"error": "Chart show not found."}, status=404)
        if show and (show.kind == "legacy" or show.is_archived):
            raise ValueError("Historical or archived shows cannot be edited.")
        if ChartShow.objects.filter(
            station=station, code=code
        ).exclude(pk=show.pk if show else None).exists():
            raise ValueError("This voting code is already used by another chart show.")
        if ChartShow.objects.filter(
            station=station, name__iexact=name
        ).exclude(pk=show.pk if show else None).exists():
            raise ValueError("A chart show with this name already exists.")
        before = serialize(show) if show else None
        if show and show.kind != kind and (
            show.raw_votes.exists() or show.published_charts.exists()
        ):
            raise ValueError("Cannot change the type of a show containing votes or archives.")
        show = show or ChartShow(station=station)
        show.name = name
        show.code = code
        show.kind = kind
        show.schedule_mode = mode
        show.is_enabled = enabled
        show.manual_open = manual_open
        show.broadcast_weekday = broadcast
        show.opens_weekday = start_day
        show.closes_weekday = end_day
        show.opens_time = start_time
        show.closes_time = end_time
        show.campaign_start = start
        show.campaign_end = end
        show.chart_size = size
        show.daily_limit = limit
        try:
            show.save()
        except IntegrityError as exc:
            raise ValueError("A chart show already uses this code.") from exc
        ReviewAudit.objects.create(
            station=station, actor=request.user,
            action="chart_show_saved",
            details={"show_id": show.pk, "before": before, "after": serialize(show)},
        )
    return JsonResponse({"ok": True, "show": serialize(show)})


@require_POST
@api_access("admin")
def archive_show(request):
    data = json.loads(request.body)
    show_id = data.get("id")
    if type(show_id) is not int or show_id <= 0:
        raise ValueError("Invalid chart show.")
    with transaction.atomic():
        station = get_active_station(request)
        lock_station(station)
        show = ChartShow.objects.select_for_update().filter(
            station=station, id=show_id, kind__in=("weekly", "year_end")
        ).first()
        if not show:
            return JsonResponse({"error": "Chart show not found."}, status=404)
        show.is_enabled = False
        show.is_archived = True
        show.save(update_fields=["is_enabled", "is_archived", "updated_at"])
        ReviewAudit.objects.create(
            station=station, actor=request.user, action="chart_show_archived",
            details={"show_id": show.pk, "name": show.name, "code": show.code},
        )
    return JsonResponse({"ok": True})


@require_POST
@api_access("admin")
def set_routing_policy(request):
    data = json.loads(request.body)
    enable = parsed_bool(data, "require_codes")
    station = get_active_station(request)
    with transaction.atomic():
        lock_station(station)
        if enable and not ChartShow.objects.filter(
            station=station, kind__in=("weekly", "year_end"),
            is_enabled=True, is_archived=False,
        ).exists():
            raise ValueError("Enable at least one chart show before requiring voting codes.")
        config, _ = StationVotingConfig.objects.get_or_create(station=station)
        config.require_codes = enable
        config.save(update_fields=["require_codes", "updated_at"])
        ReviewAudit.objects.create(
            station=station, actor=request.user, action="voting_code_policy",
            details={"require_codes": enable},
        )
    return JsonResponse({"ok": True, "require_codes": enable})


@require_GET
@api_access("admin")
def unassigned_list(request):
    station = get_active_station(request)
    rows = UnassignedVote.objects.filter(
        station=station, state="pending",
    ).order_by("-received_at", "-id")[:100]
    return JsonResponse({"items": [
        {"id": x.pk, "text": x.text, "reason": x.reason,
         "listener": "••••" + x.sender[-4:],
         "received_at": x.received_at.isoformat()} for x in rows
    ]})


@require_POST
@api_access("admin")
def assign_unassigned(request):
    data = json.loads(request.body)
    record_id = data.get("id")
    show_id = data.get("show_id")
    body = data.get("vote_text")
    if type(record_id) is not int or type(show_id) is not int:
        raise ValueError("Invalid unassigned vote or show.")
    if not isinstance(body, str) or not 3 <= len(body.strip()) <= 100:
        raise ValueError("Please enter a valid Artist - Song.")
    valid, error = validate_submission(body.strip())
    if not valid:
        raise ValueError(error)
    station = get_active_station(request)
    with transaction.atomic():
        lock_station(station)
        row = UnassignedVote.objects.select_for_update().filter(
            station=station, pk=record_id, state="pending",
        ).first()
        show = ChartShow.objects.filter(station=station, pk=show_id).first()
        if not row or not show:
            return JsonResponse({"error": "Vote or chart show not found."}, status=404)
        if not show.accepts_at(row.received_at):
            raise ValueError("This show was not open when the vote was received.")
        event, created = InboundEvent.objects.get_or_create(
            provider=row.provider, station=station, message_id=row.message_id,
            defaults={
                "sender": row.sender, "text": row.text, "vote_text": body.strip(),
                "chart_show": show, "submitted_code": show.code,
                "received_at": row.received_at,
            },
        )
        if not created:
            raise ValueError("This message has already been processed.")
        row.state = "assigned"
        row.resolved_show = show
        row.save(update_fields=["state", "resolved_show"])
        ReviewAudit.objects.create(
            station=station, actor=request.user, action="unassigned_resolved",
            details={"unassigned_id": row.pk, "chart_show_id": show.pk},
        )
    return JsonResponse({"ok": True, "event_id": event.pk})


@require_POST
@api_access("admin")
def dismiss_unassigned(request):
    data = json.loads(request.body)
    record_id = data.get("id")
    if type(record_id) is not int or record_id <= 0:
        raise ValueError("Invalid unassigned vote.")
    station = get_active_station(request)
    with transaction.atomic():
        lock_station(station)
        record = UnassignedVote.objects.select_for_update().filter(
            station=station, id=record_id, state="pending",
        ).first()
        if record is None:
            return JsonResponse({"error": "Unassigned vote not found."}, status=404)
        record.state = "dismissed"
        record.save(update_fields=["state"])
        ReviewAudit.objects.create(
            station=station, actor=request.user, action="unassigned_dismissed",
            details={"unassigned_id": record.pk, "reason": record.reason},
        )
    return JsonResponse({"ok": True})


@require_POST
@api_access("admin")
def set_station_multiplier(request):
    """Update only the super-admin's current station, with an audit record.

    This changes live chart *points*, not authentic vote counts. Previously
    published chart editions retain the multiplier captured at publication.
    """
    data = json.loads(request.body)
    if not isinstance(data, dict) or set(data) != {"multiplier"}:
        raise ValueError("Provide only the vote multiplier.")
    multiplier = data["multiplier"]
    if type(multiplier) is not int or not 1 <= multiplier <= 10:
        raise ValueError("Choose a whole-number multiplier from 1 to 10.")
    station = get_active_station(request)
    with transaction.atomic():
        lock_station(station)
        config, _ = StationVotingConfig.objects.select_for_update().get_or_create(
            station=station
        )
        before = config.vote_multiplier
        if before != multiplier:
            config.vote_multiplier = multiplier
            config.save(update_fields=["vote_multiplier", "updated_at"])
            ReviewAudit.objects.create(
                station=station, actor=request.user,
                action="vote_multiplier",
                details={
                    "old_multiplier": before,
                    "new_multiplier": multiplier,
                    "scope": "live_chart_points_only",
                    "published_charts_unchanged": True,
                },
            )
    return JsonResponse({"ok": True, "station": station, "vote_multiplier": multiplier})


@require_GET
@api_access("admin")
def suspicious_votes(request):
    """Central administrator sees only the selected station's held messages."""
    station = get_active_station(request)
    rows = SuspectedSpamVote.objects.filter(
        station=station, state="pending",
    ).select_related("chart_show").order_by("-received_at", "-pk")[:100]
    return JsonResponse({
        "station": station,
        "pending": SuspectedSpamVote.objects.filter(
            station=station, state="pending"
        ).count(),
        "items": [{
            "id": row.pk,
            "listener": "••••" + row.sender[-4:],
            "text": row.vote_text,
            "show": row.chart_show.name if row.chart_show else "Historical chart",
            "reason": row.reason,
            "received_at": row.received_at.isoformat(),
        } for row in rows],
    })


@require_POST
@api_access("admin")
def review_suspicious_vote(request):
    """Approve only a previously held authentic message, not an arbitrary vote.

    Releasing still runs normal daily limits, duplication and AI verification.
    The original station, chart show and receipt timestamp cannot be changed.
    A finalized chart period cannot be modified retroactively.
    """
    data = json.loads(request.body)
    if not isinstance(data, dict) or set(data) != {"id", "action"}:
        raise ValueError("Expected flagged vote ID and review action.")
    record_id, action = data["id"], data["action"]
    if type(record_id) is not int or record_id <= 0:
        raise ValueError("Invalid suspicious vote ID.")
    if action not in ("release", "dismiss"):
        raise ValueError("Choose release or dismiss.")

    station = get_active_station(request)
    with transaction.atomic():
        lock_station(station)
        record = SuspectedSpamVote.objects.select_for_update().filter(
            station=station, pk=record_id, state="pending",
        ).first()
        if record is None:
            return JsonResponse({"error": "Flagged vote not found."}, status=404)
        event_id = None
        if action == "release":
            if record.chart_show and record.chart_show.station != station:
                raise ValueError("Flagged vote belongs to another station.")
            # A flagged vote cannot be inserted after an official snapshot was
            # locked, preventing a mismatch between live and archived totals.
            date_received = timezone.localtime(record.received_at).date()
            if WeeklyChart.objects.filter(
                station=station, chart_show=record.chart_show,
                is_finalized=True,
                week_start__lte=date_received, week_end__gte=date_received,
            ).exists():
                raise ValueError(
                    "This voting period has a published chart. "
                    "Keep this flagged message for audit instead."
                )
            event, created = InboundEvent.objects.get_or_create(
                provider=record.provider, station=station,
                message_id=record.message_id,
                defaults={
                    "sender": record.sender, "text": record.text,
                    "vote_text": record.vote_text,
                    "submitted_code": record.submitted_code,
                    "chart_show": record.chart_show,
                    "received_at": record.received_at, "media_type": "",
                },
            )
            if not created:
                raise ValueError("This message has already entered the voting pipeline.")
            event_id = event.pk
        record.state = "released" if action == "release" else "dismissed"
        record.resolved_by = request.user
        record.resolved_at = timezone.now()
        record.save(update_fields=["state", "resolved_by", "resolved_at"])
        ReviewAudit.objects.create(
            station=station, actor=request.user,
            action="spam_released" if action == "release" else "spam_dismissed",
            details={
                "flag_id": record.pk, "reason": record.reason,
                "chart_show_id": record.chart_show_id,
                "event_id": event_id,
            },
        )
    return JsonResponse({"ok": True, "state": record.state, "event_id": event_id})
