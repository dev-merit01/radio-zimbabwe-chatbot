"""Database-driven station provisioning; no code or Railway edit per station.

Only the central administrator may create or configure a station.
Immutable station slugs preserve existing chart and vote ownership. Existing
Bird env routes are honoured and cannot be hijacked by a new station.
"""
import json
import re
from urllib.parse import urlsplit

from django.conf import settings
from django.db import IntegrityError, transaction
from django.db.models import Count
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from apps.accounts.models import BroadcastStation
from apps.charts.routing import legacy_show
from apps.voting.models import ChartShow, ReviewAudit, StationVotingConfig
from .workspace import api_access


_SLUG = re.compile(r"[a-z][a-z0-9_]{2,31}\Z")
_PHONE = re.compile(r"[1-9][0-9]{8,14}\Z")


def _validated_name(raw):
    if not isinstance(raw, str):
        raise ValueError("Enter a station name.")
    name = " ".join(raw.split())
    if not 3 <= len(name) <= 100:
        raise ValueError("Station name must be 3–100 characters.")
    return name


def _validated_number(raw, station_slug):
    if raw in (None, ""):
        return None
    if not isinstance(raw, str):
        raise ValueError("Enter a valid international WhatsApp number.")
    value = raw.strip().removeprefix("+")
    if not _PHONE.fullmatch(value):
        raise ValueError(
            "Enter 9–15 international digits, e.g. 263790573340, "
            "without spaces or a leading zero."
        )
    env_routes = getattr(settings, "BIRD_NUMBER_STATION_MAP", {})
    if not isinstance(env_routes, dict):
        raise ValueError("Bird receiving-number settings are invalid.")
    existing_station = env_routes.get(value) or env_routes.get("+" + value)
    if existing_station and existing_station != station_slug:
        raise ValueError(
            "This number is already mapped to another station in the "
            "existing Bird configuration."
        )
    if BroadcastStation.objects.filter(bird_number=value).exclude(
        slug=station_slug
    ).exists():
        raise ValueError("This WhatsApp number is already assigned to a station.")
    return value


def _validated_logo(raw):
    if raw in (None, ""):
        return ""
    if not isinstance(raw, str) or len(raw) > 500:
        raise ValueError("Provide a valid HTTPS logo URL, up to 500 characters.")
    raw = raw.strip()
    parsed = urlsplit(raw)
    if (parsed.scheme != "https" or not parsed.hostname
            or parsed.username or parsed.password or
            parsed.hostname in {"localhost", "127.0.0.1"}):
        raise ValueError("Station logo must use a public HTTPS image URL.")
    return raw


def _legacy_number(station):
    routes = getattr(settings, "BIRD_NUMBER_STATION_MAP", {})
    if not isinstance(routes, dict):
        return ""
    for number, slug in routes.items():
        if slug == station:
            return number.removeprefix("+")
    return ""


def _station_json(row, counts=None):
    counts = counts or {}
    legacy = _legacy_number(row.slug)
    effective = row.bird_number or legacy
    return {
        "slug": row.slug, "name": row.name,
        "is_active": row.is_active,
        "bird_number": row.bird_number or "",
        "effective_bird_number": effective,
        "number_source": (
            "database" if row.bird_number
            else "environment" if legacy else "not_configured"
        ),
        "logo_url": row.logo_url,
        "chart_shows": counts.get(row.slug, 0),
        "created_at": row.created_at.isoformat(),
    }


@require_GET
@api_access("admin")
def list_stations(request):
    counts = dict(
        ChartShow.objects.exclude(kind="legacy").filter(
            is_archived=False
        ).values("station").annotate(n=Count("id")).values_list("station", "n")
    )
    rows = BroadcastStation.objects.order_by("name", "slug")
    return JsonResponse({
        "stations": [_station_json(row, counts) for row in rows],
        "selected_station": request.session.get("switched_station")
        or getattr(getattr(request.user, "profile", None), "station", "radio_zimbabwe"),
    })


@require_POST
@api_access("admin")
def create_station(request):
    payload = json.loads(request.body)
    name = _validated_name(payload.get("name"))
    slug = payload.get("slug")
    if slug in ("", None):
        base = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")[:32]
        if not base or not base[0].isalpha():
            base = ("station_" + base)[:32]
        if len(base) < 3:
            base = (base + "_fm")[:32]
        slug = base
        index = 2
        while BroadcastStation.objects.filter(slug=slug).exists():
            suffix = f"_{index}"
            slug = base[:32 - len(suffix)] + suffix
            index += 1
    if not isinstance(slug, str) or not _SLUG.fullmatch(slug):
        raise ValueError(
            "Use a station ID of 3–32 lowercase letters, numbers and "
            "underscores, starting with a letter."
        )
    if type(payload.get("is_active", False)) is not bool:
        raise ValueError("Invalid station activation status.")
    if payload.get("is_active", False):
        raise ValueError(
            "New stations start disabled. Set up its chart shows and "
            "receiving number before activating it."
        )
    logo = _validated_logo(payload.get("logo_url"))
    with transaction.atomic():
        if BroadcastStation.objects.filter(name__iexact=name).exists():
            raise ValueError("A station already uses this name.")
        number = _validated_number(payload.get("bird_number"), slug)
        if BroadcastStation.objects.filter(slug=slug).exists():
            raise ValueError("That permanent station ID already exists.")
        try:
            row = BroadcastStation.objects.create(
                slug=slug, name=name, bird_number=number, logo_url=logo,
                is_active=False,
            )
        except IntegrityError as exc:
            raise ValueError("A station with this ID or number already exists.") from exc
        StationVotingConfig.objects.get_or_create(station=slug)
        legacy_show(slug)
        ReviewAudit.objects.create(
            station=slug, actor=request.user, action="station_created",
            details={"station": slug, "name": name,
                     "bird_number_configured": bool(number)},
        )
    return JsonResponse({"ok": True, "station": _station_json(row)}, status=201)


@require_POST
@api_access("admin")
def update_station(request):
    payload = json.loads(request.body)
    slug = payload.get("slug")
    if not isinstance(slug, str):
        raise ValueError("Choose a station.")
    name = _validated_name(payload.get("name"))
    enabled = payload.get("is_active")
    if type(enabled) is not bool:
        raise ValueError("Invalid station activation status.")
    logo = _validated_logo(payload.get("logo_url"))
    with transaction.atomic():
        row = BroadcastStation.objects.select_for_update().filter(slug=slug).first()
        if row is None:
            return JsonResponse({"error": "Station not found."}, status=404)
        if BroadcastStation.objects.filter(name__iexact=name).exclude(slug=slug).exists():
            raise ValueError("Another station already uses this name.")
        number = _validated_number(payload.get("bird_number"), slug)
        if enabled and not number and not _legacy_number(slug):
            raise ValueError(
                "Configure a Bird receiving number before activating "
                "a new voting station."
            )
        if row.is_active and not enabled:
            if ChartShow.objects.filter(
                station=slug, kind__in=("weekly", "year_end"),
                is_enabled=True, is_archived=False,
            ).exists():
                raise ValueError(
                    "Disable all active chart shows before disabling "
                    "this station. Its votes and archives will be preserved."
                )
        changed = {
            "name": [row.name, name] if row.name != name else None,
            "active": [row.is_active, enabled] if row.is_active != enabled else None,
            "bird_number": row.bird_number != number,
            "logo": row.logo_url != logo,
        }
        row.name = name
        row.is_active = enabled
        row.bird_number = number
        row.logo_url = logo
        try:
            row.save(update_fields=[
                "name", "is_active", "bird_number", "logo_url", "updated_at",
            ])
        except IntegrityError as exc:
            raise ValueError("Station name or number conflicts with another station.") from exc
        ReviewAudit.objects.create(
            station=slug, actor=request.user, action="station_updated",
            details={k: v for k, v in changed.items() if v is not None},
        )
    return JsonResponse({"ok": True, "station": _station_json(row)})
