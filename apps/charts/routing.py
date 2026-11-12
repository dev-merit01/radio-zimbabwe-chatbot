"""Strict station-plus-code vote routing. No unverified input can select a station."""
import re

from django.utils import timezone

from apps.voting.models import ChartShow, StationVotingConfig
from apps.accounts.context_processors import get_active_station
from apps.accounts.permissions import presenter_show, require_presenter_show


CODE_VOTE = re.compile(r"^([A-Za-z0-9]{2,16})(?:\s*[:|]\s*|\s+)(.+)$", re.S)
# The same programme code may appear after the song, separated by whitespace,
# a colon, or a pipe. Never treat an arbitrary final song word as a code:
# both patterns must be validated against this receiving station's catalogue.
CODE_AT_END = re.compile(r"^(.+?)(?:\s*[:|]\s*|\s+)([A-Za-z0-9]{2,16})$", re.S)


def legacy_show(station):
    show, _ = ChartShow.objects.get_or_create(
        station=station, kind="legacy",
        defaults={"name": "Historical station chart", "code": "", "is_enabled": False},
    )
    return show


def get_selected_show(request):
    """Only station-scoped shows may be read; never accept arbitrary station IDs."""
    station = get_active_station(request)
    raw = request.GET.get("show_id") or request.POST.get("show_id")
    assignment = presenter_show(request.user)
    if not request.user.is_superuser and assignment is None:
        raise ValueError("No chart show assigned to this account.")
    if raw is None or raw == "":
        return assignment if assignment else legacy_show(station)
    if not str(raw).isdecimal():
        raise ValueError("Invalid chart show.")
    show = ChartShow.objects.filter(pk=int(raw), station=station).first()
    if not show:
        raise ValueError("Chart show not found for this station.")
    return require_presenter_show(request, show)


def station_show(station, raw, user=None):
    """Validate a programme ID against the authenticated user's station."""
    assigned = presenter_show(user) if user else None
    if user is not None and not user.is_superuser and assigned is None:
        raise ValueError("No chart show assigned to this account.")
    if raw is None or raw == "":
        return assigned if assigned and assigned.station == station else legacy_show(station)
    if type(raw) is not int or raw <= 0:
        raise ValueError("Invalid chart show.")
    show = ChartShow.objects.filter(station=station, pk=raw).first()
    if show is None:
        raise ValueError("Chart show not found for this station.")
    if assigned and show.pk != assigned.pk:
        raise ValueError("Your presenter account cannot access this chart show.")
    return show


def station_requires_codes(station):
    """Protect multi-chart stations even after an accidental policy change.

    A missing/unknown code must never silently route to a historical chart
    when the station has published programme codes. Stations with no coded
    programmes retain the old single-chart onboarding flow.
    """
    required = StationVotingConfig.objects.filter(
        station=station, require_codes=True,
    ).exists()
    return required or ChartShow.objects.filter(
        station=station, kind__in=("weekly", "year_end"),
    ).exclude(code="").exists()


def route_vote(station, text, received_at=None):
    """Return (open show, code-stripped song, code, reason), station scoped.

    Recognise a published programme code at EITHER end of a message. Reject
    two valid code tokens rather than guessing which show is intended. A
    closed show's identity and stripped song text are returned for safe
    recording in the held-vote queue; no chart tally is changed here.
    """
    received_at = received_at or timezone.now()
    raw = (text or "").strip()
    start = CODE_VOTE.match(raw)
    end = CODE_AT_END.match(raw)
    # Always resolve against the receiving station; matching a code belonging
    # only to another station must not select or disclose that other show.
    codes = {}
    for show in ChartShow.objects.filter(station=station).exclude(code=""):
        codes[show.code.upper()] = show

    prefix = codes.get(start.group(1).upper()) if start else None
    suffix = codes.get(end.group(2).upper()) if end else None
    if prefix and suffix:
        return None, "", "", "ambiguous_code"
    if prefix:
        show = prefix
        song_text = start.group(2).strip()
    elif suffix:
        show = suffix
        song_text = end.group(1).strip()
    else:
        if station_requires_codes(station):
            return None, "", "", "missing_or_unknown_code"
        return legacy_show(station), raw, "", None

    # Never accept a code without a substantive song submission.
    if not song_text:
        return None, "", "", "missing_or_unknown_code"
    if show.accepts_at(received_at):
        return show, song_text, show.code.upper(), None
    return None, song_text, show.code.upper(), "show_closed"
