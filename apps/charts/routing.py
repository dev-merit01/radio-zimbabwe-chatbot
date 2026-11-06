"""Strict station-plus-code vote routing. No unverified input can select a station."""
import re

from django.utils import timezone

from apps.voting.models import ChartShow, StationVotingConfig


CODE_VOTE = re.compile(r"^([A-Za-z0-9]{2,16})(?:\s*[:|]\s*|\s+)(.+)$", re.S)


def legacy_show(station):
    return ChartShow.objects.get(station=station, kind="legacy")


def get_selected_show(request):
    """Only station-scoped shows may be read; never accept arbitrary station IDs."""
    station = __import__(
        "apps.accounts.context_processors", fromlist=["get_active_station"]
    ).get_active_station(request)
    raw = request.GET.get("show_id") or request.POST.get("show_id")
    if raw is None or raw == "":
        return legacy_show(station)
    if not str(raw).isdecimal():
        raise ValueError("Invalid chart show.")
    show = ChartShow.objects.filter(pk=int(raw), station=station).first()
    if not show:
        raise ValueError("Chart show not found for this station.")
    return show


def route_vote(station, text, received_at=None):
    """(show, song_text, submitted_code, error_reason).

    A code is mandatory only after the administrator enables this requirement.
    Old stations otherwise keep their legacy no-code flow during rollout.
    """
    received_at = received_at or timezone.now()
    raw = (text or "").strip()
    match = CODE_VOTE.match(raw)
    if match:
        code = match.group(1).upper()
        show = ChartShow.objects.filter(station=station, code=code).first()
        if show is not None:
            if show.accepts_at(received_at):
                return show, match.group(2).strip(), code, None
            return None, "", code, "show_closed"
    config = StationVotingConfig.objects.filter(station=station).first()
    if config and config.require_codes:
        return None, "", "", "missing_or_unknown_code"
    return legacy_show(station), raw, "", None
