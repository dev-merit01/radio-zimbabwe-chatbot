"""Private, authenticated, READ-ONLY configuration source for public voting.

No browser can write to the AirVote database. Chart programmes are never
listed publicly: each station's existing programme code or a presenter-issued
signed link selects exactly one show.
"""
from django.conf import settings
from django.core import signing
from django.http import JsonResponse
from django.templatetags.static import static
from django.utils import timezone
from django.views.decorators.http import require_GET

from apps.accounts.context_processors import _STATION_LOGOS
from apps.accounts.models import BroadcastStation, station_exists
from apps.voting.models import ChartShow, EditionCandidate, EligibleEdition
from apps.licensing.company_verification import company_status_for_station
from .bridge import bridge_only

SALT = "airvote-public-vote-link-v1"


def voting_link(show):
    """Presenter share link. Disabling or archiving the show revokes its use."""
    base = getattr(settings, "AIRVOTE_PUBLIC_VOTING_URL", "").rstrip("/")
    if not base or show.kind == "legacy":
        return ""
    token = signing.dumps({"station": show.station, "show": show.pk}, salt=SALT)
    return base + "/vote/" + token


def permitted_station(station):
    if not station_exists(station, active_only=True):
        return False
    if getattr(settings, "AIRVOTE_COMPANY_LICENSING_ENFORCED", False):
        return bool(company_status_for_station(station)["permitted"])
    return True


def station_info(request, row):
    path = _STATION_LOGOS.get(row.slug, "images/airvote/app-icon.png")
    logo = row.logo_url or request.build_absolute_uri(static(path))
    return {"id": row.slug, "name": row.name, "logo": logo}


def decode_campaign(token):
    if not isinstance(token, str) or len(token) > 800:
        return None
    try:
        claims = signing.loads(token, salt=SALT)
    except signing.BadSignature:
        return None
    if (not isinstance(claims, dict)
            or type(claims.get("station")) is not str
            or type(claims.get("show")) is not int):
        return None
    return ChartShow.objects.filter(
        pk=claims["show"], station=claims["station"],
        is_enabled=True, is_archived=False,
    ).exclude(kind="legacy").first()


@require_GET
@bridge_only
def stations(request):
    rows = BroadcastStation.objects.filter(is_active=True).order_by("name")
    return JsonResponse({"stations": [
        station_info(request, row) for row in rows if permitted_station(row.slug)
    ]})


@require_GET
@bridge_only
def resolve(request, station, code):
    if not permitted_station(station):
        return JsonResponse({"error": "Voting unavailable."}, status=404)
    if not code or len(code) > 16:
        return JsonResponse({"error": "Voting unavailable."}, status=404)
    show = ChartShow.objects.filter(
        station=station, code__iexact=code, is_enabled=True,
        is_archived=False,
    ).exclude(kind="legacy").first()
    if not show:
        return JsonResponse({"error": "Voting unavailable."}, status=404)
    token = signing.dumps({"station": station, "show": show.pk}, salt=SALT)
    return JsonResponse({"campaign": token})


def candidate_edition(show, day):
    return EligibleEdition.objects.filter(
        station=show.station, chart_show=show, status="locked",
        voting_start__lte=day, voting_end__gte=day,
    ).order_by("-chart_date", "-id").first()


@require_GET
@bridge_only
def campaign(request, token):
    show = decode_campaign(token)
    if not show or not permitted_station(show.station):
        return JsonResponse({"error": "Campaign not found."}, status=404)
    now = timezone.now()
    day = timezone.localdate(now)
    edition = candidate_edition(show, day)
    open_now = bool(show.accepts_at(now) and edition)
    station = BroadcastStation.objects.get(slug=show.station)
    from apps.voting.station_vote_policy import voting_rules_for_station
    policy = voting_rules_for_station(show.station)
    songs = []
    if edition:
        candidates = EditionCandidate.objects.filter(
            edition=edition, song__status="verified",
            song__station=show.station,
        ).select_related("song").order_by("song__artist", "song__title", "song_id")
        songs = [{
            "id": entry.song_id,
            "title": entry.song.title,
            "artist": entry.song.artist,
            "artwork": entry.song.image_url
            if entry.song.image_url.startswith("https://") else "",
        } for entry in candidates]
    return JsonResponse({
        "station": station_info(request, station),
        "campaign": token,
        "open": open_now,
        "show_id": show.pk,  # Internal to gateway; never accept this from a client.
        "eligible_edition": edition.pk if edition else None,
        "voting_day": str(day),
        "daily_limit": show.daily_limit,
        "station_daily_limit": policy["station_daily_limit"],
        "allow_repeat_song": policy["allow_repeat_song"],
        "repeat_scope": policy["repeat_scope"],
        "timezone": settings.TIME_ZONE,
        "songs": songs if open_now else [],
    })
