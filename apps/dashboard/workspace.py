import csv
import io
import json
from datetime import date, timedelta
from functools import wraps
from django.conf import settings
from django.core.paginator import Paginator
from django.db import IntegrityError, connection
from django.db.models import Sum, Count
from django.http import JsonResponse, HttpResponse
from django.utils import timezone
from django.views.decorators.http import require_POST
from apps.accounts.context_processors import get_active_station
from apps.voting.models import (
    RawVote,
    CleanedSong,
    CleanedSongTally,
    InboundEvent,
    VoteJob,
    OutboundMessage,
    ReviewAudit,
    WorkerHeartbeat,
)
from apps.voting.pipeline import review_song


def api_access(permission=None):
    def wrap(fn):
        @wraps(fn)
        def call(request, *args, **kwargs):
            if not request.user.is_authenticated:
                return JsonResponse({"error": "Sign in to continue."}, status=401)
            if permission and not request.user.has_perm(permission):
                return JsonResponse(
                    {"error": "Your account cannot perform this action."}, status=403
                )
            try:
                if request.method == "POST":
                    data = json.loads(request.body)
                    if not isinstance(data, dict):
                        raise ValueError("Expected a JSON object.")
                return fn(request, *args, **kwargs)
            except (ValueError, TypeError, KeyError, OverflowError) as exc:
                return JsonResponse(
                    {
                        "error": str(exc)
                        if isinstance(exc, ValueError)
                        else "Invalid request."
                    },
                    status=400,
                )
            except CleanedSong.DoesNotExist:
                return JsonResponse(
                    {"error": "Song not found for your station."}, status=404
                )
            except IntegrityError:
                return JsonResponse(
                    {"error": "That song already exists. Merge it instead."}, status=409
                )

        return call

    return wrap


def page(request, qs):
    p = Paginator(qs, 30).get_page(request.GET.get("page", 1))
    return p, {
        "page": p.number,
        "pages": p.paginator.num_pages,
        "total": p.paginator.count,
    }


@api_access()
def overview(request):
    station = get_active_station(request)
    today = timezone.localdate()
    start = today - timedelta(days=today.weekday())
    votes = RawVote.objects.filter(
        station=station, vote_date__gte=start, vote_date__lte=today
    )
    accepted = CleanedSongTally.objects.filter(
        station=station,
        cleaned_song__status="verified",
        date__gte=start,
        date__lte=today,
    )
    daily = dict(
        votes.values("vote_date").annotate(n=Count("id")).values_list("vote_date", "n")
    )
    return JsonResponse(
        {
            "version": settings.APP_VERSION,
            "station": station,
            "today": str(today),
            "can_review": request.user.has_perm("voting.change_cleanedsong"),
            "can_add": request.user.has_perm("voting.add_cleanedsong"),
            "can_publish": request.user.has_perm("voting.add_weeklychart"),
            "received": votes.count(),
            "verified_votes": accepted.aggregate(n=Sum("count"))["n"] or 0,
            "listeners": votes.values("user_id").distinct().count(),
            "pending_songs": CleanedSong.objects.filter(
                station=station, status="pending"
            ).count(),
            "timeline": [
                {
                    "date": str(start + timedelta(days=i)),
                    "count": daily.get(start + timedelta(days=i), 0),
                }
                for i in range(7)
            ],
            "updated_at": timezone.now().isoformat(),
        }
    )


@api_access()
def songs(request):
    station = get_active_station(request)
    qs = CleanedSong.objects.filter(station=station)
    status = request.GET.get("status", "pending")
    if status in {"pending", "verified", "rejected"}:
        qs = qs.filter(status=status)
    q = request.GET.get("q", "").strip()[:100]
    if q:
        qs = qs.filter(canonical_name__icontains=q)
    p, meta = page(
        request,
        qs.annotate(votes=Sum("match_keys__vote_count")).order_by(
            "-votes", "canonical_name", "id"
        ),
    )
    return JsonResponse(
        {
            **meta,
            "items": [
                {
                    "id": s.id,
                    "artist": s.artist,
                    "title": s.title,
                    "status": s.status,
                    "votes": s.votes or 0,
                    "spotify": bool(s.spotify_track_id),
                    "image_url": s.image_url,
                }
                for s in p
            ],
        }
    )


@require_POST
@api_access("voting.change_cleanedsong")
def review(request, song_id):
    data = json.loads(request.body)
    review_song(
        get_active_station(request),
        request.user,
        song_id,
        data["action"],
        target_id=data.get("target_id"),
        artist=data.get("artist"),
        title=data.get("title"),
    )
    return JsonResponse({"ok": True})


@require_POST
@api_access("voting.add_cleanedsong")
def add_song(request):
    data = json.loads(request.body)
    if not isinstance(data.get("artist"), str) or not isinstance(
        data.get("title"), str
    ):
        raise ValueError("Artist and title must be text.")
    artist, title = data["artist"].strip(), data["title"].strip()
    if not artist or not title or len(artist) > 240 or len(title) > 240:
        raise ValueError("Artist and title are required (maximum 240 characters).")
    from django.db import transaction
    from apps.voting.pipeline import lock_station

    station = get_active_station(request)
    with transaction.atomic():
        lock_station(station)
        canonical = f"{artist} - {title}"
        if CleanedSong.objects.filter(
            station=station, canonical_name__iexact=canonical
        ).exists():
            return JsonResponse({"error": "This song already exists."}, status=409)
        song = CleanedSong.objects.create(
            station=station,
            artist=artist,
            title=title,
            canonical_name=canonical,
            status="pending",
        )
        ReviewAudit.objects.create(
            station=station,
            actor=request.user,
            action="add",
            details={"id": song.id, "name": canonical},
        )
    return JsonResponse({"ok": True, "id": song.id}, status=201)


@api_access()
def incoming(request):
    station = get_active_station(request)
    qs = (
        RawVote.objects.filter(station=station)
        .select_related("user")
        .order_by("-created_at", "-id")
    )
    q = request.GET.get("q", "").strip()[:100]
    if q:
        qs = qs.filter(raw_input__icontains=q)
    p, meta = page(request, qs)
    return JsonResponse(
        {
            **meta,
            "items": [
                {
                    "id": v.id,
                    "text": v.raw_input,
                    "song": v.display_name,
                    "channel": v.user.channel,
                    "listener": "••••" + v.user.user_ref[-4:],
                    "date": str(v.vote_date),
                    "received_at": v.created_at.isoformat(),
                }
                for v in p
            ],
        }
    )


@api_access()
def health(request):
    station = get_active_station(request)
    now = timezone.now()
    counts = {}
    for label, qs in [
        ("inbound", InboundEvent.objects.filter(station=station)),
        ("matching", VoteJob.objects.filter(vote__station=station)),
        ("replies", OutboundMessage.objects.filter(event__station=station)),
    ]:
        counts[label] = dict(
            qs.values("state").annotate(n=Count("id")).values_list("state", "n")
        )
    roles = set(
        WorkerHeartbeat.objects.filter(
            last_seen__gte=now - timedelta(minutes=4)
        ).values_list("queue", flat=True)
    )
    return JsonResponse(
        {
            "version": settings.APP_VERSION,
            "worker_online": "all" in roles or {"intake", "matching", "replies"}.issubset(roles),
            "worker_queues": sorted(roles),
            "queues": counts,
            "can_retry": request.user.is_superuser,
            "providers": [
                {"name": name, "configured": bool(value)}
                for name, value in [
                    (
                        "Telegram",
                        settings.TELEGRAM_BOT_TOKEN
                        and settings.TELEGRAM_WEBHOOK_SECRET,
                    ),
                    (
                        "Bird WhatsApp",
                        settings.BIRD_ACCESS_KEY
                        and settings.BIRD_WORKSPACE_ID
                        and settings.BIRD_CHANNEL_ID
                        and settings.BIRD_WEBHOOK_SECRET,
                    ),
                    (
                        "OneMsg WhatsApp",
                        settings.ONEMSG_APP_KEY
                        and settings.ONEMSG_AUTH_KEY
                        and settings.ONEMSG_WEBHOOK_SECRET,
                    ),
                    ("OpenAI", settings.OPENAI_API_KEY),
                    (
                        "Spotify",
                        settings.SPOTIFY_CLIENT_ID and settings.SPOTIFY_CLIENT_SECRET,
                    ),
                ]
            ],
            "ai_enabled": settings.AUTO_AI_MATCH,
            "daily_limit": settings.VOTING_DAILY_LIMIT,
            "repeat_songs": settings.ALLOW_REPEAT_SONG,
            "database": connection.vendor,
            "note": "Configuration status does not verify live provider access.",
        }
    )


@require_POST
@api_access()
def retry_failed(request):
    if not request.user.is_superuser:
        return JsonResponse({"error": "Administrator access required."}, status=403)
    station = get_active_station(request)
    # Unknown send outcomes are deliberately excluded to avoid duplicate messages.
    total = 0
    for qs in [
        InboundEvent.objects.filter(station=station),
        VoteJob.objects.filter(vote__station=station),
        OutboundMessage.objects.filter(event__station=station),
    ]:
        total += qs.filter(state="failed").update(
            state="queued", attempts=0, available_at=timezone.now(), error=""
        )
    ReviewAudit.objects.create(
        station=station,
        actor=request.user,
        action="retry_failed",
        details={"count": total},
    )
    return JsonResponse({"ok": True, "count": total})


@api_access()
def audit(request):
    p, meta = page(
        request,
        ReviewAudit.objects.filter(station=get_active_station(request))
        .select_related("actor")
        .order_by("-created_at", "-id"),
    )
    return JsonResponse(
        {
            **meta,
            "items": [
                {
                    "action": a.action,
                    "actor": a.actor.get_username() if a.actor else "System",
                    "details": a.details,
                    "created_at": a.created_at.isoformat(),
                }
                for a in p
            ],
        }
    )


@require_POST
@api_access("voting.add_weeklychart")
def publish(request):
    from apps.charts.publishing import publish_week

    data = json.loads(request.body)
    chart, created = publish_week(
        get_active_station(request),
        date.fromisoformat(data["week_start"]),
        request.user,
        int(data.get("size", 20)),
    )
    return JsonResponse({"ok": True, "id": chart.id, "created": created})


@api_access()
def export_chart(request):
    from .views import chart_today, chart_detail

    archive = request.GET.get("archive")
    response = chart_detail(request, int(archive)) if archive else chart_today(request)
    if response.status_code != 200:
        return response
    data = json.loads(response.content)
    rows = data.get("entries", data.get("top100", []))

    def safe(value):
        value = str(value)
        return (
            "'" + value
            if value.lstrip().startswith(("=", "+", "-", "@", "\t", "\r"))
            else value
        )

    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["Rank", "Title", "Artist", "Votes"])
    for row in rows:
        writer.writerow(
            [row["rank"], safe(row["title"]), safe(row["artists"]), row["count"]]
        )
    response = HttpResponse(
        "\ufeff" + output.getvalue(), content_type="text/csv; charset=utf-8"
    )
    response["Content-Disposition"] = 'attachment; filename="radio-zimbabwe-chart.csv"'
    return response
