"""Admin-only, station-scoped AI matching controls and sanitized diagnostics."""
import json
import math
from datetime import timedelta

from django.conf import settings
from django.db import transaction
from django.db.models import Avg, Count, Q
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.http import require_POST

from apps.accounts.context_processors import get_active_station
from apps.voting.calibration import active_thresholds
from apps.voting.matching_controls import options_for_station
from apps.voting.models import MatchingControl, ReviewAudit, VoteMatchingDiagnostic
from .workspace import api_access


@api_access("admin")
def intelligence(request):
    if request.method != "GET":
        return JsonResponse({"error": "GET required."}, status=405)
    station = get_active_station(request)
    effective = options_for_station(station)
    cutoff = timezone.now() - timedelta(days=7)
    recent = VoteMatchingDiagnostic.objects.filter(
        station=station, created_at__gte=cutoff,
    )
    totals = recent.aggregate(
        total=Count("id"),
        approved=Count("id", filter=Q(decision="approved")),
        review=Count("id", filter=Q(decision="review")),
        retry=Count("id", filter=Q(decision="retry")),
        api_success=Count("id", filter=Q(api_status="success")),
        api_error=Count("id", filter=Q(api_status__in=["error", "parse_error"])),
        avg_api_ms=Avg("api_latency_ms"),
        avg_queue_ms=Avg("queue_wait_ms"),
    )
    records = list(
        recent.select_related("matched_song")
        .order_by("-updated_at")[:25]
    )
    calibration = active_thresholds(station)
    return JsonResponse({
        "station": station,
        "config": effective,
        "model_options": [
            {"value": "", "label": f"Railway default ({settings.OPENAI_MODEL})"},
            {"value": "gpt-4o-mini", "label": "GPT-4o mini"},
            *([] if settings.OPENAI_MODEL in {"", "gpt-4o-mini"} else
              [{"value": settings.OPENAI_MODEL, "label": settings.OPENAI_MODEL}]),
        ],
        "metrics": {
            key: (round(value, 1) if isinstance(value, float) else value)
            for key, value in totals.items()
        },
        "calibration": {
            "min_score": calibration.min_score, "min_gap": calibration.min_gap,
            "min_artist": calibration.min_artist, "min_title": calibration.min_title,
        },
        "items": [{
            "trace_id": str(row.trace_id), "vote_id": row.vote_id,
            "method": row.verification_method, "model": row.model_used,
            "api_status": row.api_status, "api_ms": row.api_latency_ms,
            "queue_ms": row.queue_wait_ms, "candidate_count": row.candidate_count,
            "fuzzy": row.fuzzy_score, "artist": row.artist_score,
            "title": row.title_score, "vector": row.vector_score,
            "decision": row.decision, "reason": row.decision_reason,
            "matched_song": row.matched_song.canonical_name if row.matched_song else "",
            "attempts": row.attempts, "at": row.updated_at.isoformat(),
        } for row in records],
        "note": (
            "Similarity scores are retrieval heuristics, not probabilities. "
            "A high score cannot override OpenAI and independent safety checks. "
            "Full prompts, raw API responses and listener identifiers are not exposed."
        ),
    })


@require_POST
@api_access("admin")
def save_intelligence(request):
    data = json.loads(request.body)
    station = get_active_station(request)
    model = data.get("model_override", "")
    mode = data.get("cache_mode", "inherit")
    limit = data.get("shortlist_limit")
    artist = data.get("retrieval_min_artist")
    title = data.get("retrieval_min_title")
    permitted_models = {"", settings.OPENAI_MODEL, "gpt-4o-mini"}
    if type(model) is not str or model not in permitted_models:
        return JsonResponse({"error": "Choose an approved model."}, status=400)
    if type(mode) is not str or mode not in {"inherit", "on", "off"}:
        return JsonResponse({"error": "Invalid cache policy."}, status=400)
    if type(limit) is not int or not 5 <= limit <= 24:
        return JsonResponse({"error": "Candidate shortlist must be from 5 to 24."}, status=400)
    if type(artist) not in {int, float} or type(title) not in {int, float}:
        return JsonResponse({"error": "Candidate thresholds must be numeric."}, status=400)
    if (not math.isfinite(artist) or not math.isfinite(title)
            or not 0.55 <= artist <= 0.80 or not 0.55 <= title <= 0.80):
        return JsonResponse({"error": "Retrieval thresholds must be between 0.55 and 0.80."}, status=400)

    with transaction.atomic():
        previous = MatchingControl.objects.select_for_update().filter(station=station).first()
        before = (
            {"model_override": previous.model_override, "shortlist_limit": previous.shortlist_limit,
             "cache_mode": previous.cache_mode, "retrieval_min_artist": previous.retrieval_min_artist,
             "retrieval_min_title": previous.retrieval_min_title}
            if previous else {}
        )
        values = {
            "model_override": model,
            "shortlist_limit": limit,
            "cache_mode": mode,
            "retrieval_min_artist": round(artist, 2),
            "retrieval_min_title": round(title, 2),
        }
        MatchingControl.objects.update_or_create(station=station, defaults=values)
        ReviewAudit.objects.create(
            station=station, actor=request.user, action="matching_settings",
            details={"before": before, "after": values},
        )
    return JsonResponse({"ok": True, "config": options_for_station(station)})
