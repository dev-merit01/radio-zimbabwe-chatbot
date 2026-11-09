"""Edition shortlists: presenter drafts, administrator approval, immutable eligibility."""
import json
from datetime import date, timedelta

from django.db import IntegrityError, transaction
from django.db.models import Count
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from apps.accounts.context_processors import get_active_station
from apps.accounts.permissions import presenter_show, require_presenter_show
from apps.charts.routing import station_show
from apps.voting.models import (
    CandidateSongSuggestion, ChartShow, CleanedSong, EditionCandidate,
    EligibleEdition, InboundEvent, MatchKeyMapping, RawVote, ReviewAudit,
)
from apps.voting.pipeline import lock_station

from .workspace import api_access


def _permitted(request):
    return request.user.is_superuser or bool(presenter_show(request.user))


def _show(request, show_id):
    station = get_active_station(request)
    show = station_show(station, show_id, user=request.user)
    if show.kind == "legacy":
        raise ValueError("Select a regular or end-of-year chart show.")
    return show


def _edition(request, edition_id, lock=False):
    qs = EligibleEdition.objects.select_related("chart_show")
    if lock:
        qs = qs.select_for_update()
    edition = qs.filter(
        id=edition_id, station=get_active_station(request),
    ).first()
    if edition is None:
        raise ValueError("Candidate list not found for this station.")
    require_presenter_show(request, edition.chart_show)
    return edition


def _edition_window(show, chart_date):
    if show.kind == "weekly":
        if chart_date.weekday() != show.broadcast_weekday:
            raise ValueError("Chart date must match the programme's broadcast day.")
        return chart_date - timedelta(days=6), chart_date
    if show.kind == "year_end":
        if chart_date.month != 12 or not show.campaign_start or not show.campaign_end:
            raise ValueError("End-of-year shows require December and a configured campaign.")
        start, end = show.campaign_start.date(), show.campaign_end.date()
        if not start <= chart_date <= end:
            raise ValueError("Chart date must fall in the configured annual campaign.")
        return start, end
    raise ValueError("Historical charts cannot use candidate lists.")


def _serialize(edition, details=False):
    data = {
        "id": edition.pk, "chart_show_id": edition.chart_show_id,
        "chart_show": edition.chart_show.name,
        "chart_date": str(edition.chart_date),
        "voting_start": str(edition.voting_start),
        "voting_end": str(edition.voting_end),
        "status": edition.status,
        "prepared_by": edition.prepared_by.username if edition.prepared_by else "",
        "approved_by": edition.approved_by.username if edition.approved_by else "",
        "approved_at": edition.approved_at.isoformat() if edition.approved_at else None,
        "note": edition.note,
        "candidate_count": edition.candidates.count(),
    }
    if details:
        data["candidates"] = [
            {"id": row.song_id, "artist": row.song.artist,
             "title": row.song.title, "status": row.song.status}
            for row in edition.candidates.select_related("song")
            .order_by("song__artist", "song__title", "pk")
        ]
        data["suggestions"] = [
            {"id": row.pk, "artist": row.artist, "title": row.title,
             "notes": row.notes, "status": row.status,
             "matched_song_id": row.matched_song_id}
            for row in edition.suggestions.order_by("-created_at", "-pk")[:100]
        ]
    return data


@require_GET
@api_access()
def list_editions(request):
    if not _permitted(request):
        return JsonResponse({"error": "Presenter assignment required."}, status=403)
    show = _show(request, request.GET.get("show_id") and
                 int(request.GET["show_id"]) if
                 str(request.GET.get("show_id", "")).isdigit() else None)
    if show.is_archived:
        # Archived shows remain visible read-only.
        pass
    editions = list(EligibleEdition.objects.filter(
        station=show.station, chart_show=show
    ).select_related("chart_show", "prepared_by", "approved_by")
        .order_by("-chart_date", "-id")[:40])
    requested = request.GET.get("edition_id", "")
    if requested and not requested.isdigit():
        raise ValueError("Invalid edition ID.")
    chosen = next((e for e in editions if e.pk == int(requested)), None) if requested else (
        editions[0] if editions else None
    )
    if requested and not chosen:
        raise ValueError("Candidate list not found.")
    payload = {
        "station": show.station, "chart_show_id": show.pk,
        "chart_show": show.name, "chart_size": show.chart_size,
        "broadcast_weekday": show.broadcast_weekday,
        "kind": show.kind,
        "eligibility_enforced": show.eligibility_enforced,
        "is_admin": request.user.is_superuser,
        "can_prepare": _permitted(request) and not show.is_archived,
        "editions": [_serialize(e) for e in editions],
        "selected": _serialize(chosen, True) if chosen else None,
    }
    if chosen:
        # Interest from votes that match no approved candidate; never count as
        # official votes. No listener phone numbers are exposed.
        included = list(chosen.candidates.values_list("song_id", flat=True))
        keys = MatchKeyMapping.objects.filter(
            station=show.station, cleaned_song_id__in=included
        ).values_list("match_key", flat=True)
        other = RawVote.objects.filter(
            station=show.station, chart_show=show,
            vote_date__range=(chosen.voting_start, chosen.voting_end),
        ).exclude(match_key__in=keys)
        payload["nomination_interest"] = list(
            other.values("display_name").annotate(n=Count("id"))
            .order_by("-n", "display_name")[:25]
        )
    return JsonResponse(payload)


@require_POST
@api_access()
def create_edition(request):
    if not _permitted(request):
        return JsonResponse({"error": "Presenter assignment required."}, status=403)
    data = json.loads(request.body)
    show = _show(request, data.get("chart_show_id"))
    if show.is_archived:
        raise ValueError("Archived shows cannot accept new candidate lists.")
    try:
        chart_date = date.fromisoformat(data["chart_date"])
    except (TypeError, KeyError) as exc:
        raise ValueError("Enter a valid chart date.") from exc
    start, end = _edition_window(show, chart_date)
    if end < timezone.localdate():
        raise ValueError("Cannot create candidate lists for past editions.")
    with transaction.atomic():
        lock_station(show.station)
        edition, created = EligibleEdition.objects.get_or_create(
            station=show.station, chart_show=show, chart_date=chart_date,
            defaults={
                "voting_start": start, "voting_end": end,
                "prepared_by": request.user,
            },
        )
        if created:
            ReviewAudit.objects.create(
                station=show.station, actor=request.user, action="edition_created",
                details={"edition_id": edition.pk, "show_id": show.pk},
            )
    return JsonResponse({"ok": True, "created": created, "edition": _serialize(edition)})


@require_POST
@api_access()
def edit_candidate(request):
    if not _permitted(request):
        return JsonResponse({"error": "Presenter assignment required."}, status=403)
    data = json.loads(request.body)
    edition_id, song_id, action = data.get("edition_id"), data.get("song_id"), data.get("action")
    if type(edition_id) is not int or type(song_id) is not int:
        raise ValueError("Expected edition ID and verified song ID.")
    if action not in ("add", "remove"):
        raise ValueError("Choose add or remove.")
    with transaction.atomic():
        station = get_active_station(request)
        lock_station(station)
        edition = _edition(request, edition_id, lock=True)
        if edition.status not in ("draft", "returned"):
            raise ValueError("This candidate list is submitted or locked.")
        song = CleanedSong.objects.filter(
            pk=song_id, station=edition.station, status="verified"
        ).first()
        if not song:
            raise ValueError("Only verified songs from this station may be selected.")
        if action == "add":
            EditionCandidate.objects.get_or_create(edition=edition, song=song)
        else:
            EditionCandidate.objects.filter(edition=edition, song=song).delete()
        ReviewAudit.objects.create(
            station=station, actor=request.user, action="edition_candidate",
            details={"edition_id": edition.pk, "song_id": song.pk, "action": action},
        )
    return JsonResponse({"ok": True})


@require_POST
@api_access()
def suggest_song(request):
    if not _permitted(request):
        return JsonResponse({"error": "Presenter assignment required."}, status=403)
    data = json.loads(request.body)
    edition_id = data.get("edition_id")
    if type(edition_id) is not int:
        raise ValueError("Invalid candidate list.")
    artist, title, notes = data.get("artist"), data.get("title"), data.get("notes", "")
    if not (isinstance(artist, str) and isinstance(title, str)
            and isinstance(notes, str) and
            2 <= len(artist.strip()) <= 240 and 2 <= len(title.strip()) <= 240
            and len(notes) <= 500):
        raise ValueError("Provide an artist and title of 2–240 characters.")
    with transaction.atomic():
        station = get_active_station(request)
        lock_station(station)
        edition = _edition(request, edition_id, lock=True)
        if edition.status not in ("draft", "returned"):
            raise ValueError("This list is no longer open for suggestions.")
        if CleanedSong.objects.filter(
            station=station, canonical_name__iexact=f"{artist.strip()} - {title.strip()}",
            status="verified",
        ).exists():
            raise ValueError("That song is already verified. Search and add it directly.")
        proposal = CandidateSongSuggestion.objects.create(
            edition=edition, artist=artist.strip(), title=title.strip(),
            notes=notes.strip(), suggested_by=request.user,
        )
        ReviewAudit.objects.create(
            station=station, actor=request.user, action="song_suggested",
            details={"suggestion_id": proposal.pk, "edition_id": edition.pk},
        )
    return JsonResponse({"ok": True, "suggestion_id": proposal.pk})


@require_POST
@api_access()
def submit_edition(request):
    if not _permitted(request):
        return JsonResponse({"error": "Presenter assignment required."}, status=403)
    data = json.loads(request.body)
    edition_id = data.get("edition_id")
    if type(edition_id) is not int:
        raise ValueError("Invalid candidate list.")
    with transaction.atomic():
        station = get_active_station(request)
        lock_station(station)
        edition = _edition(request, edition_id, lock=True)
        if edition.status not in ("draft", "returned"):
            raise ValueError("This list cannot be resubmitted.")
        if not edition.candidates.exists() and not edition.suggestions.filter(status="pending").exists():
            raise ValueError("Add verified songs or propose a new song first.")
        edition.status = "submitted"
        edition.save(update_fields=["status", "updated_at"])
        ReviewAudit.objects.create(
            station=station, actor=request.user, action="edition_submitted",
            details={"edition_id": edition.pk},
        )
    return JsonResponse({"ok": True, "status": edition.status})


@require_POST
@api_access("admin")
def review_suggestion(request):
    data = json.loads(request.body)
    proposal_id, action = data.get("suggestion_id"), data.get("action")
    if type(proposal_id) is not int or action not in ("accept", "reject"):
        raise ValueError("Invalid suggestion review.")
    with transaction.atomic():
        station = get_active_station(request)
        lock_station(station)
        proposal = CandidateSongSuggestion.objects.select_for_update().filter(
            pk=proposal_id, edition__station=station, status="pending"
        ).select_related("edition").first()
        if not proposal:
            raise ValueError("Suggestion not found or already reviewed.")
        if proposal.edition.status == "locked":
            raise ValueError("The approved shortlist is locked.")
        song = None
        if action == "accept":
            song_id = data.get("song_id")
            if type(song_id) is not int:
                raise ValueError("Choose a verified catalogue song to accept.")
            song = CleanedSong.objects.filter(
                pk=song_id, station=station, status="verified"
            ).first()
            if not song:
                raise ValueError("Verify the proposed song in the catalogue first.")
            EditionCandidate.objects.get_or_create(
                edition=proposal.edition, song=song,
            )
        proposal.status = "accepted" if song else "rejected"
        proposal.matched_song = song
        proposal.reviewed_by = request.user
        proposal.reviewed_at = timezone.now()
        proposal.save(update_fields=[
            "status", "matched_song", "reviewed_by", "reviewed_at",
        ])
        ReviewAudit.objects.create(
            station=station, actor=request.user, action="suggestion_reviewed",
            details={"suggestion_id": proposal.pk, "action": action,
                     "song_id": song.pk if song else None},
        )
    return JsonResponse({"ok": True})


@require_POST
@api_access("admin")
def review_edition(request):
    data = json.loads(request.body)
    edition_id, action = data.get("edition_id"), data.get("action")
    if type(edition_id) is not int or action not in ("approve", "return"):
        raise ValueError("Invalid candidate list review.")
    with transaction.atomic():
        station = get_active_station(request)
        lock_station(station)
        edition = _edition(request, edition_id, lock=True)
        if edition.status != "submitted":
            raise ValueError("Only submitted candidate lists may be reviewed.")
        if action == "return":
            edition.status = "returned"
            edition.note = str(data.get("note", ""))[:500]
            edition.save(update_fields=["status", "note", "updated_at"])
        else:
            # No switching eligibility on after listeners have already
            # participated in this period; no silent retroactive exclusions.
            current_window = _edition_window(edition.chart_show, edition.chart_date)
            if current_window != (edition.voting_start, edition.voting_end):
                raise ValueError("Programme dates changed. Create a new shortlist.")
            if RawVote.objects.filter(
                station=station, chart_show=edition.chart_show,
                vote_date__range=(edition.voting_start, edition.voting_end),
            ).exists() or InboundEvent.objects.filter(
                station=station, chart_show=edition.chart_show,
                received_at__date__range=(edition.voting_start, edition.voting_end),
            ).exists():
                raise ValueError("Voting has already started for this edition. "
                                 "Prepare the candidate list for the next edition.")
            if edition.voting_start < timezone.localdate():
                raise ValueError("A shortlist must be approved before its voting period.")
            if edition.voting_start == timezone.localdate() and edition.chart_show.accepts_at(timezone.now()):
                raise ValueError("Voting is currently open. Approval must precede voting.")
            if edition.suggestions.filter(status="pending").exists():
                raise ValueError("Review all suggested songs before approving this shortlist.")
            if not edition.candidates.exists():
                raise ValueError("An approved edition must contain verified songs.")
            if edition.candidates.exclude(
                song__station=station, song__status="verified"
            ).exists():
                raise ValueError("All selected songs must still be verified for this station.")
            if EligibleEdition.objects.filter(
                chart_show=edition.chart_show, status="locked",
                voting_start__lte=edition.voting_end,
                voting_end__gte=edition.voting_start,
            ).exclude(pk=edition.pk).exists():
                raise ValueError("A locked edition already covers these voting dates.")
            edition.status = "locked"
            edition.approved_by = request.user
            edition.approved_at = timezone.now()
            edition.save(update_fields=[
                "status", "approved_by", "approved_at", "updated_at",
            ])
            show = edition.chart_show
            show.eligibility_enforced = True
            show.save(update_fields=["eligibility_enforced", "updated_at"])
        ReviewAudit.objects.create(
            station=station, actor=request.user,
            action="edition_approved" if action == "approve" else "edition_returned",
            details={"edition_id": edition.pk, "show_id": edition.chart_show_id,
                     "candidate_count": edition.candidates.count()},
        )
    return JsonResponse({"ok": True, "status": edition.status})
