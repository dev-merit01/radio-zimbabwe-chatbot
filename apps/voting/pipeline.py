"""One authoritative station-scoped review and reconciliation pipeline."""

from itertools import islice
from django.db import transaction
from django.db.models import OuterRef, Subquery, Sum
from .models import (
    StationState,
    CleanedSong,
    MatchKeyMapping,
    RawSongTally,
    CleanedSongTally,
    ReviewAudit,
)


def lock_station(station):
    obj, _ = StationState.objects.get_or_create(station=station)
    return StationState.objects.select_for_update().get(pk=obj.pk)


def rebuild_tallies(station, song_ids=None, date_range=None):
    """Reconcile all dates, including zero totals and rejected/deleted mappings."""
    with transaction.atomic():
        lock_station(station)
        mapping_qs = MatchKeyMapping.objects.filter(
            station=station,
            cleaned_song__station=station,
            cleaned_song__status="verified",
        )
        raw = RawSongTally.objects.filter(station=station)
        cleaned = CleanedSongTally.objects.filter(station=station)
        if song_ids is not None:
            mapping_qs = mapping_qs.filter(cleaned_song_id__in=song_ids)
            cleaned = cleaned.filter(cleaned_song_id__in=song_ids)
        # Resolve and aggregate in SQL: no full mapping dictionary, giant IN
        # parameter list, or application-side per-key/date totals dictionary.
        # Keep affected-song rebuilds selective through the existing
        # (station, match_key, date) index, while the IN values remain in SQL.
        raw = raw.filter(
            match_key__in=mapping_qs.order_by().values("match_key")
        ).annotate(
            song_id=Subquery(
                mapping_qs.filter(match_key=OuterRef("match_key"))
                .order_by()
                .values("cleaned_song_id")[:1]
            )
        )
        if date_range is not None:
            raw = raw.filter(date__range=date_range)
            cleaned = cleaned.filter(date__range=date_range)
        totals = raw.order_by().values("date", "song_id").annotate(total=Sum("count"))
        cleaned.delete()
        rows = (
            CleanedSongTally(
                station=station,
                date=row["date"],
                cleaned_song_id=row["song_id"],
                count=row["total"],
            )
            for row in totals.iterator(chunk_size=500)
        )
        while batch := list(islice(rows, 500)):
            CleanedSongTally.objects.bulk_create(batch, batch_size=500)


def update_song_tally(station, song_id, date):
    # Caller holds the station lock.
    song = CleanedSong.objects.get(pk=song_id, station=station)
    keys = MatchKeyMapping.objects.filter(
        station=station, cleaned_song=song
    ).values_list("match_key", flat=True)
    count = (
        RawSongTally.objects.filter(
            station=station, date=date, match_key__in=keys
        ).aggregate(n=Sum("count"))["n"]
        or 0
    )
    if song.status != "verified" or not count:
        CleanedSongTally.objects.filter(
            station=station, date=date, cleaned_song=song
        ).delete()
    else:
        CleanedSongTally.objects.update_or_create(
            station=station, date=date, cleaned_song=song, defaults={"count": count}
        )


def process_vote(vote):
    from django.conf import settings
    from .ai import match_vote_with_llm

    station = vote.station
    mapping = MatchKeyMapping.objects.filter(
        station=station, match_key=vote.match_key, cleaned_song__station=station
    ).first()
    candidate = None
    if not mapping:
        candidates = CleanedSong.objects.filter(
            station=station, status="verified", artist__iexact=vote.artist_normalized
        )
        exact = list(candidates.filter(title__iexact=vote.song_normalized)[:2])
        if len(exact) == 1:
            candidate = exact[0]
        elif settings.AUTO_AI_MATCH:
            # A relevant, bounded shortlist; no catalogue-wide API call on intake.
            shortlist = list(candidates.order_by("id")[:30])
            if shortlist:
                result = match_vote_with_llm(
                    vote.artist_raw,
                    vote.song_raw,
                    [
                        {"id": s.id, "canonical_name": s.canonical_name}
                        for s in shortlist
                    ],
                )
                if str(result.get("reasoning", "")).startswith(
                    ("LLM error:", "LLM response parsing error:")
                ):
                    raise RuntimeError("AI matching unavailable")
                allowed = {s.id: s for s in shortlist}
                if result.get("matched") and result.get("confidence") == "high":
                    matched_id = result.get("matched_song_id")
                    candidate = (
                        allowed.get(matched_id) if type(matched_id) is int else None
                    )
    # External calls complete before taking a database lock.
    with transaction.atomic():
        lock_station(station)
        mapping = MatchKeyMapping.objects.filter(
            station=station, match_key=vote.match_key, cleaned_song__station=station
        ).first()
        if not mapping:
            if candidate:
                candidate = CleanedSong.objects.filter(
                    pk=candidate.pk, station=station, status="verified"
                ).first()
            if not candidate:
                candidate = CleanedSong.objects.filter(
                    station=station, canonical_name__iexact=vote.display_name
                ).first()
            if not candidate:
                candidate = CleanedSong.objects.create(
                    station=station,
                    artist=vote.artist_raw,
                    title=vote.song_raw,
                    canonical_name=vote.display_name,
                    status="pending",
                )
            mapping = MatchKeyMapping.objects.create(
                station=station,
                match_key=vote.match_key,
                cleaned_song=candidate,
                sample_display_name=vote.display_name,
                is_auto_mapped=candidate.status == "verified",
            )
        total = (
            RawSongTally.objects.filter(
                station=station, match_key=vote.match_key
            ).aggregate(n=Sum("count"))["n"]
            or 0
        )
        MatchKeyMapping.objects.filter(pk=mapping.pk).update(vote_count=total)
        update_song_tally(station, mapping.cleaned_song_id, vote.vote_date)


def review_song(
    station, actor, song_id, action, target_id=None, artist=None, title=None
):
    with transaction.atomic():
        lock_station(station)
        song = CleanedSong.objects.get(pk=song_id, station=station)
        affected = [song.id]
        before = {"id": song.id, "name": song.canonical_name, "status": song.status}
        if action == "merge":
            target = CleanedSong.objects.get(
                pk=target_id, station=station, status="verified"
            )
            if song.pk == target.pk:
                raise ValueError("Choose a different target song.")
            MatchKeyMapping.objects.filter(station=station, cleaned_song=song).update(
                cleaned_song=target
            )
            affected.append(target.id)
            # Preserve original record and immutable archive snapshots.
            song.status = "rejected"
        elif action in {"verified", "pending", "rejected"}:
            song.status = action
        elif action == "edit":
            if not isinstance(artist, str) or not isinstance(title, str):
                raise ValueError("Artist and title must be text.")
            artist, title = artist.strip(), title.strip()
            if not artist or not title or len(artist) > 240 or len(title) > 240:
                raise ValueError(
                    "Artist and title are required (maximum 240 characters)."
                )
            canonical = f"{artist} - {title}"
            if (
                CleanedSong.objects.filter(
                    station=station, canonical_name__iexact=canonical
                )
                .exclude(pk=song.pk)
                .exists()
            ):
                raise ValueError("That song already exists. Merge it instead.")
            song.artist, song.title = artist.strip(), title.strip()
            song.canonical_name = f"{song.artist} - {song.title}"
        else:
            raise ValueError("Unknown review action.")
        song.save()
        if action != "edit":
            rebuild_tallies(station, song_ids=affected)
        ReviewAudit.objects.create(
            station=station,
            actor=actor,
            action=action,
            details={
                "before": before,
                "after": {"name": song.canonical_name, "status": song.status},
                "target_id": target_id,
            },
        )
