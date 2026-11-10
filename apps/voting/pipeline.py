"""One authoritative station-scoped review and reconciliation pipeline."""

import logging
from itertools import islice

from django.conf import settings
from django.db import transaction
from django.db.models import Count, OuterRef, Subquery, Sum

from .models import (
    CleanedSong,
    CleanedSongTally,
    LLMDecisionLog,
    MatchKeyMapping,
    SongAlias,
    RawSongTally,
    RawVote,
    ReviewAudit,
    StationState,
)
from .presentation import music_name

logger = logging.getLogger(__name__)

def lock_station(station):
    obj, _ = StationState.objects.get_or_create(station=station)
    return StationState.objects.select_for_update().get(pk=obj.pk)


def duplicate_adjustments(station, mappings, date_range=None):
    """Subtract repeated listener/day/song submissions without deleting evidence.

    Keep legacy raw tallies intact: only duplicates backed by individual raw
    submissions are deducted. Imported historical totals remain available.
    """
    if settings.ALLOW_REPEAT_SONG:
        return {}
    votes = RawVote.objects.filter(
        station=station, match_key__in=mappings.values("match_key"),
        ai_status__in=("legacy", "approved"),
    )
    if date_range:
        votes = votes.filter(vote_date__range=date_range)
    votes = votes.annotate(
        song_id=Subquery(
            mappings.filter(match_key=OuterRef("match_key"))
            .order_by()
            .values("cleaned_song_id")[:1]
        )
    )
    groups = (
        votes.order_by()
        .values("vote_date", "song_id")
        .annotate(extra=Count("id") - Count("user_id", distinct=True))
        .filter(extra__gt=0)
    )
    return {
        (r["vote_date"], r["song_id"]): r["extra"]
        for r in groups.iterator(chunk_size=500)
    }


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
        duplicates = duplicate_adjustments(station, mapping_qs, date_range)
        # RawSongTally intentionally retains all received submissions. Exclude
        # new votes not yet approved by AI/reviewer from published counts.
        excluded = RawVote.objects.filter(
            station=station, ai_status__in=("awaiting", "review"),
            match_key__in=mapping_qs.order_by().values("match_key"),
        ).annotate(
            song_id=Subquery(
                mapping_qs.filter(match_key=OuterRef("match_key"))
                .order_by().values("cleaned_song_id")[:1]
            )
        )
        if date_range is not None:
            excluded = excluded.filter(vote_date__range=date_range)
        exclusions = {
            (row["vote_date"], row["song_id"]): row["n"]
            for row in excluded.order_by().values("vote_date", "song_id")
            .annotate(n=Count("id")).iterator(chunk_size=500)
        }
        cleaned.delete()
        rows = (
            CleanedSongTally(
                station=station,
                date=row["date"],
                cleaned_song_id=row["song_id"],
                count=max(
                    0, row["total"]
                    - duplicates.get((row["date"], row["song_id"]), 0)
                    - exclusions.get((row["date"], row["song_id"]), 0)
                ),
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
    # Votes awaiting or declined by AI stay in raw auditing totals only.
    excluded = RawVote.objects.filter(
        station=station, vote_date=date, match_key__in=keys,
        ai_status__in=("awaiting", "review"),
    ).count()
    count = max(0, count - excluded)
    if not settings.ALLOW_REPEAT_SONG:
        votes = RawVote.objects.filter(
            station=station, vote_date=date, match_key__in=keys,
            ai_status__in=("legacy", "approved"),
        )
        counts = votes.aggregate(
            total=Count("id"), listeners=Count("user_id", distinct=True)
        )
        count = max(0, count - (counts["total"] - counts["listeners"]))
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

    # No catalogue auto-match, cached mapping or prior listener can bypass AI
    # for newly accepted votes while this mode is enabled.
    if settings.AI_ALL_VOTES:
        return process_vote_with_ai(vote)

    station = vote.station
    mapping = MatchKeyMapping.objects.filter(
        station=station, match_key=vote.match_key, cleaned_song__station=station
    ).first()
    candidate = None
    decision = None
    if not mapping:
        from .catalogue_matching import automatic_candidate, ranked_candidates

        ranked = ranked_candidates(
            station, vote.artist_normalized, vote.song_normalized
        )
        candidate = automatic_candidate(
            ranked, vote.artist_normalized, vote.song_normalized
        )
        if candidate:
            decision = {
                "action": "match",
                "confidence": "high",
                "reasoning": "Conservative catalogue similarity with runner-up gap.",
            }
        elif settings.AUTO_AI_MATCH and ranked:
            shortlist = [row[3] for row in ranked]
            result = match_vote_with_llm(
                vote.artist_raw,
                vote.song_raw,
                [
                    {"id": song.id, "canonical_name": song.canonical_name}
                    for song in shortlist
                ],
            )
            if str(result.get("reasoning", "")).startswith(
                ("LLM error:", "LLM response parsing error:")
            ):
                raise RuntimeError("AI matching unavailable")
            allowed = {song.id: song for song in shortlist}
            matched_id = result.get("matched_song_id")
            # AI cannot override ambiguity between near-identical candidates.
            gap = ranked[0][0] - ranked[1][0] if len(ranked) > 1 else 1
            if (
                result.get("matched") is True
                and result.get("confidence") == "high"
                and type(matched_id) is int
                and gap >= 0.08
                and matched_id == ranked[0][3].pk
                and min(len(vote.song_normalized), len(ranked[0][3].title)) >= 6
            ):
                candidate = allowed.get(matched_id)
            decision = {
                "action": "match" if candidate else "new",
                "confidence": "high" if candidate else "none",
                "reasoning": "AI shortlist decision: "
                + str(result.get("reasoning", ""))[:2000],
            }
    # External calls complete before taking a database lock.
    with transaction.atomic():
        lock_station(station)
        mapping = MatchKeyMapping.objects.filter(
            station=station, match_key=vote.match_key, cleaned_song__station=station
        ).first()
        if not mapping:
            if candidate:
                candidate = CleanedSong.objects.filter(
                    pk=candidate.pk,
                    station=station,
                    status="verified",
                    artist=candidate.artist,
                    title=candidate.title,
                ).first()
            if not candidate:
                candidate = CleanedSong.objects.filter(
                    station=station, canonical_name__iexact=vote.display_name
                ).first()
            if not candidate:
                candidate = CleanedSong.objects.create(
                    station=station,
                    artist=music_name(vote.artist_normalized),
                    title=music_name(vote.song_normalized),
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
            if decision and decision["reasoning"].startswith("AI shortlist"):
                LLMDecisionLog.objects.create(
                    station=station,
                    input_text=vote.raw_input,
                    input_type="raw_vote",
                    action=decision["action"],
                    confidence=decision["confidence"],
                    reasoning=decision["reasoning"],
                    matched_song=candidate,
                    matched_song_name=candidate.canonical_name,
                    was_applied=candidate.status == "verified",
                )
            if candidate.status == "verified":
                ReviewAudit.objects.create(
                    station=station,
                    actor=None,
                    action="auto_match",
                    details={
                        "vote_id": vote.pk,
                        "song_id": candidate.pk,
                        "method": "ai"
                        if decision and decision["reasoning"].startswith("AI shortlist")
                        else "catalogue",
                    },
                )
        total = (
            RawSongTally.objects.filter(
                station=station, match_key=vote.match_key
            ).aggregate(n=Sum("count"))["n"]
            or 0
        )
        MatchKeyMapping.objects.filter(pk=mapping.pk).update(vote_count=total)
        update_song_tally(station, mapping.cleaned_song_id, vote.vote_date)



def unresolved_mapping(mapping):
    """Only untouched machine-created pending entries may be reconsidered."""
    if not mapping or mapping.cleaned_song.status != "pending":
        return False
    from django.db.models import Q
    song_id = mapping.cleaned_song_id
    reviewed = ReviewAudit.objects.filter(station=mapping.station).filter(
        Q(details__before__id=song_id) | Q(details__song_id=song_id)
    ).exclude(action__in=("auto_match", "ai_remap"))
    return not reviewed.exists() and not RawVote.objects.filter(
        station=mapping.station, match_key=mapping.match_key,
        ai_status__in=("approved", "legacy"),
    ).exists()


def process_vote_with_ai(vote):
    """Verify EVERY queued valid vote with OpenAI before publishing any tally.

    An AI network or parsing failure leaves the durable worker job for retry.
    Unknown/mismatched responses are saved for review, never auto-counted.
    """
    from .ai import match_vote_with_llm
    from .catalogue_matching import (
        automatic_candidate,
        artist_credits,
        normalise,
        ranked_candidates,
        variants,
    )

    # Retried/duplicate work must not purchase additional AI requests.
    if RawVote.objects.filter(pk=vote.pk, ai_status__in=("approved", "review")).exists():
        return

    station = vote.station
    mapping = MatchKeyMapping.objects.select_related("cleaned_song").filter(
        station=station, match_key=vote.match_key,
        cleaned_song__station=station,
    ).first()
    ranked = ranked_candidates(station, vote.artist_normalized, vote.song_normalized)
    shortlist = [row[3] for row in ranked]
    if (mapping and mapping.cleaned_song.status == "verified"
            and mapping.cleaned_song_id not in {song.pk for song in shortlist}):
        shortlist = [mapping.cleaned_song] + shortlist[:7]
    # Give OpenAI actual musical identities, not only opaque full strings.
    # Store approved station-scoped aliases separately from model guesses.
    approved_aliases = {}
    for row in SongAlias.objects.filter(
        station=station, is_approved=True, song__id__in=[song.pk for song in shortlist],
        song__station=station, song__status="verified",
    ).values("song_id", "value").order_by("id"):
        approved_aliases.setdefault(row["song_id"], []).append(row["value"])
    candidates = []
    for song in shortlist:
        lead, featured = artist_credits(song.artist)
        candidates.append({
            "id": song.pk, "canonical_name": song.canonical_name,
            "primary_artist": lead, "featured_artists": featured,
            "title": song.title, "aliases": approved_aliases.get(song.pk, [])[:6],
        })
    result = match_vote_with_llm(vote.artist_raw, vote.song_raw, candidates)
    reason = str(result.get("reasoning", ""))
    if reason.startswith(("LLM error:", "LLM response parsing error:")):
        raise RuntimeError("AI verification unavailable")

    chosen_id = result.get("matched_song_id")
    chosen = None
    if (result.get("matched") is True
            and result.get("confidence") == "high"
            and type(chosen_id) is int):
        selected = next((song for song in shortlist if song.pk == chosen_id), None)
        if selected and selected.status == "verified":
            if mapping and not unresolved_mapping(mapping):
                # Never override a reviewed, rejected or verified mapping.
                if mapping.cleaned_song_id == selected.pk:
                    chosen = selected
            elif ranked and selected.pk == ranked[0][3].pk:
                exact = (
                    normalise(vote.artist_normalized) == normalise(selected.artist)
                    and normalise(vote.song_normalized) == normalise(selected.title)
                )
                gap = ranked[0][0] - ranked[1][0] if len(ranked) > 1 else 1
                safe = automatic_candidate(ranked, vote.artist_normalized,
                                           vote.song_normalized) == selected
                # OpenAI confirmation is necessary, not sufficient. Both
                # artist and title must independently support an unambiguous
                # candidate; the former length-only fallback could approve a
                # wrong song if there were no close runner-up.
                _score, artist_score, title_score, _song = ranked[0]
                fuzzy = (
                    gap >= 0.08
                    and _score >= 0.88
                    and artist_score >= 0.83
                    and title_score >= 0.83
                    and min(
                        len(normalise(vote.song_normalized)),
                        len(normalise(selected.title)),
                    ) >= 6
                )
                if variants(vote.song_normalized) == variants(selected.title) and (
                    exact or safe or fuzzy
                ):
                    chosen = selected

    with transaction.atomic():
        lock_station(station)
        current = RawVote.objects.select_for_update().get(pk=vote.pk, station=station)
        if current.ai_status in ("approved", "review"):
            return
        mapping = MatchKeyMapping.objects.select_related("cleaned_song").filter(
            station=station, match_key=vote.match_key,
            cleaned_song__station=station,
        ).first()
        if chosen:
            chosen = CleanedSong.objects.filter(
                pk=chosen.pk, station=station, status="verified",
                artist=chosen.artist, title=chosen.title,
            ).first()
        if mapping and chosen and mapping.cleaned_song_id != chosen.pk:
            if unresolved_mapping(mapping):
                previous = mapping.cleaned_song
                mapping.cleaned_song = chosen
                mapping.is_auto_mapped = True
                mapping.save(update_fields=["cleaned_song", "is_auto_mapped"])
                ReviewAudit.objects.create(
                    station=station, action="ai_remap",
                    details={"vote_id": vote.pk, "from_song_id": previous.pk,
                             "song_id": chosen.pk},
                )
                # Retain the old placeholder for audit, remove it from review.
                if not MatchKeyMapping.objects.filter(cleaned_song=previous).exists():
                    previous.status = "rejected"
                    previous.save(update_fields=["status"])
                rebuild_tallies(station, song_ids=[previous.pk, chosen.pk])
            else:
                chosen = None
        if not mapping:
            target = chosen
            if not target:
                target = CleanedSong.objects.filter(
                    station=station, canonical_name__iexact=vote.display_name
                ).first()
                # Existing verified entries can hold a review mapping, but
                # remain UNCOUNTED because this vote has not passed AI.
            if not target:
                from .presentation import music_name
                target = CleanedSong.objects.create(
                    station=station,
                    artist=music_name(vote.artist_normalized),
                    title=music_name(vote.song_normalized),
                    canonical_name=vote.display_name,
                    status="pending",
                )
            mapping = MatchKeyMapping.objects.create(
                station=station, match_key=vote.match_key,
                cleaned_song=target,
                sample_display_name=vote.display_name,
                is_auto_mapped=chosen is not None,
            )
            if chosen:
                ReviewAudit.objects.create(
                    station=station, actor=None, action="auto_match",
                    details={"vote_id":vote.pk,"song_id":chosen.pk,"method":"ai"},
                )
        approved = chosen is not None and mapping.cleaned_song_id == chosen.pk
        current.ai_status = "approved" if approved else "review"
        current.save(update_fields=["ai_status"])
        LLMDecisionLog.objects.create(
            station=station, input_text=vote.raw_input, input_type="raw_vote",
            action="match" if approved else "new",
            confidence="high" if approved else result.get("confidence", "none"),
            reasoning=("AI per-vote decision: " + reason)[:2000],
            matched_song=mapping.cleaned_song,
            matched_song_name=mapping.cleaned_song.canonical_name,
            was_applied=approved,
        )
        total = (RawSongTally.objects.filter(
            station=station, match_key=vote.match_key
        ).aggregate(n=Sum("count"))["n"] or 0)
        MatchKeyMapping.objects.filter(pk=mapping.pk).update(vote_count=total)
        update_song_tally(station, mapping.cleaned_song_id, vote.vote_date)
        tally = CleanedSongTally.objects.filter(
            station=station, cleaned_song_id=mapping.cleaned_song_id,
            date=vote.vote_date,
        ).values_list("count", flat=True).first() or 0
        logger.info("AI vote result station=%s vote_id=%s status=%s song_id=%s day_count=%s",
                    station, current.pk, current.ai_status, mapping.cleaned_song_id, tally)


def review_song(
    station, actor, song_id, action, target_id=None, artist=None, title=None
):
    with transaction.atomic():
        lock_station(station)
        song = CleanedSong.objects.get(pk=song_id, station=station)
        # Do not silently remove, rename or unverify songs after the
        # presenter has published them in an official candidate list.
        if action in {"pending", "rejected", "merge", "edit"}:
            from .models import EditionCandidate
            if EditionCandidate.objects.filter(
                song=song, edition__status="locked",
            ).exists():
                raise ValueError(
                    "This song is on a published eligible list. "
                    "Preserve the official edition and review changes separately."
                )
        affected = [song.id]
        review_keys = list(MatchKeyMapping.objects.filter(
            station=station, cleaned_song=song
        ).values_list("match_key", flat=True))
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
            artist, title = music_name(artist), music_name(title)
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
        if action in {"verified", "merge"}:
            # Manual approval resolves reviewed (already AI-checked) votes,
            # but never silently approves votes whose AI jobs are still queued.
            RawVote.objects.filter(
                station=station, match_key__in=review_keys,
                ai_status="review",
            ).update(ai_status="approved")
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
