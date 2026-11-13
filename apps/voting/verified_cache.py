"""Short-lived reuse of a previously model-confirmed vote spelling.

This is a cache of catalogue identity, NOT a cache of a counted vote. Every
new listener submission still passes webhook deduplication, spam rules,
station/chart routing, per-listener limits, and atomic tally reconciliation.
Never let an AI model or local similarity score alone populate the cache.
"""
from datetime import timedelta

from django.conf import settings
from django.utils import timezone

from .catalogue_matching import (
    automatic_candidate, normalise, ranked_candidates, variants,
)
from .models import VerifiedVoteCache
from .matching_controls import options_for_station


def reusable_song(vote):
    if not options_for_station(vote.station)["cache_enabled"]:
        return None
    spelling = normalise(vote.raw_input)
    if not spelling or len(spelling) > 512:
        return None
    row = (
        VerifiedVoteCache.objects.select_related("song")
        .filter(
            station=vote.station,
            normalised_text=spelling,
            expires_at__gt=timezone.now(),
            song__station=vote.station,
            song__status="verified",
        )
        .first()
    )
    if not row:
        return None
    song = row.song
    if (
        song.artist != row.artist_snapshot
        or song.title != row.title_snapshot
        or variants(vote.song_normalized) != variants(song.title)
    ):
        return None
    # Recheck the COMPLETE current station catalogue on every cache hit:
    # added/remixed/renamed songs may invalidate an old spelling decision.
    ranked = ranked_candidates(
        vote.station, vote.artist_normalized, vote.song_normalized,
        exhaustive=True,
    )
    return song if automatic_candidate(
        ranked, vote.artist_normalized, vote.song_normalized,
    ) == song else None


def remember_verified_spelling(vote, song):
    """Call only after an actual OpenAI decision has been accepted.

    Called under the station row lock by process_vote_with_ai.
    """
    if not getattr(settings, "AI_MATCH_CACHE_ENABLED", False):
        return
    seconds = getattr(settings, "AI_MATCH_CACHE_SECONDS", 600)
    if not isinstance(seconds, int) or not 1 <= seconds <= 3600:
        return
    spelling = normalise(vote.raw_input)
    if not spelling or len(spelling) > 512:
        return
    now = timezone.now()
    VerifiedVoteCache.objects.update_or_create(
        station=vote.station, normalised_text=spelling,
        defaults={
            "song": song, "artist_snapshot": song.artist,
            "title_snapshot": song.title,
            "verified_at": now, "expires_at": now + timedelta(seconds=seconds),
            "source": "openai",
        },
    )
