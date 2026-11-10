"""Station-local PostgreSQL trigram/pattern candidate prefilter.

All candidate acceptance still belongs to catalogue_matching/pipeline.
GIN pg_trgm indexes support ILIKE substring and trigram searches; SQLite
tests use the original full-catalogue fallback for identical behaviour.
"""
import logging
from functools import lru_cache

from django.conf import settings
from django.db import connection
from django.db.models import Q

from .models import CleanedSong, SongAlias

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def trigram_available():
    if connection.vendor != "postgresql":
        return False
    with connection.cursor() as cursor:
        cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm')")
        return bool(cursor.fetchone()[0])


def narrowed_catalogue(station, artist, title):
    """Return (queryset, was_filtered) without crossing station boundaries."""
    songs = CleanedSong.objects.filter(station=station, status="verified").only(
        "id", "artist", "title", "canonical_name"
    )
    if not getattr(settings, "AI_INDEXED_SEARCH_ENABLED", True):
        return songs, False
    if connection.vendor != "postgresql":
        return songs, False

    from .catalogue_matching import normalise, strip_vote_prefix

    raw = strip_vote_prefix(normalise(
        (" " if normalise(artist) == "unknown artist" else artist + " ") + title
    ))
    # Whole phrases are still compared later in the exhaustive matcher.
    tokens = []
    for token in raw.split():
        if len(token) >= 3 and token not in tokens:
            tokens.append(token)
    # Cap SQL complexity on unusually long listener messages.
    tokens = sorted(tokens, key=len, reverse=True)[:8]
    if not tokens:
        return songs, False

    predicate = Q()
    for token in tokens:
        predicate |= (
            Q(artist__icontains=token) | Q(title__icontains=token)
            | Q(canonical_name__icontains=token)
        )
    try:
        trigrams = trigram_available()
    except Exception as exc:
        logger.warning("Trigram extension inspection unavailable (%s)", type(exc).__name__)
        trigrams = False
    if trigrams:
        for token in tokens:
            predicate |= (
                Q(artist__trigram_similar=token)
                | Q(title__trigram_similar=token)
            )

    # Curated aliases can carry spellings very different from canonical names.
    alias_predicate = Q()
    for token in tokens:
        alias_predicate |= Q(normalised__icontains=token)
        if trigrams:
            alias_predicate |= Q(normalised__trigram_similar=token)
    alias_ids = SongAlias.objects.filter(
        station=station, is_approved=True, song__station=station,
        song__status="verified",
    ).filter(alias_predicate).values("song_id")
    predicate |= Q(pk__in=alias_ids)
    return songs.filter(predicate).distinct(), True
