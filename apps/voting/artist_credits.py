"""Stable main/featured artist credits for AirVote catalogue management.

Store whole credit in CleanedSong.artist, preserving its existing canonical
identity, song IDs, voting history, aliases and AI worker matching.
No migration or separate competing song row.
"""
import re

from .presentation import music_name


_SPLIT = re.compile(r"\s+(?:feat(?:uring)?|ft)\.?\s*:?\s+", re.IGNORECASE)
_LEADING_CREDIT = re.compile(r"^(?:feat(?:uring)?|ft)\b\.?\s*:?\s*", re.IGNORECASE)


def split_artist_credits(artist):
    """Separate an existing catalogue artist credit without losing features."""
    if not isinstance(artist, str):
        return "", ""
    chunks = _SPLIT.split(artist.strip(), maxsplit=1)
    main = music_name(chunks[0])
    featured = ", ".join(
        music_name(name) for name in chunks[1].split(",") if name.strip()
    ) if len(chunks) == 2 else ""
    return main, featured


def combine_artist_credits(artist, featured_artists):
    """Validate main artist and optional comma-separated featured artists.

    The features field, when explicitly provided, replaces embedded credits,
    including when blank (intentional removal during editing). Older API
    clients omitting the field may submit 'Main feat. Guest' unchanged.
    """
    if not isinstance(artist, str):
        raise ValueError("Artist must be text.")
    if featured_artists is not None and not isinstance(featured_artists, str):
        raise ValueError("Featured artists must be text.")

    main, embedded = split_artist_credits(artist)
    if not main:
        raise ValueError("Main artist is required.")
    raw = embedded if featured_artists is None else featured_artists.strip()
    raw = _LEADING_CREDIT.sub("", raw)
    names = [music_name(value) for value in raw.split(",") if value.strip()]
    # Avoid crediting the main performer or a collaborator twice.
    deduped = []
    seen = {main.casefold()}
    for name in names:
        if name.casefold() not in seen:
            deduped.append(name)
            seen.add(name.casefold())

    combined = main + (f" feat. {', '.join(deduped)}" if deduped else "")
    if len(combined) > 240:
        raise ValueError("Artist credits are too long (maximum 240 characters combined).")
    return combined
