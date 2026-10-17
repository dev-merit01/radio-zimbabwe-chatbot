"""Conservative, station-scoped candidate retrieval for the live worker.

Scores are string similarities, not probabilities. Short/ambiguous titles and
recording variants require review. No catalogue mutation or external I/O here.
"""

import heapq
import re
import unicodedata
from difflib import SequenceMatcher
from .models import CleanedSong


def normalise(value):
    value = unicodedata.normalize("NFKC", value).casefold()
    return " ".join(re.findall(r"\w+", value, flags=re.UNICODE))


def variants(value):
    return set(
        re.findall(
            r"\b(?:remix|live|acoustic|instrumental|edit|extended)\b", normalise(value)
        )
    )


def ranked_candidates(station, artist, title, limit=8):
    artist, title = normalise(artist), normalise(title)
    if artist == "unknown artist":
        return []

    def scored():
        songs = (
            CleanedSong.objects.filter(station=station, status="verified")
            .only("id", "artist", "title", "canonical_name")
            .iterator(chunk_size=500)
        )
        for song in songs:
            a, t = normalise(song.artist), normalise(song.title)
            if variants(title) != variants(t):
                continue
            artist_score = SequenceMatcher(None, artist, a, autojunk=False).ratio()
            if artist_score < 0.75:
                continue
            title_score = SequenceMatcher(None, title, t, autojunk=False).ratio()
            if title_score < 0.75:
                continue
            yield ((artist_score + title_score) / 2, artist_score, title_score, song)

    return heapq.nlargest(limit, scored(), key=lambda row: (row[0], -row[3].pk))


def automatic_candidate(ranked, artist, title):
    if not ranked:
        return None
    score, a, t, song = ranked[0]
    exact = [
        row[3]
        for row in ranked
        if normalise(artist) == normalise(row[3].artist)
        and normalise(title) == normalise(row[3].title)
    ]
    if exact:
        return exact[0] if len(exact) == 1 else None
    # Non-exact matches require a substantial runner-up gap.
    if len(ranked) > 1 and score - ranked[1][0] < 0.08:
        return None
    if normalise(artist) == normalise(song.artist) and normalise(title) == normalise(
        song.title
    ):
        return song
    if min(len(normalise(title)), len(normalise(song.title))) < 6:
        return None
    return song if a >= 0.90 and t >= 0.90 and score >= 0.94 else None
