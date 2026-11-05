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
            if artist == "unknown artist":
                # No separator: compare complete artist/title partitions in either
                # order. Never use substring matches that discard extra words.
                words = title.split()
                pairs = [
                    (" ".join(words[:i]), " ".join(words[i:]))
                    for i in range(1, len(words))
                ]
                pairs += [(right, left) for left, right in pairs]
                scores = [
                    (
                        SequenceMatcher(None, left, a, autojunk=False).ratio(),
                        SequenceMatcher(None, right, t, autojunk=False).ratio(),
                    )
                    for left, right in pairs
                ]
                # A no-separator vote can be the *whole title*, not an
                # artist/title pair. Compare it to the verified title as well.
                # Only accept small typos in longer multi-word titles: never
                # discard extra words or guess from a short fuzzy title.
                candidates = [
                    ((aa + tt) / 2, aa, tt)
                    for aa, tt in scores if aa >= 0.75 and tt >= 0.75
                ]
                title_similarity = SequenceMatcher(
                    None, title, t, autojunk=False
                ).ratio()
                title_only = title == t or (
                    min(len(title), len(t)) >= 10
                    and len(title.split()) == len(t.split())
                    and len(t.split()) >= 2
                    and title_similarity >= 0.94
                )
                if title_only:
                    # Artist score 0 denotes a title-only candidate; do not
                    # misrepresent the absent artist as a perfect artist match.
                    candidates.append((title_similarity, 0.0, title_similarity))
                if not candidates:
                    continue
                score, artist_score, title_score = max(
                    candidates, key=lambda row: row[0]
                )
                yield (score, artist_score, title_score, song)
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
    if normalise(artist) == "unknown artist" and a == 0.0:
        # Complete title matches (even short titles) must be unique. A typo
        # without an artist is only safe for a distinctive multi-word title,
        # with a clear lead over the runner-up. AI must still approve the vote.
        if t == 1.0:
            return song
        return song if (
            t >= 0.94 and min(len(normalise(title)), len(normalise(song.title))) >= 10
            and len(normalise(title).split()) >= 2
        ) else None
    if normalise(artist) == normalise(song.artist) and normalise(title) == normalise(
        song.title
    ):
        return song
    # Exact short title plus a near-exact artist is strong evidence too.
    # This also covers an unambiguous title-only vote after AI confirmation.
    if t == 1.0 and a >= 0.90:
        return song
    if min(len(normalise(title)), len(normalise(song.title))) < 6:
        return None
    return song if a >= 0.90 and t >= 0.90 and score >= 0.94 else None
