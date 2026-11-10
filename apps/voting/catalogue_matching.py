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


def phonetic_spelling(value):
    """Compare common spoken/transliterated spellings as an additional signal.

    This is NOT an identity check. Only the AI + an unambiguous title and artist
    match can approve a vote. Keep it deliberately small: broad phonetic keys
    would collapse unrelated artists in a national music catalogue.
    """
    value = normalise(value)
    for old, new in (
        ("ph", "f"), ("ee", "i"), ("ea", "i"), ("ie", "i"),
        ("oo", "u"), ("ou", "u"), ("ck", "k"),
    ):
        value = value.replace(old, new)
    return value


def artist_similarity(submitted, canonical):
    """Compare artist spellings with optional stage prefixes/suffixes.

    Listeners often drop an abbreviation from a performer's stage name
    ("Friman" for "Freeman HKD"). Preserve the exact artist, compare a
    well-supported alternative, and require title evidence as well.
    """
    submitted, canonical = normalise(submitted), normalise(canonical)
    if not submitted or not canonical:
        return 0.0

    words = canonical.split()
    alternatives = {canonical}
    if len(words) >= 2 and words[0] in {"jnr", "jr", "junior", "dj", "mc"}:
        alternatives.add(" ".join(words[1:]))
    # These are branding suffixes/initialisms, not musical title words.
    # Do not strip arbitrary artist tokens: e.g. "Winky D" must stay whole.
    if len(words) >= 2 and words[-1] in {"hkd", "official", "music"}:
        alternatives.add(" ".join(words[:-1]))

    return max(
        max(
            SequenceMatcher(None, submitted, alternative, autojunk=False).ratio(),
            SequenceMatcher(
                None, phonetic_spelling(submitted),
                phonetic_spelling(alternative), autojunk=False,
            ).ratio(),
        )
        for alternative in alternatives
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
                        artist_similarity(left, song.artist),
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
                    for aa, tt in scores if aa >= 0.75 and tt >= 0.70
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
            artist_score = artist_similarity(artist, song.artist)
            if artist_score < 0.75:
                continue
            title_score = SequenceMatcher(None, title, t, autojunk=False).ratio()
            # Keep nearby short-title alternatives in the shortlist so a
            # "Dak" vote cannot silently pick between "Daka" and "Dakar".
            if title_score < 0.70:
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
    if normalise(artist) == "unknown artist":
        # The incoming text can contain both parts without a dash or stage-name
        # prefix/suffix ("Spraga Dak" -> "Jnr Spragga - Daka",
        # "Friman Gitar" -> "Freeman HKD - Gitare"). The split must strongly
        # identify the artist AND almost identify the title. AI must
        # independently approve before the vote is counted.
        if a >= 0.90 and t >= 0.84:
            return song
        return None
    if normalise(artist) == normalise(song.artist) and normalise(title) == normalise(
        song.title
    ):
        return song
    # A one-character typo in a short title may be recovered only with a
    # uniquely strong artist match. Competing songs fail the runner-up gap.
    if (
        a >= 0.90 and t >= 0.84
        and len(normalise(title).split()) == len(normalise(song.title).split()) == 1
        and 3 <= len(normalise(title)) <= 5
        and 3 <= len(normalise(song.title)) <= 5
    ):
        return song
    # Exact short title plus a near-exact artist is strong evidence too.
    # This also covers an unambiguous title-only vote after AI confirmation.
    if t == 1.0 and a >= 0.90:
        return song
    if min(len(normalise(title)), len(normalise(song.title))) < 6:
        return None
    return song if a >= 0.90 and t >= 0.90 and score >= 0.94 else None
