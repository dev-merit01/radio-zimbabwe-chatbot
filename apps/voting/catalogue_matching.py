"""Conservative, station-scoped candidate retrieval for the live worker.

Scores are string similarities, not probabilities. Short/ambiguous titles and
recording variants require review. No catalogue mutation or external I/O here.
"""

import heapq
import re
import unicodedata
from collections import defaultdict
from difflib import SequenceMatcher

from .models import CleanedSong, SongAlias


def normalise(value):
    value = unicodedata.normalize("NFKD", str(value or "")).casefold()
    value = "".join(character for character in value if not unicodedata.combining(character))
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


def text_similarity(submitted, canonical):
    """Score literal and mild phonetic spelling; never interpret as probability."""
    submitted, canonical = normalise(submitted), normalise(canonical)
    return max(
        SequenceMatcher(None, submitted, canonical, autojunk=False).ratio(),
        SequenceMatcher(
            None, phonetic_spelling(submitted),
            phonetic_spelling(canonical), autojunk=False,
        ).ratio(),
    )


def strip_vote_prefix(value):
    """Ignore only familiar, explicitly marked voting instructions.

    Keep the raw vote for audit/AI. Stripping arbitrary leading words would
    silently turn unrelated song requests into votes for catalogue entries.
    """
    value = normalise(value)
    return re.sub(
        r"^(?:(?:please )?(?:i (?:want to )?vote for|vote for|"
        r"my vote is|i choose|ndovhotera)) +",
        "", value,
    ) or value


def artist_credits(artist):
    """Return the lead artist and featured names, without dropping either."""
    pieces = re.split(
        r"\s+(?:feat(?:uring)?|ft)\.?\s*:?\s*",
        normalise(artist), maxsplit=1,
    )
    return pieces[0], (pieces[1] if len(pieces) > 1 else "")


def title_similarity(submitted, canonical, aliases=()):
    """Use word-boundary title prefixes for shortened radio votes.

    'Bhura' is a candidate for 'Bhura Dhanzi', NOT an automatic identity.
    A competing 'Bhura ...' by the same artist will survive retrieval and
    block automatic matching through the score-gap check.
    """
    submitted, canonical = normalise(submitted), normalise(canonical)
    score = text_similarity(submitted, canonical)
    if submitted and submitted in aliases:
        return 1.0
    query_words, title_words = submitted.split(), canonical.split()
    if (
        query_words and len(query_words) < len(title_words)
        and all(
            len(word) >= 3 and target.startswith(word)
            for word, target in zip(query_words, title_words)
        )
    ):
        return max(score, 0.95)
    return score


def artist_similarity(submitted, canonical, aliases=()):
    """Compare artist spellings with optional stage prefixes/suffixes.

    Listeners often drop an abbreviation from a performer's stage name
    ("Friman" for "Freeman HKD"). Preserve the exact artist, compare a
    well-supported alternative, and require title evidence as well.
    """
    submitted, canonical = normalise(submitted), normalise(canonical)
    if not submitted or not canonical:
        return 0.0

    words = canonical.split()
    primary, _featured = artist_credits(canonical)
    alternatives = {canonical, primary, *aliases}
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


def ranked_candidates(station, artist, title, limit=16):
    artist, title = normalise(artist), normalise(title)
    if artist == "unknown artist":
        title = strip_vote_prefix(title)
    else:
        artist = strip_vote_prefix(artist)

    # Approved aliases are explicitly curated. Unapproved AI suggestions must
    # never become catalogue evidence. Colliding aliases stay competing entries.
    aliases_by_song = defaultdict(lambda: defaultdict(set))
    for row in SongAlias.objects.filter(
        station=station, is_approved=True, song__station=station,
        song__status="verified",
    ).values("song_id", "kind", "normalised").iterator(chunk_size=500):
        aliases_by_song[row["song_id"]][row["kind"]].add(row["normalised"])

    def scored():
        songs = (
            CleanedSong.objects.filter(station=station, status="verified")
            .only("id", "artist", "title", "canonical_name")
            .iterator(chunk_size=500)
        )
        for song in songs:
            t = normalise(song.title)
            aliases = aliases_by_song[song.pk]
            # A full-expression alias represents an explicitly confirmed
            # listener phrase, not a fuzzy guess.
            phrase = title if artist == "unknown artist" else f"{artist} {title}"
            if phrase in aliases["full"]:
                yield (1.0, 1.0, 1.0, song)
                continue
            if artist == "unknown artist":
                if variants(title) != variants(t):
                    continue
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
                        artist_similarity(left, song.artist, aliases["artist"]),
                        title_similarity(right, t, aliases["title"]),
                    )
                    for left, right in pairs
                ]
                # A no-separator vote can be the *whole title*, not an
                # artist/title pair. Compare it to the verified title as well.
                # Only accept small typos in longer multi-word titles: never
                # discard extra words or guess from a short fuzzy title.
                candidates = [
                    ((aa + tt) / 2, aa, tt)
                    for aa, tt in scores if aa >= 0.68 and tt >= 0.66
                ]
                only_title_score = title_similarity(title, t, aliases["title"])
                title_only = title == t or title in aliases["title"] or (
                    min(len(title), len(t)) >= 10
                    and len(title.split()) == len(t.split())
                    and len(t.split()) >= 2
                    and only_title_score >= 0.88
                )
                if title_only:
                    # Artist score 0 denotes a title-only candidate; do not
                    # misrepresent the absent artist as a perfect artist match.
                    candidates.append((only_title_score, 0.0, only_title_score))
                if not candidates:
                    continue
                score, artist_score, title_score = max(
                    candidates, key=lambda row: row[0]
                )
                yield (score, artist_score, title_score, song)
                continue
            # An explicit separator may still be backwards: "Gitare - Freeman
            # HKD". Offer both interpretations to OpenAI, keeping title/version
            # checks and the same conservative acceptance gate afterwards.
            interpretations = []
            if variants(title) == variants(t):
                interpretations.append((
                    artist_similarity(artist, song.artist, aliases["artist"]),
                    title_similarity(title, t, aliases["title"]),
                ))
            if variants(artist) == variants(t):
                interpretations.append((
                    artist_similarity(title, song.artist, aliases["artist"]),
                    title_similarity(artist, t, aliases["title"]),
                ))
            plausible = [
                (a, title_score) for a, title_score in interpretations
                if a >= 0.68 and title_score >= 0.66
            ]
            if not plausible:
                continue
            artist_score, title_score = max(
                plausible, key=lambda pair: (sum(pair), pair[1])
            )
            yield (
                (artist_score + title_score) / 2,
                artist_score, title_score, song,
            )

    return heapq.nlargest(limit, scored(), key=lambda row: (row[0], -row[3].pk))


def automatic_candidate(ranked, artist, title):
    artist, title = normalise(artist), normalise(title)
    if artist == "unknown artist":
        title = strip_vote_prefix(title)
    else:
        artist = strip_vote_prefix(artist)
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
            t >= 0.88 and min(len(normalise(title)), len(normalise(song.title))) >= 10
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
        and min(len(normalise(title)), len(normalise(song.title))) >= 3
        and max(len(normalise(title)), len(normalise(song.title))) <= 8
    ):
        return song
    # Exact short title plus a near-exact artist is strong evidence too.
    # This also covers an unambiguous title-only vote after AI confirmation.
    if t == 1.0 and a >= 0.90:
        return song
    if min(len(normalise(title)), len(normalise(song.title))) < 6:
        return None
    return song if a >= 0.90 and t >= 0.90 and score >= 0.94 else None
