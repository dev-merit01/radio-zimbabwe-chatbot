"""Conservative, station-scoped candidate retrieval for the live worker.

Scores are string similarities, not probabilities. Short/ambiguous titles and
recording variants require review. No catalogue mutation or external I/O here.
"""

import heapq
import re
import unicodedata
from collections import defaultdict
from difflib import SequenceMatcher

from django.conf import settings

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


def distinctive_title_prefix(submitted, canonical):
    """Recognise a long, exact leading phrase as a possible shortened title.

    The artist may be omitted, but we must *never* approve solely because
    a short common word starts a title. A unique long prefix still needs an
    OpenAI confirmation and a full-catalogue ambiguity check.
    """
    submitted, canonical = normalise(submitted), normalise(canonical)
    incoming, official = submitted.split(), canonical.split()
    return (
        len(submitted) >= 10
        and len(incoming) < len(official)
        and incoming == official[:len(incoming)]
        and len(incoming[0]) >= 6
        and (len(incoming) > 1 or len(incoming[0]) >= 10)
    )


def distinctive_single_word_typo(submitted, canonical):
    """Retrieve long one-word title typos with at most two character edits.

    A distinctive single-word title can survive phonetic transcriptions such
    as "Kwadabuka" for "Kukwadabuka". Both must be long one-word titles.
    This is strictly candidate evidence, never an approval without OpenAI,
    a UNIQUE station catalogue match and version checks.
    """
    submitted, canonical = normalise(submitted), normalise(canonical)
    if (
        len(submitted.split()) != 1 or len(canonical.split()) != 1
        or min(len(submitted), len(canonical)) < 9
        or abs(len(submitted) - len(canonical)) > 2
        or text_similarity(submitted, canonical) < 0.89
    ):
        return False

    # Early-terminated Levenshtein with a tiny edit budget; no dependency
    # and no unbounded work even for maliciously long messages.
    prior = list(range(len(canonical) + 1))
    for i, source in enumerate(submitted, 1):
        current = [i]
        for j, target in enumerate(canonical, 1):
            current.append(min(
                prior[j] + 1,
                current[j - 1] + 1,
                prior[j - 1] + (source != target),
            ))
        if min(current) > 2:
            return False
        prior = current
    return prior[-1] <= 2


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


def shortened_artist_suffix(submitted, canonical):
    """Return a conservative similarity hint for clipped lead stage names.

    Radio listeners often omit the first syllables of a performer, e.g.
    "Ngende" for "Hulengende". A *single*, distinctive final substring
    is only evidence for shortlist retrieval, never proof on its own.
    The title and competing verified songs must still pass independently.
    """
    submitted = normalise(submitted)
    lead, _featured = artist_credits(canonical)
    # Do not trim multiword names, short generic syllables or the beginning
    # of a long artist string; separate leading-word aliases already exist.
    return (
        len(submitted.split()) == 1
        and len(lead.split()) == 1
        and len(submitted) >= 5
        and len(lead) - len(submitted) >= 2
        and len(submitted) / max(len(lead), 1) >= 0.55
        and lead.endswith(submitted)
    )


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
    # A listener may give a well-known leading stage-name token without the
    # artist's second word: "Atenda My Man" vs "Atenda Chinx - My Man".
    # Retrieval keeps ALL same-prefix competing artists; a solo artist word
    # never identifies a song by itself.
    leading = primary.split()
    if len(submitted.split()) == 1 and len(submitted) >= 5 and (
        len(leading) >= 2 and submitted == leading[0]
    ):
        alternatives.add(submitted)
    if len(words) >= 2 and words[0] in {"jnr", "jr", "junior", "dj", "mc"}:
        alternatives.add(" ".join(words[1:]))
    # These are branding suffixes/initialisms, not musical title words.
    # Do not strip arbitrary artist tokens: e.g. "Winky D" must stay whole.
    if len(words) >= 2 and words[-1] in {"hkd", "official", "music"}:
        alternatives.add(" ".join(words[:-1]))

    scores = [
        max(
            SequenceMatcher(None, submitted, alternative, autojunk=False).ratio(),
            SequenceMatcher(
                None, phonetic_spelling(submitted),
                phonetic_spelling(alternative), autojunk=False,
            ).ratio(),
        )
        for alternative in alternatives
    ]
    if shortened_artist_suffix(submitted, canonical):
        scores.append(0.93)
    return max(scores)


def ranked_candidates(station, artist, title, limit=16, *, exhaustive=False):
    from .matching_controls import options_for_station
    options = options_for_station(station)
    min_artist = options["retrieval_min_artist"]
    min_title = options["retrieval_min_title"]
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

    def scored(songs):
        for song in songs.iterator(chunk_size=500):
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
                    for aa, tt in scores if aa >= min_artist and tt >= min_title
                ]
                only_title_score = title_similarity(title, t, aliases["title"])
                title_only = (
                    title == t or title in aliases["title"]
                    or distinctive_title_prefix(title, t)
                    or distinctive_single_word_typo(title, t)
                    or (
                    min(len(title), len(t)) >= 10
                    and len(title.split()) == len(t.split())
                    and len(t.split()) >= 2
                    and only_title_score >= 0.88
                    )
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
                if a >= min_artist and title_score >= min_title
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

    all_songs = CleanedSong.objects.filter(
        station=station, status="verified",
    ).only("id", "artist", "title", "canonical_name")
    filtered = False
    if exhaustive:
        searched = all_songs
    else:
        from .indexed_retrieval import narrowed_catalogue

        searched, filtered = narrowed_catalogue(station, artist, title)
    ranked = heapq.nlargest(
        limit, scored(searched), key=lambda row: (row[0], -row[3].pk)
    )
    # Preserve recall for severely misspelt votes when the indexed pass
    # found nothing. Final fuzzy approval also uses an exhaustive ambiguity
    # check in pipeline.py, so a missing indexed competitor cannot count.
    if filtered and (not ranked or ranked[0][0] < 0.84):
        # A weak indexed shortlist may omit the true song when every word
        # contains errors. Recover full-catalogue recall in those rare cases;
        # never pretend weak indexed matches are decisive.
        ranked = heapq.nlargest(
            limit, scored(all_songs), key=lambda row: (row[0], -row[3].pk)
        )

    if not exhaustive and getattr(settings, "AI_VECTOR_ENABLED", False) and (
        not ranked or ranked[0][0] < 0.90
    ):
        from .semantic_matching import semantic_candidates

        extra = semantic_candidates(
            station, artist, title, limit=min(6, limit),
            exclude_ids={row[3].pk for row in ranked},
        )
        # Vector-only rows are retrieval hints, never proof of identity:
        # zero lexical scores make auto-matching fail until an alias or
        # independent lexical evidence justifies the selection.
        ranked += [(min(sim * 0.8, 0.72), 0.0, 0.0, song) for sim, song in extra]
        ranked.sort(key=lambda row: (row[0], -row[3].pk), reverse=True)
        ranked = ranked[:limit]
    return ranked


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
        # A long, exact title prefix such as "Ndakainamatira" can be an
        # unambiguous shorthand for "Ndakainamatira Nyaya Iyoyi", even
        # without an artist. The candidate shortlist includes *all* songs
        # sharing that prefix and the runner-up gap is checked above.
        if t >= 0.95 and distinctive_title_prefix(title, song.title):
            return song
        if t >= 0.89 and distinctive_single_word_typo(title, song.title):
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
    # A single recognisable title word plus a strongly identified lead
    # artist can be an unambiguous abbreviation: "Hulengende - Bhura" for
    # "Bhura Dhanzi". The runner-up gap above must still hold.
    short_words, full_words = normalise(title).split(), normalise(song.title).split()
    if (
        a >= 0.90 and t >= 0.95
        and len(short_words) == 1 and len(full_words) > 1
        and len(short_words[0]) >= 4
        and full_words[0].startswith(short_words[0])
    ):
        return song
    if min(len(normalise(title)), len(normalise(song.title))) < 6:
        return None
    return song if a >= 0.90 and t >= 0.90 and score >= 0.94 else None
