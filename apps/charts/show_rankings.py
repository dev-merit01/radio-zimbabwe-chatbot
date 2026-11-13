"""Show-specific counts from individually approved votes, not station-wide tallies."""
from collections import Counter

from django.db.models import Count, OuterRef, Subquery

from apps.voting.models import (
    CleanedSong, EditionCandidate, EligibleEdition, MatchKeyMapping, RawVote,
)


def counts_for_show(show, start, end):
    """Verified song totals, deduplicated per listener/day/song within a show.

    Match keys remain station-level so the same OpenAI-reviewed aliases work
    across programmes, but only RawVote rows linked to this show are counted.
    """
    if show.kind == "legacy":
        from django.db.models import Sum
        from apps.voting.models import CleanedSongTally
        return dict(
            CleanedSongTally.objects.filter(
                station=show.station, date__range=(start, end),
                cleaned_song__status="verified",
            ).values("cleaned_song_id").annotate(n=Sum("count"))
            .values_list("cleaned_song_id", "n")
        )
    eligible_song_ids = None
    if show.eligibility_enforced:
        # Only a presenter-published shortlist for this exact programme
        # period can contribute to public rankings. Unlisted requests remain
        # in raw records as nomination interest, never as chart votes.
        edition = EligibleEdition.objects.filter(
            station=show.station, chart_show=show, status="locked",
            voting_start__lte=start, voting_end__gte=end,
        ).first()
        if edition is None:
            return {}
        eligible_song_ids = EditionCandidate.objects.filter(
            edition=edition
        ).values("song_id")
    mapping = MatchKeyMapping.objects.filter(
        station=show.station, match_key=OuterRef("match_key"),
        cleaned_song__station=show.station,
        cleaned_song__status="verified",
    ).order_by()
    votes = RawVote.objects.filter(
        chart_show=show, station=show.station,
        vote_date__range=(start, end),
        ai_status__in=("approved", "legacy"),
    ).annotate(song_id=Subquery(mapping.values("cleaned_song_id")[:1]))
    votes = votes.filter(song_id__isnull=False)
    if eligible_song_ids is not None:
        votes = votes.filter(song_id__in=eligible_song_ids)
    from apps.voting.station_vote_policy import voting_rules_for_station
    if voting_rules_for_station(show.station)["allow_repeat_song"]:
        rows = votes.order_by().values("song_id").annotate(n=Count("id"))
    else:
        # A typo can arrive before the first AI job completes. Defend against
        # equivalent aliases from the same voter counting twice in one day.
        rows = votes.order_by().values("song_id", "vote_date").annotate(
            n=Count("user_id", distinct=True)
        )
    result = Counter()
    for row in rows.iterator(chunk_size=500):
        result[row["song_id"]] += row["n"]
    return dict(result)


def ranked_songs(show, start, end, limit=50):
    counts = counts_for_show(show, start, end)
    songs = CleanedSong.objects.filter(
        pk__in=counts.keys(), station=show.station, status="verified"
    )
    return sorted(
        ((song, counts[song.pk]) for song in songs if counts.get(song.pk)),
        key=lambda row: (-row[1], row[0].canonical_name.casefold(), row[0].pk),
    )[:limit]
