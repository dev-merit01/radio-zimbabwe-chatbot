"""Auditable chart-position comparisons for broadcast countdowns.

Positions are compared only against the immediately preceding *published*
edition of the SAME station, permanent chart-show ID and chart type.

A missing previous edition means *no comparison*, not "new". A song absent
from last week's chart is NEW only if it has never charted in that programme;
otherwise it is a RE-ENTRY. Live positions are provisional until published.
"""
from datetime import timedelta

from apps.voting.models import WeeklyChart, WeeklyChartEntry

STATES = frozenset(("up", "down", "same", "new", "re_entry", "no_previous"))


def previous_published_chart(show, start, end, *, annual=False):
    """Return exactly the preceding week/annual edition, or None.

    Skipped weeks never compare to a non-adjacent edition. Even if a
    neighbouring programme has a published chart, it is never consulted.
    """
    qs = WeeklyChart.objects.filter(
        station=show.station, chart_show=show,
        is_finalized=True, is_year_end=annual,
    )
    if annual:
        return qs.filter(year=end.year - 1).order_by("-week_end", "-pk").first()
    return qs.filter(
        week_end=start - timedelta(days=1),
    ).order_by("-week_end", "-pk").first()


def published_history_ids(show, start, *, annual=False, song_ids=None):
    """IDs which charted on THIS programme in an earlier official edition."""
    qs = WeeklyChartEntry.objects.filter(
        chart__station=show.station, chart__chart_show=show,
        chart__is_finalized=True, chart__is_year_end=annual,
        chart__week_end__lt=start,
        cleaned_song_id__isnull=False,
    )
    if song_ids is not None:
        qs = qs.filter(cleaned_song_id__in=song_ids)
    return set(qs.values_list("cleaned_song_id", flat=True))


def movement_for_rank(rank, previous_rank, *, has_comparison, charted_before):
    """Pure state calculator usable by live charts and frozen publications."""
    if previous_rank is not None:
        delta = previous_rank - rank
        status = "up" if delta > 0 else "down" if delta < 0 else "same"
        label = f"+{delta}" if delta > 0 else str(delta) if delta < 0 else "="
    elif not has_comparison:
        status, delta, label = "no_previous", None, "—"
    elif charted_before:
        status, delta, label = "re_entry", None, "re-entry"
    else:
        status, delta, label = "new", None, "new"
    return {
        "previous_rank": previous_rank,
        "movement_status": status,
        "movement_delta": delta,
        "movement": label,
    }


def archived_movement(entry, *, has_comparison, charted_before):
    """Legacy archive fallback; newly published charts retain saved status."""
    info = movement_for_rank(
        entry.rank, entry.previous_rank,
        has_comparison=has_comparison, charted_before=charted_before,
    )
    if entry.movement_status in STATES and entry.previous_rank is None:
        # Preserve the publisher's original judgement even when later
        # historical editions are imported or backfilled.
        status = entry.movement_status
        info["movement_status"] = status
        info["movement"] = {
            "new": "new", "re_entry": "re-entry", "no_previous": "—",
        }.get(status, info["movement"])
    return info


def comparison_metadata(chart, *, annual=False):
    if chart is None:
        return {
            "available": False, "chart_id": None, "chart_date": None,
            "label": "Previous annual chart" if annual else "Previous weekly chart",
        }
    return {
        "available": True,
        "chart_id": chart.pk,
        "chart_date": str(chart.chart_date or chart.week_end),
        "label": "Previous annual chart" if annual else "Previous weekly chart",
    }
