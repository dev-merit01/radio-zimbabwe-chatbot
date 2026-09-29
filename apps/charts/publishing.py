from datetime import date, timedelta
from django.db import transaction
from django.db.models import Sum, Min
from django.utils import timezone
from apps.voting.models import CleanedSong, WeeklyChart, WeeklyChartEntry, ReviewAudit
from apps.voting.pipeline import lock_station, rebuild_tallies
from apps.voting.models import InboundEvent, VoteJob


def publish_week(station, start, actor, size=20):
    if start.weekday() != 0 or start + timedelta(days=6) >= timezone.localdate():
        raise ValueError(
            "Choose the Monday of a completed week. Live voting remains open for the current week."
        )
    if size not in (20, 50, 100):
        raise ValueError("Chart size must be 20, 50 or 100.")
    # Compatibility for old command callers; new UI uses Saturday editions.
    return _publish(station, start, start + timedelta(days=6), actor, size)


def publish_edition(station, chart_date, actor, size=20, kind="weekly"):
    if size not in (20, 50):
        raise ValueError("Choose Top 20 or Top 50.")
    if chart_date > timezone.localdate():
        raise ValueError("Cannot save a chart for a future date.")
    if kind == "weekly":
        if chart_date.weekday() != 5:
            raise ValueError("Choose a Saturday for a weekly chart.")
        start = chart_date - timedelta(days=6)
    elif kind == "year_end":
        if chart_date.month != 12 or size != 50:
            raise ValueError(
                "The year-end chart must be a Top 50 with a December publication date."
            )
        start = date(chart_date.year, 1, 1)
    else:
        raise ValueError("Unknown chart type.")
    return _publish(
        station, start, chart_date, actor, size, chart_date, kind == "year_end"
    )


def _publish(station, start, end, actor, size, chart_date=None, year_end=False):
    year, week, _ = (chart_date or start).isocalendar()
    if year_end:
        year, week = end.year, 0
    with transaction.atomic():
        lock_station(station)
        existing_charts = WeeklyChart.objects.filter(
            station=station, year=year, is_year_end=year_end
        )
        if not year_end:
            existing_charts = existing_charts.filter(week_number=week)
        existing = existing_charts.first()
        if existing:
            if existing.is_finalized:
                if (
                    existing.week_start != start
                    or existing.week_end != end
                    or existing.chart_size != size
                ):
                    raise ValueError(
                        "An archive already exists for this edition with a different period or size. Saved charts cannot be overwritten."
                    )
                return existing, False
            raise ValueError(
                "An unfinished legacy archive exists for this week; review it before publishing."
            )
        unfinished = ["queued", "running", "failed"]
        if (
            InboundEvent.objects.filter(
                station=station,
                received_at__date__range=(start, end),
                state__in=unfinished,
            ).exists()
            or VoteJob.objects.filter(
                vote__station=station,
                vote__vote_date__range=(start, end),
                state__in=unfinished,
            ).exists()
        ):
            raise ValueError(
                "This period still has unprocessed votes. Resolve the processing queue before publishing."
            )
        rebuild_tallies(station, date_range=(start, end))
        songs = (
            CleanedSong.objects.filter(
                station=station,
                status="verified",
                cleanedsongtally__station=station,
                cleanedsongtally__date__range=(start, end),
            )
            .annotate(n=Sum("cleanedsongtally__count"))
            .filter(n__gt=0)
            .order_by("-n", "canonical_name", "id")
        )
        total_votes = songs.aggregate(total=Sum("n"))["total"] or 0
        unique_songs = songs.count()
        totals = list(songs[:size])
        if not totals:
            raise ValueError("No verified votes exist for that period.")
        previous = (
            WeeklyChart.objects.filter(
                station=station,
                week_end=end - timedelta(days=7),
                is_finalized=True,
                is_year_end=False,
            ).first()
            if not year_end
            else None
        )
        previous_entries = (
            {e.cleaned_song_id: e for e in previous.entries.all()} if previous else {}
        )
        chart = WeeklyChart.objects.create(
            station=station,
            week_start=start,
            week_end=end,
            chart_date=chart_date,
            is_year_end=year_end,
            year=year,
            week_number=week,
            chart_size=size,
            total_votes=total_votes,
            unique_songs=unique_songs,
            is_finalized=True,
            finalized_at=timezone.now(),
        )
        historical_peaks = dict(
            WeeklyChartEntry.objects.filter(
                chart__station=station,
                chart__is_finalized=True,
                chart__week_start__lt=start,
                chart__is_year_end=year_end,
            )
            .values("cleaned_song_id")
            .annotate(peak=Min("rank"))
            .values_list("cleaned_song_id", "peak")
        )
        entries = []
        for rank, song in enumerate(totals[:size], 1):
            old = previous_entries.get(song.id)
            peak = historical_peaks.get(song.id)
            entries.append(
                WeeklyChartEntry(
                    chart=chart,
                    rank=rank,
                    cleaned_song=song,
                    title=song.title,
                    artist=song.artist,
                    canonical_name=song.canonical_name,
                    vote_count=song.n,
                    previous_rank=old.rank if old else None,
                    weeks_on_chart=old.weeks_on_chart + 1 if old else 1,
                    peak_rank=min(rank, peak or rank),
                    spotify_track_id=song.spotify_track_id or "",
                    image_url=song.image_url,
                    album=song.album,
                )
            )
        WeeklyChartEntry.objects.bulk_create(entries)
        ReviewAudit.objects.create(
            station=station,
            actor=actor,
            action="publish",
            details={
                "chart_id": chart.id,
                "week_start": str(start),
                "end": str(end),
                "size": size,
                "kind": "year_end" if year_end else "weekly",
            },
        )
        return chart, True
