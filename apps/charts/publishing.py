from datetime import timedelta
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
    end = start + timedelta(days=6)
    year, week, _ = start.isocalendar()
    with transaction.atomic():
        lock_station(station)
        existing = WeeklyChart.objects.filter(
            station=station, year=year, week_number=week
        ).first()
        if existing:
            if existing.is_finalized:
                return existing, False
            raise ValueError(
                "An unfinished legacy archive exists for this week; review it before publishing."
            )
        unfinished = ['queued', 'running', 'failed']
        if InboundEvent.objects.filter(station=station, received_at__date__range=(start,end), state__in=unfinished).exists() or VoteJob.objects.filter(vote__station=station, vote__vote_date__range=(start,end), state__in=unfinished).exists():
            raise ValueError('This week still has unprocessed votes. Resolve the processing queue before publishing.')
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
        totals = list(songs)
        if not totals:
            raise ValueError("No verified votes exist for that week.")
        previous = WeeklyChart.objects.filter(
            station=station, week_start=start - timedelta(days=7), is_finalized=True
        ).first()
        previous_entries = (
            {e.cleaned_song_id: e for e in previous.entries.all()} if previous else {}
        )
        chart = WeeklyChart.objects.create(
            station=station,
            week_start=start,
            week_end=end,
            year=year,
            week_number=week,
            chart_size=size,
            total_votes=sum(s.n for s in totals),
            unique_songs=len(totals),
            is_finalized=True,
            finalized_at=timezone.now(),
        )
        historical_peaks = dict(
            WeeklyChartEntry.objects.filter(
                chart__station=station, chart__is_finalized=True
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
            details={"chart_id": chart.id, "week_start": str(start)},
        )
        return chart, True
