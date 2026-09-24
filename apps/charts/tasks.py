"""Station-scoped compatibility tasks using the authoritative v2 reconciler."""

from datetime import date, timedelta
from celery import shared_task
from django.db.models import Sum
from django.utils import timezone
from apps.accounts.models import Station
from apps.voting.models import CleanedSongTally
from apps.voting.pipeline import rebuild_tallies


def validate_station(station):
    if station not in Station.values:
        raise ValueError("An explicit valid station is required.")


@shared_task
def compute_daily_chart(date_str=None, station=None):
    validate_station(station)
    day = date.fromisoformat(date_str) if date_str else timezone.localdate()
    rebuild_tallies(station, date_range=(day, day))
    return {"date": str(day), "station": station, "status": "completed"}


@shared_task
def compute_weekly_chart(station=None):
    validate_station(station)
    today = timezone.localdate()
    start = today - timedelta(days=today.weekday())
    totals = (
        CleanedSongTally.objects.filter(
            station=station,
            cleaned_song__station=station,
            cleaned_song__status="verified",
            date__range=(start, today),
        )
        .values("cleaned_song")
        .annotate(total=Sum("count"))
    )
    return {
        "week_start": str(start),
        "week_end": str(today),
        "station": station,
        "top_songs_count": totals.count(),
    }
