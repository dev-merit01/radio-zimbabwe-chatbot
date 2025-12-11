"""
Celery tasks for Radio Zimbabwe chart computation.

This task computes daily charts from vote tallies.
It supports both raw (unverified) and cleaned (verified) song data.
"""
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone
from celery import shared_task

from apps.voting.models import (
    RawSongTally,
    CleanedSong,
    CleanedSongTally,
    MatchKeyMapping,
)


@shared_task
def compute_daily_chart(date_str: str = None):
    """
    Compute daily chart from vote tallies.
    
    This task:
    1. Aggregates CleanedSongTally for verified songs
    2. Falls back to RawSongTally for songs not yet cleaned
    3. Can be run manually or scheduled via Celery Beat
    
    Args:
        date_str: Optional date in 'YYYY-MM-DD' format. Defaults to today.
    
    Usage:
        # From Django shell:
        from apps.charts.tasks import compute_daily_chart
        compute_daily_chart.delay()  # Today
        compute_daily_chart.delay('2025-12-09')  # Specific date
    """
    from datetime import datetime
    
    if date_str:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    else:
        target_date = timezone.localdate()
    
    # Update CleanedSongTally from RawSongTally via MatchKeyMapping
    _sync_cleaned_tallies(target_date)
    
    return {
        'date': str(target_date),
        'status': 'completed',
    }


def _sync_cleaned_tallies(target_date):
    """
    Sync CleanedSongTally from RawSongTally data.
    
    For each raw tally, if there's a MatchKeyMapping to a CleanedSong,
    aggregate the votes into CleanedSongTally.
    """
    with transaction.atomic():
        # Get all raw tallies for the date
        raw_tallies = RawSongTally.objects.filter(date=target_date)
        
        # Group by cleaned_song via mappings
        cleaned_counts = {}
        
        for raw_tally in raw_tallies:
            mapping = MatchKeyMapping.objects.filter(
                match_key=raw_tally.match_key
            ).select_related('cleaned_song').first()
            
            if mapping and mapping.cleaned_song.status == 'verified':
                song_id = mapping.cleaned_song_id
                if song_id not in cleaned_counts:
                    cleaned_counts[song_id] = 0
                cleaned_counts[song_id] += raw_tally.count
        
        # Update or create CleanedSongTally entries
        for song_id, count in cleaned_counts.items():
            CleanedSongTally.objects.update_or_create(
                date=target_date,
                cleaned_song_id=song_id,
                defaults={'count': count}
            )


@shared_task
def compute_weekly_chart():
    """
    Compute weekly chart (aggregates last 7 days).
    Can be scheduled to run weekly via Celery Beat.
    """
    from datetime import timedelta
    
    today = timezone.localdate()
    week_start = today - timedelta(days=6)
    
    # Aggregate verified songs over the week
    weekly_totals = (
        CleanedSongTally.objects
        .filter(date__gte=week_start, date__lte=today)
        .filter(cleaned_song__status='verified')
        .values('cleaned_song')
        .annotate(total=Sum('count'))
        .order_by('-total')[:100]
    )
    
    return {
        'week_start': str(week_start),
        'week_end': str(today),
        'top_songs_count': len(weekly_totals),
    }
