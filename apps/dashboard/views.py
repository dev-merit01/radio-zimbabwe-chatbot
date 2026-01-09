from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.db.models import Sum, Q
from django.contrib.auth.decorators import login_required
from datetime import timedelta, date
from apps.voting.models import RawSongTally, CleanedSongTally, CleanedSong, WeeklyChart, WeeklyChartEntry
from apps.accounts.context_processors import get_active_station


def get_current_week_dates():
    """Get the start (Monday) and end (Sunday) of the current week."""
    today = timezone.localdate()
    # Monday is 0, Sunday is 6
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def get_week_dates_for_date(target_date):
    """Get the start (Monday) and end (Sunday) for a given date's week."""
    monday = target_date - timedelta(days=target_date.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


@login_required
def chart_today(request):
    """API endpoint returning the current week's chart (expandable to 50)."""
    today = timezone.localdate()
    week_start, week_end = get_current_week_dates()
    station = get_active_station(request)
    
    # Check for limit parameter (default 20, max 50)
    limit = min(int(request.GET.get('limit', 20)), 50)
    
    # Get previous week's chart for movement calculation
    prev_week_start = week_start - timedelta(days=7)
    prev_week_end = week_end - timedelta(days=7)
    
    # Build previous week's rankings (station-scoped)
    prev_rankings = {}
    prev_chart = WeeklyChart.objects.filter(
        station=station,
        week_start=prev_week_start,
        is_finalized=True
    ).first()
    
    if prev_chart:
        for entry in prev_chart.entries.all():
            if entry.cleaned_song_id:
                prev_rankings[entry.cleaned_song_id] = entry.rank
    
    # Get votes for the current week only (station-scoped)
    cleaned_songs = (
        CleanedSong.objects
        .filter(station=station, status='verified')
        .annotate(
            week_votes=Sum(
                'cleanedsongtally__count',
                filter=Q(
                    cleanedsongtally__station=station,
                    cleanedsongtally__date__gte=week_start,
                    cleanedsongtally__date__lte=week_end
                )
            )
        )
        .filter(week_votes__gt=0)
        .order_by('-week_votes')[:limit]
    )
    
    # Only show verified/cleaned songs on the dashboard
    data = []
    for rank, song in enumerate(cleaned_songs, start=1):
        prev_rank = prev_rankings.get(song.id)
        if prev_rank:
            diff = prev_rank - rank
            if diff > 0:
                movement = f'+{diff}'
            elif diff < 0:
                movement = str(diff)
            else:
                movement = '='
        else:
            movement = 'new'
        
        data.append({
            'rank': rank,
            'title': song.title,
            'artists': song.artist,
            'display_name': song.canonical_name,
            'album': song.album or '',
            'image_url': song.image_url or '',
            'spotify_track_id': song.spotify_track_id or '',
            'count': song.week_votes or 0,
            'previous_rank': prev_rank,
            'movement': movement,
            'is_verified': True,
        })
    
    # No fallback to raw data - only verified songs appear on dashboard
    # If no verified songs, data will be empty and dashboard shows "No votes yet"
    
    # Calculate total votes for the week (station-scoped)
    total_week_votes = CleanedSongTally.objects.filter(
        station=station,
        date__gte=week_start, date__lte=week_end
    ).aggregate(total=Sum('count'))['total'] or 0
    
    return JsonResponse({
        'date': str(today),
        'week_start': str(week_start),
        'week_end': str(week_end),
        'week_number': today.isocalendar()[1],
        'year': today.year,
        'updated_at': timezone.now().isoformat(),
        'total_songs': len(data),
        'total_votes': total_week_votes,
        'chart_type': 'weekly',
        'limit': limit,
        'top100': data,  # Keep key for backward compatibility
    })


@login_required
def chart_archives(request):
    """API endpoint returning list of all archived weekly charts."""
    year = request.GET.get('year', timezone.localdate().year)
    station = get_active_station(request)
    
    charts = WeeklyChart.objects.filter(
        station=station,
        year=year,
        is_finalized=True
    ).order_by('-week_number')
    
    data = []
    for chart in charts:
        data.append({
            'id': chart.id,
            'week_number': chart.week_number,
            'week_start': str(chart.week_start),
            'week_end': str(chart.week_end),
            'year': chart.year,
            'is_year_end': chart.is_year_end,
            'chart_size': chart.chart_size,
            'total_votes': chart.total_votes,
            'unique_songs': chart.unique_songs,
            'finalized_at': chart.finalized_at.isoformat() if chart.finalized_at else None,
        })
    
    # Get available years (station-scoped)
    available_years = WeeklyChart.objects.filter(
        station=station,
        is_finalized=True
    ).values_list('year', flat=True).distinct().order_by('-year')
    
    return JsonResponse({
        'year': int(year),
        'available_years': list(available_years),
        'charts': data,
    })


@login_required
def chart_detail(request, chart_id):
    """API endpoint returning a specific archived chart's entries."""
    try:
        chart = WeeklyChart.objects.get(id=chart_id)
    except WeeklyChart.DoesNotExist:
        return JsonResponse({'error': 'Chart not found'}, status=404)
    
    entries = chart.entries.all().order_by('rank')
    
    data = []
    for entry in entries:
        data.append({
            'rank': entry.rank,
            'title': entry.title,
            'artists': entry.artist,
            'display_name': entry.canonical_name,
            'album': entry.album,
            'image_url': entry.image_url,
            'spotify_track_id': entry.spotify_track_id,
            'count': entry.vote_count,
            'previous_rank': entry.previous_rank,
            'weeks_on_chart': entry.weeks_on_chart,
            'peak_rank': entry.peak_rank,
            'movement': entry.movement,
        })
    
    return JsonResponse({
        'chart': {
            'id': chart.id,
            'week_number': chart.week_number,
            'week_start': str(chart.week_start),
            'week_end': str(chart.week_end),
            'year': chart.year,
            'is_year_end': chart.is_year_end,
            'chart_size': chart.chart_size,
            'total_votes': chart.total_votes,
            'finalized_at': chart.finalized_at.isoformat() if chart.finalized_at else None,
        },
        'entries': data,
    })


@login_required
def stats_overview(request):
    """API endpoint returning overall statistics."""
    today = timezone.localdate()
    year = today.year
    week_start, week_end = get_current_week_dates()
    station = get_active_station(request)
    
    # This week stats (station-scoped)
    week_votes = CleanedSongTally.objects.filter(
        station=station,
        date__gte=week_start, date__lte=week_end
    ).aggregate(total=Sum('count'))['total'] or 0
    
    week_songs = CleanedSongTally.objects.filter(
        station=station,
        date__gte=week_start, date__lte=week_end
    ).values('cleaned_song').distinct().count()
    
    # Year-to-date stats (station-scoped)
    year_start = date(year, 1, 1)
    ytd_votes = CleanedSongTally.objects.filter(
        station=station,
        date__gte=year_start
    ).aggregate(total=Sum('count'))['total'] or 0
    
    # Total charts finalized (station-scoped)
    total_charts = WeeklyChart.objects.filter(station=station, year=year, is_finalized=True).count()
    
    # Next Saturday (chart day)
    days_until_saturday = (5 - today.weekday()) % 7
    if days_until_saturday == 0 and timezone.localtime().hour >= 18:
        days_until_saturday = 7
    next_chart_date = today + timedelta(days=days_until_saturday)
    
    # Check if Dec 31 is coming (year-end special)
    dec_31 = date(year, 12, 31)
    is_year_end_week = week_start <= dec_31 <= week_end
    
    return JsonResponse({
        'week': {
            'number': today.isocalendar()[1],
            'start': str(week_start),
            'end': str(week_end),
            'votes': week_votes,
            'songs': week_songs,
        },
        'year': {
            'value': year,
            'total_votes': ytd_votes,
            'charts_published': total_charts,
        },
        'next_chart': {
            'date': str(next_chart_date),
            'days_until': days_until_saturday,
            'is_year_end': is_year_end_week,
        },
        'updated_at': timezone.now().isoformat(),
    })


@login_required
def dashboard(request):
    """Render the Top 100 dashboard HTML page."""
    return render(request, 'dashboard/dashboard.html', {
        'page_title': 'Radio Zimbabwe Charts',
    })
