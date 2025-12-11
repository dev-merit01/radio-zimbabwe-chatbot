from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.db.models import Sum
from apps.voting.models import RawSongTally, CleanedSongTally, CleanedSong


def chart_today(request):
    """API endpoint returning the Top 100 as JSON (all-time cumulative votes)."""
    today = timezone.localdate()
    
    # Get all-time cumulative votes for cleaned/verified songs
    cleaned_songs = (
        CleanedSong.objects
        .filter(status='verified')
        .annotate(total_votes=Sum('cleanedsongtally__count'))
        .filter(total_votes__gt=0)
        .order_by('-total_votes')[:100]
    )
    
    if cleaned_songs.exists():
        # Use cleaned data with cumulative votes
        data = []
        for rank, song in enumerate(cleaned_songs, start=1):
            data.append({
                'rank': rank,
                'title': song.title,
                'artists': song.artist,
                'display_name': song.canonical_name,
                'album': song.album or '',
                'image_url': song.image_url or '',
                'spotify_track_id': song.spotify_track_id or '',
                'count': song.total_votes or 0,
                'is_verified': True,
            })
    else:
        # Fallback to raw data (all-time cumulative)
        raw_tallies = (
            RawSongTally.objects
            .values('match_key', 'display_name')
            .annotate(total_votes=Sum('count'))
            .order_by('-total_votes')[:100]
        )
        
        data = []
        for rank, tally in enumerate(raw_tallies, start=1):
            # Parse display_name back into artist and song
            parts = tally['display_name'].split(' - ', 1)
            if len(parts) == 2:
                artist, song = parts
            else:
                artist = "Unknown"
                song = tally['display_name']
            
            data.append({
                'rank': rank,
                'title': song,
                'artists': artist,
                'display_name': tally['display_name'],
                'match_key': tally['match_key'],
                'count': tally['total_votes'],
                'is_verified': False,
            })
    
    return JsonResponse({
        'date': str(today),
        'updated_at': timezone.now().isoformat(),
        'total_songs': len(data),
        'chart_type': 'all_time',
        'top100': data,
    })


def dashboard(request):
    """Render the Top 100 dashboard HTML page."""
    return render(request, 'dashboard/top100.html', {
        'page_title': 'Radio Zimbabwe Top 100',
    })
