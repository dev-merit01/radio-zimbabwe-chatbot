from apps.voting.presentation import music_name
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.db.models import Sum, Q
from django.contrib.auth.decorators import login_required
from datetime import timedelta, date
from apps.voting.models import CleanedSongTally, CleanedSong, WeeklyChart
from apps.accounts.context_processors import get_active_station
from apps.charts.routing import get_selected_show
from apps.accounts.permissions import presenter_show
from apps.charts.show_rankings import counts_for_show, ranked_songs
from apps.charts.multiplier import station_multiplier, score_for_votes


def get_current_week_dates():
    from apps.charts.periods import week_dates
    return week_dates(timezone.localdate())


def get_week_dates_for_date(target_date):
    from apps.charts.periods import week_dates
    return week_dates(target_date)


def chart_today_for_show(show, limit):
    """Live Top 20/50 from this show only, with previous programme-specific ranks."""
    today = timezone.localdate()
    multiplier = station_multiplier(show.station)
    if show.kind == "year_end":
        start = show.campaign_start.date() if show.campaign_start else date(today.year, 1, 1)
        end = today
        kind = "year_end"
    else:
        end = today + timedelta(days=(show.broadcast_weekday - today.weekday()) % 7)
        start = end - timedelta(days=6)
        kind = "weekly"
    previous = (
        WeeklyChart.objects.filter(
            station=show.station, chart_show=show,
            week_start=start - timedelta(days=7),
            is_finalized=True, is_year_end=False,
        ).first() if kind == "weekly" else None
    )
    previous_ranks = (
        {row.cleaned_song_id: row.rank for row in previous.entries.all()}
        if previous else {}
    )
    ranked = ranked_songs(show, start, end, limit)
    data = []
    for rank, (song, count) in enumerate(ranked, 1):
        old = previous_ranks.get(song.pk)
        difference = old - rank if old else None
        movement = ("new" if difference is None else
                    "=" if difference == 0 else
                    f"+{difference}" if difference > 0 else str(difference))
        data.append({
            "rank": rank, "title": music_name(song.title),
            "artists": music_name(song.artist),
            "display_name": f"{music_name(song.artist)} - {music_name(song.title)}",
            "album": song.album or "", "image_url": song.image_url or "",
            "spotify_track_id": song.spotify_track_id or "",
            "count": count, "score": score_for_votes(count, multiplier), "previous_rank": old,
            "movement": movement, "is_verified": True,
        })
    totals = counts_for_show(show, start, end)
    return JsonResponse({
        "date": str(today), "week_start": str(start), "week_end": str(end),
        "week_number": end.isocalendar()[1], "year": end.year,
        "updated_at": timezone.now().isoformat(),
        "total_songs": len(data), "total_votes": sum(totals.values()),
        "total_score": score_for_votes(sum(totals.values()), multiplier),
        "vote_multiplier": multiplier,
        "chart_type": kind, "chart_show": show.name, "chart_show_id": show.pk,
        "limit": limit, "top100": data,
    })


@login_required
def chart_today(request):
    """API endpoint returning the current week's chart (expandable to 50)."""
    today = timezone.localdate()
    week_start, week_end = get_current_week_dates()
    station = get_active_station(request)
    multiplier = station_multiplier(station)
    try:
        show = get_selected_show(request)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)
    period = request.GET.get("period", "weekly")
    if period not in {"weekly", "year_end"}:
        return JsonResponse({"error": "Invalid chart period"}, status=400)
    if period == "year_end":
        week_start, week_end = date(today.year, 1, 1), today

    # Check for limit parameter (default 20, max 50)
    try:
        limit = max(1, min(int(request.GET.get("limit", 20)), 100))
    except ValueError:
        return JsonResponse({"error": "Invalid limit"}, status=400)

    if show.kind != "legacy":
        return chart_today_for_show(show, limit)

    # Get previous week's chart for movement calculation
    prev_week_start = week_start - timedelta(days=7)

    # Build previous week's rankings (station-scoped)
    prev_rankings = {}
    prev_chart = WeeklyChart.objects.filter(
        station=station, week_start=prev_week_start, is_finalized=True, is_year_end=False
    ).first()

    if prev_chart and period == "weekly":
        for entry in prev_chart.entries.all():
            if entry.cleaned_song_id:
                prev_rankings[entry.cleaned_song_id] = entry.rank

    # Get votes for the current week only (station-scoped)
    cleaned_songs = (
        CleanedSong.objects.filter(station=station, status="verified")
        .annotate(
            week_votes=Sum(
                "cleanedsongtally__count",
                filter=Q(
                    cleanedsongtally__station=station,
                    cleanedsongtally__date__gte=week_start,
                    cleanedsongtally__date__lte=week_end,
                ),
            )
        )
        .filter(week_votes__gt=0)
        .order_by("-week_votes", "canonical_name", "id")[:limit]
    )

    # Only show verified/cleaned songs on the dashboard
    data = []
    for rank, song in enumerate(cleaned_songs, start=1):
        prev_rank = prev_rankings.get(song.id)
        if prev_rank:
            diff = prev_rank - rank
            if diff > 0:
                movement = f"+{diff}"
            elif diff < 0:
                movement = str(diff)
            else:
                movement = "="
        else:
            movement = "new"

        data.append(
            {
                "rank": rank,
                "title": music_name(song.title),
                "artists": music_name(song.artist),
                "display_name": f"{music_name(song.artist)} - {music_name(song.title)}",
                "album": song.album or "",
                "image_url": song.image_url or "",
                "spotify_track_id": song.spotify_track_id or "",
                "count": song.week_votes or 0,
                "score": score_for_votes(song.week_votes or 0, multiplier),
                "previous_rank": prev_rank,
                "movement": movement,
                "is_verified": True,
            }
        )

    # No fallback to raw data - only verified songs appear on dashboard
    # If no verified songs, data will be empty and dashboard shows "No votes yet"

    # Calculate total votes for the week (station-scoped)
    total_week_votes = (
        CleanedSongTally.objects.filter(
            station=station,
            cleaned_song__status="verified",
            date__gte=week_start,
            date__lte=week_end,
        ).aggregate(total=Sum("count"))["total"]
        or 0
    )

    return JsonResponse(
        {
            "date": str(today),
            "week_start": str(week_start),
            "week_end": str(week_end),
            "week_number": week_end.isocalendar()[1],
            "year": today.year if period == "year_end" else week_end.isocalendar().year,
            "updated_at": timezone.now().isoformat(),
            "total_songs": len(data),
            "total_votes": total_week_votes,
            "total_score": score_for_votes(total_week_votes, multiplier),
            "vote_multiplier": multiplier,
            "chart_type": period,
            "limit": limit,
            "top100": data,  # Keep key for backward compatibility
        }
    )


@login_required
def chart_archives(request):
    """API endpoint returning list of all archived weekly charts."""
    try:
        year = int(request.GET.get("year", get_current_week_dates()[1].isocalendar().year))
        if not 1900 <= year <= 9999:
            raise ValueError()
    except ValueError:
        return JsonResponse({"error": "Invalid year"}, status=400)
    station = get_active_station(request)

    try:
        show = get_selected_show(request)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)
    charts = WeeklyChart.objects.filter(
        station=station, chart_show=show, year=year, is_finalized=True
    ).order_by("-week_end", "-is_year_end")

    data = []
    for chart in charts:
        data.append(
            {
                "id": chart.id,
                "week_number": chart.week_number,
                "week_start": str(chart.week_start),
                "week_end": str(chart.week_end),
                "year": chart.year,
                "is_year_end": chart.is_year_end,
                "chart_date": str(chart.chart_date) if chart.chart_date else None,
                "chart_size": chart.chart_size,
                "chart_show_id": chart.chart_show_id,
                "chart_show": chart.chart_show.name if chart.chart_show else "",
                "total_votes": chart.total_votes,
                "total_score": score_for_votes(chart.total_votes, chart.vote_multiplier),
                "vote_multiplier": chart.vote_multiplier,
                "unique_songs": chart.unique_songs,
                "finalized_at": chart.finalized_at.isoformat()
                if chart.finalized_at
                else None,
            }
        )

    # Get available years (station-scoped)
    available_years = (
        WeeklyChart.objects.filter(station=station, chart_show=show, is_finalized=True)
        .values_list("year", flat=True)
        .distinct()
        .order_by("-year")
    )

    return JsonResponse(
        {
            "year": int(year),
            "available_years": list(available_years),
            "charts": data,
        }
    )


@login_required
def chart_detail(request, chart_id):
    """API endpoint returning a specific archived chart's entries."""
    try:
        chart = WeeklyChart.objects.get(
            id=chart_id, station=get_active_station(request), is_finalized=True
        )
    except WeeklyChart.DoesNotExist:
        return JsonResponse({"error": "Chart not found"}, status=404)

    assigned = presenter_show(request.user)
    if assigned and chart.chart_show_id != assigned.pk:
        return JsonResponse({"error": "Chart not found"}, status=404)
    entries = chart.entries.all().order_by("rank")

    data = []
    for entry in entries:
        data.append(
            {
                "rank": entry.rank,
                "title": music_name(entry.title),
                "artists": music_name(entry.artist),
                "display_name": f"{music_name(entry.artist)} - {music_name(entry.title)}",
                "album": entry.album,
                "image_url": entry.image_url,
                "spotify_track_id": entry.spotify_track_id,
                "count": entry.vote_count,
                "score": score_for_votes(entry.vote_count, chart.vote_multiplier),
                "previous_rank": entry.previous_rank,
                "weeks_on_chart": entry.weeks_on_chart,
                "peak_rank": entry.peak_rank,
                "movement": entry.movement,
            }
        )

    return JsonResponse(
        {
            "chart": {
                "id": chart.id,
                "week_number": chart.week_number,
                "week_start": str(chart.week_start),
                "week_end": str(chart.week_end),
                "year": chart.year,
                "is_year_end": chart.is_year_end,
                "chart_date": str(chart.chart_date) if chart.chart_date else None,
                "chart_size": chart.chart_size,
                "total_votes": chart.total_votes,
                "total_score": score_for_votes(chart.total_votes, chart.vote_multiplier),
                "vote_multiplier": chart.vote_multiplier,
                "finalized_at": chart.finalized_at.isoformat()
                if chart.finalized_at
                else None,
            },
            "entries": data,
        }
    )


@login_required
def stats_overview(request):
    """API endpoint returning overall statistics."""
    if presenter_show(request.user):
        # Global station statistics contain other programmes; a presenter
        # must use the assigned-show overview endpoint instead.
        return JsonResponse(
            {"error": "Use your assigned programme overview."}, status=403,
        )
    today = timezone.localdate()
    year = today.year
    week_start, week_end = get_current_week_dates()
    station = get_active_station(request)

    # This week stats (station-scoped)
    week_votes = (
        CleanedSongTally.objects.filter(
            station=station,
            cleaned_song__status="verified",
            date__gte=week_start,
            date__lte=week_end,
        ).aggregate(total=Sum("count"))["total"]
        or 0
    )

    week_songs = (
        CleanedSongTally.objects.filter(
            station=station,
            cleaned_song__status="verified",
            date__gte=week_start,
            date__lte=week_end,
        )
        .values("cleaned_song")
        .distinct()
        .count()
    )

    # Year-to-date stats (station-scoped)
    year_start = date(year, 1, 1)
    ytd_votes = (
        CleanedSongTally.objects.filter(
            station=station, cleaned_song__status="verified", date__gte=year_start
        ).aggregate(total=Sum("count"))["total"]
        or 0
    )

    # Total charts finalized (station-scoped)
    total_charts = WeeklyChart.objects.filter(
        station=station, year=year, is_finalized=True
    ).count()

    # Next Saturday (chart day)
    days_until_saturday = (5 - today.weekday()) % 7
    if days_until_saturday == 0 and timezone.localtime().hour >= 18:
        days_until_saturday = 7
    next_chart_date = today + timedelta(days=days_until_saturday)

    # Check if Dec 31 is coming (year-end special)
    dec_31 = date(year, 12, 31)
    is_year_end_week = week_start <= dec_31 <= week_end

    return JsonResponse(
        {
            "week": {
                "number": today.isocalendar()[1],
                "start": str(week_start),
                "end": str(week_end),
                "votes": week_votes,
                "songs": week_songs,
            },
            "year": {
                "value": year,
                "total_votes": ytd_votes,
                "charts_published": total_charts,
            },
            "next_chart": {
                "date": str(next_chart_date),
                "days_until": days_until_saturday,
                "is_year_end": is_year_end_week,
            },
            "updated_at": timezone.now().isoformat(),
        }
    )


@login_required
def dashboard(request):
    """Render the Top 100 dashboard HTML page."""
    return render(
        request,
        "dashboard/workspace.html",
        {
            "page_title": "Radio Zimbabwe Charts",
        },
    )
