from datetime import timedelta


def week_dates(day):
    """A station voting week runs Sunday through Saturday, inclusive."""
    start = day - timedelta(days=(day.weekday() + 1) % 7)
    return start, start + timedelta(days=6)


def campaign_local_dates(show):
    """Return inclusive Zimbabwe-local calendar bounds for an annual campaign.

    ChartShow.campaign_end is an exclusive timestamp. Database datetime values
    are stored in UTC; calling .date() before converting timezones may silently
    include the preceding day instead of 1 December in Zimbabwe.
    """
    from zoneinfo import ZoneInfo
    from django.conf import settings
    from django.utils import timezone

    tz = ZoneInfo(settings.TIME_ZONE)
    start = (
        timezone.localtime(show.campaign_start, tz).date()
        if show.campaign_start else None
    )
    end = (
        timezone.localtime(
            show.campaign_end - timedelta(microseconds=1), tz
        ).date() if show.campaign_end else None
    )
    return start, end
