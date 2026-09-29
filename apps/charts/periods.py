from datetime import timedelta


def week_dates(day):
    """A station voting week runs Sunday through Saturday, inclusive."""
    start = day - timedelta(days=(day.weekday() + 1) % 7)
    return start, start + timedelta(days=6)
