"""Voting rules for one station, applied to every ingestion channel.

The legacy/inherit option preserves historical per-programme behaviour until
the broadcaster explicitly activates a station-wide policy in Admin.
"""
from django.conf import settings

from .models import StationVotingConfig


def voting_rules_for_station(station):
    row = StationVotingConfig.objects.filter(station=station).first()
    policy = row.repeat_song_policy if row else "inherit"
    if policy not in {"inherit", "block", "allow"}:
        policy = "inherit"
    station_limit = row.station_daily_limit if row else None
    return {
        "station_daily_limit": station_limit,
        "repeat_song_policy": policy,
        "allow_repeat_song": (
            bool(settings.ALLOW_REPEAT_SONG)
            if policy == "inherit" else policy == "allow"
        ),
        "repeat_scope": "show" if policy == "inherit" else "station",
        "system_daily_limit": settings.VOTING_DAILY_LIMIT,
    }
