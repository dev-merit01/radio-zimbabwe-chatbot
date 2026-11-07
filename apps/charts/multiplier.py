"""Transparent weighted chart scores; the underlying listener vote never changes."""
from apps.voting.models import StationVotingConfig


def station_multiplier(station):
    return StationVotingConfig.objects.filter(station=station).values_list(
        "vote_multiplier", flat=True
    ).first() or 1


def score_for_votes(votes, multiplier):
    return int(votes) * int(multiplier)
