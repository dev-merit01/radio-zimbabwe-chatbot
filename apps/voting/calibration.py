"""Measured station-specific thresholds for the secondary fuzzy acceptance gate.

Scores are heuristics, not probabilities. No profile is trusted until a
held-out labelled evaluation meets minimum sample and precision requirements.
"""
from dataclasses import dataclass

from django.conf import settings

from .models import MatchCalibration


@dataclass(frozen=True)
class Thresholds:
    min_score: float = 0.88
    min_gap: float = 0.08
    min_artist: float = 0.83
    min_title: float = 0.83


DEFAULTS = Thresholds()


def active_thresholds(station):
    if not getattr(settings, "AI_CALIBRATED_MATCHING_ENABLED", True):
        return DEFAULTS
    profile = MatchCalibration.objects.filter(
        station=station, is_active=True, labelled_count__gte=500,
    ).first()
    if not profile:
        return DEFAULTS
    if not (
        0.70 <= profile.min_score <= 1.0
        and 0.05 <= profile.min_gap <= 0.30
        and 0.70 <= profile.min_artist <= 1.0
        and 0.70 <= profile.min_title <= 1.0
        and profile.evaluated_precision >= 0.99
    ):
        return DEFAULTS
    return Thresholds(
        profile.min_score, profile.min_gap,
        profile.min_artist, profile.min_title,
    )


def secondary_fuzzy_candidate(ranked, artist, title, thresholds=DEFAULTS):
    """Independent lexical confirmation for an AI choice, never a solo count."""
    if not ranked:
        return None
    from .catalogue_matching import normalise, variants

    score, artist_score, title_score, song = ranked[0]
    gap = score - ranked[1][0] if len(ranked) > 1 else 1.0
    if not (
        gap >= thresholds.min_gap
        and score >= thresholds.min_score
        and artist_score >= thresholds.min_artist
        and title_score >= thresholds.min_title
        and min(len(normalise(title)), len(normalise(song.title))) >= 6
    ):
        return None
    if variants(title) != variants(song.title):
        return None
    return song
