"""Small, safe station-level overrides for vote candidate retrieval and OpenAI."""
from django.conf import settings
from .models import MatchingControl


def options_for_station(station):
    row = MatchingControl.objects.filter(station=station).first() if station else None
    model = (row.model_override if row else "") or settings.OPENAI_MODEL
    limit = row.shortlist_limit if row else 16
    limit = min(24, max(5, int(limit)))
    mode = row.cache_mode if row else "inherit"
    return {
        "model": model,
        "shortlist_limit": limit,
        "cache_enabled": (
            bool(getattr(settings, "AI_MATCH_CACHE_ENABLED", False))
            if mode == "inherit" else mode == "on"
        ),
        "cache_mode": mode,
        "model_override": row.model_override if row else "",
    }
