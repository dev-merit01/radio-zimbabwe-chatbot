"""Small, safe station-level overrides for vote candidate retrieval and OpenAI."""
from django.conf import settings
from .models import MatchingControl

# Curated text-model choices compatible with the vote verification workflow.
# Model availability and pricing depend on the configured OpenAI API account.
SUPPORTED_MATCHING_MODELS = (
    ("gpt-6-luna", "GPT-6 Luna · efficient"),
    ("gpt-6.1-terra", "GPT-6.1 Terra · balanced"),
    ("gpt-6.1-sol", "GPT-6.1 Sol · advanced"),
    ("gpt-6-astra", "GPT-6 Astra · premium"),
    ("gpt-4o-mini", "GPT-4o mini · economical"),
    ("gpt-4.1-mini", "GPT-4.1 mini"),
    ("gpt-4o", "GPT-4o"),
    ("gpt-4.1", "GPT-4.1"),
    ("gpt-5.6-sol", "GPT-5.6 Sol"),
)


def matching_model_choices():
    """Allow the documented options plus a pre-existing Railway default."""
    choices = [("", f"Railway default ({settings.OPENAI_MODEL})")]
    choices.extend(SUPPORTED_MATCHING_MODELS)
    if settings.OPENAI_MODEL not in {id for id, _ in SUPPORTED_MATCHING_MODELS}:
        choices.append((settings.OPENAI_MODEL, settings.OPENAI_MODEL))
    return choices



def options_for_station(station):
    row = MatchingControl.objects.filter(station=station).first() if station else None
    model = (row.model_override if row else "") or settings.OPENAI_MODEL
    limit = row.shortlist_limit if row else 16
    limit = min(24, max(5, int(limit)))
    mode = row.cache_mode if row else "inherit"
    return {
        "model": model,
        "shortlist_limit": limit,
        "retrieval_min_artist": row.retrieval_min_artist if row else 0.68,
        "retrieval_min_title": row.retrieval_min_title if row else 0.66,
        "cache_enabled": (
            bool(getattr(settings, "AI_MATCH_CACHE_ENABLED", False))
            if mode == "inherit" else mode == "on"
        ),
        "cache_mode": mode,
        "model_override": row.model_override if row else "",
    }
