"""Conservative, explainable spam heuristics for new inbound votes.

Called while holding the existing per-station DB lock. Only sender-local
behaviour is examined: popularity across independent listeners is not spam.
Flagged messages never enter the vote/AI pipeline until manually released.
"""
import re
from datetime import timedelta

from apps.voting.models import InboundEvent, SuspectedSpamVote


# Review starting with the sixth vote in one minute, or the third repetition
# of the same vote within two minutes (from the same provider and sender).
ONE_MINUTE_LIMIT = 5
IDENTICAL_TWO_MINUTE_LIMIT = 2


def _normalise(text):
    return re.sub(r"\s+", " ", text.casefold().strip())


def suspected_spam_reason(station, provider, sender, vote_text, received_at):
    """Return a short auditable reason, or None for an unflagged submission.

    Use receipt timestamps rather than worker time. This function must run
    under lock_station(station) so concurrent deliveries cannot both skip
    the threshold.
    """
    one_minute_ago = received_at - timedelta(seconds=60)
    two_minutes_ago = received_at - timedelta(seconds=120)
    scope = {"station": station, "provider": provider, "sender": sender}
    recent_events = InboundEvent.objects.filter(
        **scope, received_at__gt=one_minute_ago,
        received_at__lte=received_at,
    ).count()
    recent_held = SuspectedSpamVote.objects.filter(
        **scope, received_at__gt=one_minute_ago,
        received_at__lte=received_at,
    ).count()
    if recent_events + recent_held >= ONE_MINUTE_LIMIT:
        return "rapid_burst"

    normalized = _normalise(vote_text)
    if not normalized:
        return None

    # Limit text reads even if a malicious sender floods the endpoint.
    previous = InboundEvent.objects.filter(
        **scope, received_at__gt=two_minutes_ago,
        received_at__lte=received_at,
    ).order_by("-received_at", "-pk").values_list("vote_text", flat=True)[:100]
    seen = sum(_normalise(text) == normalized for text in previous)
    if seen >= IDENTICAL_TWO_MINUTE_LIMIT:
        return "repeated_identical_vote"
    return None
