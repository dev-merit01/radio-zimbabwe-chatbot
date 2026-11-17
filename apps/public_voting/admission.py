"""Atomic admission to AirVote from the separate, private web holding store.

The D1 gateway can submit the same leased receipt repeatedly. Only the first
transaction can create a canonical vote; the receipt's UUID is unique in
PostgreSQL and checked before VotingService is called.
"""
import re
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from apps.accounts.models import station_exists
from apps.voting.models import ChartShow, CleanedSong, EditionCandidate
from apps.voting.services import VotingService
from .models import WebVoteReceipt
from .views import candidate_edition, permitted_station

_HEX64 = re.compile(r"[a-f0-9]{64}\Z")


def parse_receipt(data):
    if not isinstance(data, dict):
        raise ValueError("invalid_payload")
    try:
        vote_id = uuid.UUID(data["id"])
        station = data["station"]
        show_id = data["show_id"]
        song_id = data["song_id"]
        voter_key = data["voter_key"]
        received_at = datetime.fromisoformat(data["received_at"].replace("Z", "+00:00"))
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise ValueError("invalid_payload") from exc
    if (not isinstance(station, str) or not 1 <= len(station) <= 32
            or type(show_id) is not int or show_id <= 0
            or type(song_id) is not int or song_id <= 0
            or not isinstance(voter_key, str) or not _HEX64.fullmatch(voter_key)
            or timezone.is_naive(received_at)):
        raise ValueError("invalid_payload")
    return vote_id, station, show_id, song_id, voter_key, received_at


def admit_web_vote(data):
    vote_id, station, show_id, song_id, voter_key, received_at = parse_receipt(data)
    # Fail closed on forged/future/stale receipts; only gateway server time is used.
    now = timezone.now()
    if received_at > now + timedelta(minutes=2) or received_at < now - timedelta(hours=72):
        return _rejection(vote_id, station, "receipt_expired")
    with transaction.atomic():
        previous = WebVoteReceipt.objects.select_for_update().filter(vote_id=vote_id).first()
        if previous:
            return {"id": str(vote_id), "state": previous.state, "reason": previous.reason}
        receipt = WebVoteReceipt.objects.create(
            vote_id=vote_id, station=station, state="rejected", reason="invalid_vote",
        )
        if not station_exists(station, active_only=True) or not permitted_station(station):
            return _finish(receipt, "station_unavailable")
        show = ChartShow.objects.filter(
            pk=show_id, station=station, is_enabled=True, is_archived=False,
        ).exclude(kind="legacy").first()
        if not show or not show.accepts_at(received_at):
            return _finish(receipt, "voting_closed")
        song = CleanedSong.objects.filter(
            pk=song_id, station=station, status="verified",
        ).first()
        if not song:
            return _finish(receipt, "song_unavailable")
        vote_date = timezone.localtime(received_at, ZoneInfo(settings.TIME_ZONE)).date()
        edition = candidate_edition(show, vote_date)
        if not edition or not EditionCandidate.objects.filter(edition=edition, song=song).exists():
            return _finish(receipt, "song_not_eligible")
        # The existing station lock and VotingService policy are the only
        # authoritative daily limit and repeat-song decision, for all channels.
        result = VotingService(
            "web", voter_key, station=station, voter_fingerprint=voter_key,
        ).handle_incoming_text(
            f"{song.artist} - {song.title}", vote_date=vote_date,
            chart_show=show, submitted_code=show.code,
        )
        receipt.chart_show = show
        receipt.song = song
        if result.startswith("✅"):
            receipt.state = "admitted"
            receipt.reason = ""
        else:
            receipt.reason = (
                "repeat_vote" if "already voted" in result
                else "vote_limit" if "used all" in result
                else "invalid_vote"
            )
        receipt.save(update_fields=["state", "reason", "chart_show", "song"])
        return {"id": str(vote_id), "state": receipt.state, "reason": receipt.reason}


def _finish(receipt, reason):
    receipt.reason = reason
    receipt.save(update_fields=["reason"])
    return {"id": str(receipt.vote_id), "state": "rejected", "reason": reason}


def _rejection(vote_id, station, reason):
    # Replaying an old receipt must not turn a previously admitted vote into a rejection.
    with transaction.atomic():
        row, _ = WebVoteReceipt.objects.get_or_create(
            vote_id=vote_id,
            defaults={"station": station, "state": "rejected", "reason": reason},
        )
        return {"id": str(vote_id), "state": row.state, "reason": row.reason}
