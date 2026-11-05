"""One-time recovery of clear catalogue matches stranded by the old parser."""

from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from apps.accounts.models import Station
from apps.voting.catalogue_matching import automatic_candidate, ranked_candidates
from apps.voting.models import MatchKeyMapping, RawVote, ReviewAudit, VoteJob
from apps.voting.pipeline import lock_station, unresolved_mapping

# New matching logic requires a fresh one-time recovery pass for previously
# reviewed votes, including title typos submitted without an artist.
BATCH = "title-only-typo-retry-2026-11-05"


class Command(BaseCommand):
    help = "Queue unresolved high-confidence catalogue candidates for fresh AI verification once."

    def handle(self, *args, **options):
        if not settings.AI_ALL_VOTES:
            return
        for station in Station.values:
            with transaction.atomic():
                lock_station(station)
                if ReviewAudit.objects.filter(
                    station=station, action="ai_retry_batch", details__batch_id=BATCH
                ).exists():
                    continue
                queued = []
                for vote in RawVote.objects.filter(
                    station=station, ai_status="review"
                ).iterator():
                    mapping = (
                        MatchKeyMapping.objects.select_related("cleaned_song")
                        .filter(
                            station=station,
                            match_key=vote.match_key,
                        )
                        .first()
                    )
                    if not unresolved_mapping(mapping):
                        continue
                    ranked = ranked_candidates(
                        station, vote.artist_normalized, vote.song_normalized
                    )
                    if not automatic_candidate(
                        ranked, vote.artist_normalized, vote.song_normalized
                    ):
                        continue
                    job = VoteJob.objects.select_for_update().filter(vote=vote).first()
                    if job and job.state == "running":
                        continue
                    vote.ai_status = "awaiting"
                    vote.save(update_fields=["ai_status"])
                    VoteJob.objects.update_or_create(
                        vote=vote,
                        defaults={
                            "state": "queued",
                            "attempts": 0,
                            "available_at": timezone.now() + timedelta(seconds=45),
                            "lease_until": None,
                            "lease_token": "",
                            "error": "",
                        },
                    )
                    queued.append(vote.pk)
                ReviewAudit.objects.create(
                    station=station,
                    action="ai_retry_batch",
                    details={"batch_id": BATCH, "vote_ids": queued},
                )
                self.stdout.write(
                    f"{station}: queued {len(queued)} catalogue votes for AI retry; vote_ids={queued}"
                )
