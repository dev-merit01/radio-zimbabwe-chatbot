from concurrent.futures import ThreadPoolExecutor
from django.db import close_old_connections, connection
from django.test import TransactionTestCase, override_settings
from apps.voting.models import CleanedSong, MatchKeyMapping, RawVote, User, VoteJob
from apps.voting.services import VotingService


@override_settings(VOTING_DAILY_LIMIT=5, ALLOW_REPEAT_SONG=False)
class TestConcurrentIntake(TransactionTestCase):
    def test_one_listener_cannot_race_past_daily_limit(self):
        if connection.vendor != "postgresql":
            self.skipTest("Requires real PostgreSQL row locking; exercised in CI.")
        User.objects.create(
            channel="telegram", user_ref="concurrent", station="radio_zimbabwe"
        )

        def vote(i):
            close_old_connections()
            try:
                return VotingService("telegram", "concurrent").handle_incoming_text(
                    f"Concurrent Artist - Track {i}"
                )
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(vote, range(16)))
        self.assertEqual(sum("Vote recorded!" in r for r in results), 5)
        self.assertEqual(RawVote.objects.count(), 5)
        self.assertEqual(VoteJob.objects.count(), 5)

    def test_canonical_and_alias_cannot_race_before_matching(self):
        if connection.vendor != "postgresql":
            self.skipTest("Requires real PostgreSQL row locking; exercised in CI.")
        User.objects.create(
            channel="telegram", user_ref="alias-race", station="radio_zimbabwe"
        )
        song = CleanedSong.objects.create(
            station="radio_zimbabwe",
            artist="Example Artist",
            title="Example Song",
            canonical_name="Example Artist - Example Song",
            status="verified",
        )
        MatchKeyMapping.objects.create(
            station="radio_zimbabwe",
            match_key="example artist::alternate title",
            cleaned_song=song,
        )

        def vote(i):
            close_old_connections()
            try:
                title = "Example Song" if i % 2 else "Alternate Title"
                return VotingService("telegram", "alias-race").handle_incoming_text(
                    f"Example Artist - {title}"
                )
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(vote, range(16)))
        self.assertEqual(sum("Vote recorded!" in result for result in results), 1)
        self.assertEqual(RawVote.objects.count(), 1)
        self.assertEqual(VoteJob.objects.count(), 1)
