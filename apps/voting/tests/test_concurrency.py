from concurrent.futures import ThreadPoolExecutor
from django.db import close_old_connections, connection
from django.test import TransactionTestCase, override_settings
from apps.voting.models import RawVote, User, VoteJob
from apps.voting.services import VotingService


@override_settings(VOTING_DAILY_LIMIT=5, ALLOW_REPEAT_SONG=False)
class TestConcurrentIntake(TransactionTestCase):
    def test_one_listener_cannot_race_past_daily_limit(self):
        if connection.vendor != 'postgresql':
            self.skipTest('Requires real PostgreSQL row locking; exercised in CI.')
        User.objects.create(channel='telegram',user_ref='concurrent',station='radio_zimbabwe')
        def vote(i):
            close_old_connections()
            try:
                return VotingService('telegram','concurrent').handle_incoming_text(f'Concurrent Artist - Track {i}')
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=8) as pool:
            results=list(pool.map(vote,range(16)))
        self.assertEqual(sum('Vote recorded!' in r for r in results),5)
        self.assertEqual(RawVote.objects.count(),5)
        self.assertEqual(VoteJob.objects.count(),5)
