from io import StringIO

from django.core.management import call_command
from django.test import TestCase

from apps.accounts.models import Station
from apps.voting.models import (
    CleanedSong,
    CleanedSongTally,
    MatchKeyMapping,
    RawSongTally,
    RawVote,
    ReviewAudit,
)


class CatalogueImportTests(TestCase):
    def run_import(self, **kwargs):
        call_command('import_verified_catalogue', stdout=StringIO(), **kwargs)

    def test_all_stations_idempotent_and_no_votes_created(self):
        self.run_import(all_stations=True)
        self.assertEqual(CleanedSong.objects.count(), 192 * len(Station.values))
        for station in Station.values:
            self.assertEqual(CleanedSong.objects.filter(station=station, status='verified').count(), 192)
        self.run_import(all_stations=True)
        self.assertEqual(CleanedSong.objects.count(), 192 * len(Station.values))
        self.assertEqual(ReviewAudit.objects.filter(action='verified_seed').count(), len(Station.values))
        for model in (RawVote, RawSongTally, CleanedSongTally, MatchKeyMapping):
            self.assertFalse(model.objects.exists())

    def test_existing_decisions_credit_variants_and_aliases_preserved(self):
        rows = [
            ('Sane.wav', '4MABABE', 'rejected'),
            ('Sugar Sugar', 'Mudododo', 'pending'),
            ('Shinsoman', 'Munhu Wangu', 'verified'),
        ]
        existing = [CleanedSong.objects.create(
            station='radio_zimbabwe', artist=a, title=t,
            canonical_name=f'{a} - {t}', status=s,
        ) for a, t, s in rows]
        self.run_import(station='radio_zimbabwe')
        self.assertEqual(CleanedSong.objects.count(), 192)
        for song, (_, _, status) in zip(existing, rows):
            song.refresh_from_db()
            self.assertEqual(song.status, status)
        audit = ReviewAudit.objects.get(action='verified_seed')
        self.assertEqual(audit.details['created'], 189)
        self.assertEqual(audit.details['preserved'], 3)
        self.assertFalse(CleanedSong.objects.exclude(station='radio_zimbabwe').exists())

    def test_remix_does_not_replace_original_or_other_artist(self):
        for artist in ('Learn Zimbabwe', 'Other Artist'):
            CleanedSong.objects.create(
                station='radio_zimbabwe', artist=artist, title='Chibatamabvi',
                canonical_name=f'{artist} - Chibatamabvi', status='verified',
            )
        self.run_import(station='radio_zimbabwe')
        self.assertEqual(CleanedSong.objects.filter(title='Chibatamabvi').count(), 2)
        self.assertEqual(CleanedSong.objects.filter(title='Chibatamabvi (Remix version)').count(), 1)

    def test_restarting_does_not_reapprove_reviewed_import(self):
        self.run_import(station='radio_zimbabwe')
        song = CleanedSong.objects.get(station='radio_zimbabwe', title='Rape Case')
        song.status = 'rejected'
        song.save(update_fields=['status'])
        self.run_import(station='radio_zimbabwe')
        song.refresh_from_db()
        self.assertEqual(song.status, 'rejected')
