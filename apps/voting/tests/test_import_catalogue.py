import json
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from apps.accounts.models import Station
from apps.voting.models import CleanedSong, ReviewAudit, StationState
from apps.voting.catalogue_matching import ranked_candidates, automatic_candidate


class ImportCatalogueTests(TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "songs.json"
        self.rows = [
            {
                "artist": "Test Artist",
                "title": "Test Song",
                "featured_artists": ["Guest Artist"],
                "album": "Test Album",
            }
        ]
        self.write()

    def write(self):
        self.path.write_text(json.dumps({"schema_version": 1, "songs": self.rows}))

    def run_import(self, *args):
        call_command("import_catalogue", str(self.path), *args, stdout=StringIO())

    def test_all_stations_preserve_decisions_and_repeat_import(self):
        existing = CleanedSong.objects.create(
            station=Station.POWER_FM,
            artist="test artist",
            title="test song",
            canonical_name="test artist - test song",
            status="rejected",
        )
        self.run_import("--all-stations")
        self.assertEqual(CleanedSong.objects.count(), len(Station.values))
        existing.refresh_from_db()
        self.assertEqual(existing.status, "rejected")
        self.run_import("--all-stations")
        self.assertEqual(CleanedSong.objects.count(), len(Station.values))
        self.assertEqual(ReviewAudit.objects.count(), 2 * len(Station.values))

    def test_main_artist_matches_after_review_and_full_credit_is_retained(self):
        self.run_import("--station", Station.NATIONAL_FM)
        song = CleanedSong.objects.get()
        self.assertEqual(song.status, "pending")
        self.assertEqual(song.artist, "Test Artist")
        self.assertEqual(
            song.canonical_name, "Test Artist feat. Guest Artist - Test Song"
        )
        self.assertFalse(
            ranked_candidates(Station.NATIONAL_FM, song.artist, song.title)
        )
        song.status = "verified"
        song.save()
        ranked = ranked_candidates(Station.NATIONAL_FM, song.artist, song.title)
        self.assertEqual(automatic_candidate(ranked, song.artist, song.title), song)
        self.assertFalse(ranked_candidates(Station.POWER_FM, song.artist, song.title))

    def test_dry_run_has_no_writes(self):
        self.run_import("--all-stations", "--dry-run")
        self.assertFalse(CleanedSong.objects.exists())
        self.assertFalse(ReviewAudit.objects.exists())
        self.assertFalse(StationState.objects.exists())

    def test_invalid_last_row_prevents_partial_import(self):
        self.rows.append({"artist": "Missing title"})
        self.write()
        with self.assertRaises(CommandError):
            self.run_import("--all-stations")
        self.assertFalse(CleanedSong.objects.exists())

    def test_normalized_duplicate_rejected(self):
        self.rows.append({"artist": " TEST   ARTIST ", "title": "test song"})
        self.write()
        with self.assertRaises(CommandError):
            self.run_import("--all-stations")
        self.assertFalse(CleanedSong.objects.exists())

    def test_new_songs_can_be_appended_without_duplicates(self):
        self.run_import("--station", Station.POWER_FM)
        self.rows.append({"artist": "New Artist", "title": "New Song"})
        self.write()
        self.run_import("--station", Station.POWER_FM)
        self.assertEqual(CleanedSong.objects.count(), 2)
