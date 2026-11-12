"""Featured artist credits in the official station-owned song catalogue."""
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from apps.voting.artist_credits import (
    combine_artist_credits, split_artist_credits,
)
from apps.voting.catalogue_matching import artist_credits
from apps.voting.models import CleanedSong


@override_settings(SECURE_SSL_REDIRECT=False)
class FeaturedArtistCreditsTests(TestCase):
    def setUp(self):
        user = get_user_model().objects.create_superuser(
            "featured-admin", password="test-feature-credits-only"
        )
        self.client.force_login(user)
        self.url = "/api/workspace/songs"

    def post(self, path, payload):
        return self.client.post(
            path, payload, content_type="application/json",
        )

    def test_add_song_with_multiple_features_creates_one_canonical_record(self):
        response = self.post(self.url + "/add", {
            "artist": "  atenda chinx ",
            "title": " PINDIRAI ",
            "featured_artists": " GEMMA GRIFFITHS, LEARN ZIMBABWE ",
        })
        self.assertEqual(response.status_code, 201, response.content)
        song = CleanedSong.objects.get(pk=response.json()["id"])
        self.assertEqual(song.artist, "Atenda Chinx feat. Gemma Griffiths, Learn Zimbabwe")
        self.assertEqual(song.title, "Pindirai")
        self.assertEqual(
            song.canonical_name,
            "Atenda Chinx feat. Gemma Griffiths, Learn Zimbabwe - Pindirai",
        )
        self.assertEqual(CleanedSong.objects.filter(pk=song.pk).count(), 1)
        data = self.client.get(self.url, {"status": "all"}).json()["items"]
        row = next(s for s in data if s["id"] == song.pk)
        self.assertEqual(row["artist"], song.artist)
        self.assertEqual(row["primary_artist"], "Atenda Chinx")
        self.assertEqual(row["featured_artists"], "Gemma Griffiths, Learn Zimbabwe")
        self.assertEqual(
            self.client.get(self.url, {"status": "all", "q": "Gemma"}).json()["total"],
            1,
        )
        lead, features = artist_credits(song.artist)
        self.assertEqual(lead, "atenda chinx")
        self.assertEqual(features, "gemma griffiths learn zimbabwe")

    def test_edit_feature_credits_preserves_song_id_and_vote_identity(self):
        created = self.post(self.url + "/add", {
            "artist": "Freeman HKD", "title": "Gitare", "featured_artists": "",
        })
        self.assertEqual(created.status_code, 201)
        song_id = created.json()["id"]
        edited = self.post(f"{self.url}/{song_id}/review", {
            "action": "edit", "artist": "Freeman HKD", "title": "Gitare",
            "featured_artists": "Madedevido, Gemma Griffiths",
        })
        self.assertEqual(edited.status_code, 200, edited.content)
        song = CleanedSong.objects.get(pk=song_id)
        self.assertEqual(song.artist, "Freeman HKD feat. Madedevido, Gemma Griffiths")
        self.assertEqual(song.pk, song_id)
        self.assertEqual(CleanedSong.objects.count(), 1)
        removed = self.post(f"{self.url}/{song_id}/review", {
            "action": "edit", "artist": "Freeman HKD", "title": "Gitare",
            "featured_artists": "",
        })
        self.assertEqual(removed.status_code, 200, removed.content)
        song.refresh_from_db()
        self.assertEqual(song.artist, "Freeman HKD")

    def test_existing_credited_songs_are_prepopulated_and_editable(self):
        song = CleanedSong.objects.create(
            station="radio_zimbabwe",
            artist="Jnr Spragga feat. Delroy Shewe", title="Daka",
            canonical_name="Jnr Spragga feat. Delroy Shewe - Daka",
            status="verified",
        )
        rows = self.client.get(self.url, {"status": "all"}).json()["items"]
        row = next(s for s in rows if s["id"] == song.id)
        self.assertEqual(row["primary_artist"], "Jnr Spragga")
        self.assertEqual(row["featured_artists"], "Delroy Shewe")
        response = self.post(f"{self.url}/{song.id}/review", {
            "action": "edit", "artist": "Jnr Spragga", "title": "Daka",
            "featured_artists": "Delroy Shewe, Gemma Griffiths",
        })
        self.assertEqual(response.status_code, 200, response.content)
        song.refresh_from_db()
        self.assertEqual(song.artist, "Jnr Spragga feat. Delroy Shewe, Gemma Griffiths")

    def test_duplicate_featured_song_is_not_created(self):
        body = {
            "artist": "Atenda Chinx", "title": "My Man",
            "featured_artists": "Gemma Griffiths",
        }
        self.assertEqual(self.post(self.url + "/add", body).status_code, 201)
        self.assertEqual(self.post(self.url + "/add", body).status_code, 409)
        self.assertEqual(CleanedSong.objects.count(), 1)

    def test_old_api_and_mixed_credit_spellings_remain_compatible(self):
        response = self.post(self.url + "/add", {
            "artist": "Atenda Chinx FT Gemma Griffiths", "title": "Pindirai",
        })
        self.assertEqual(response.status_code, 201, response.content)
        song = CleanedSong.objects.get(pk=response.json()["id"])
        self.assertEqual(song.artist, "Atenda Chinx feat. Gemma Griffiths")
        self.assertEqual(
            combine_artist_credits("Atenda Chinx feat. Gemma Griffiths", "Learn Zimbabwe"),
            "Atenda Chinx feat. Learn Zimbabwe",
        )
        self.assertEqual(
            split_artist_credits("Atenda Chinx featuring Gemma Griffiths"),
            ("Atenda Chinx", "Gemma Griffiths"),
        )

    def test_invalid_credits_do_not_create_catalogue_records(self):
        for featured in (123, ["Gemma"], "X" * 300):
            with self.subTest(featured=featured):
                response = self.post(self.url + "/add", {
                    "artist": "Atenda Chinx", "title": "My Man",
                    "featured_artists": featured,
                })
                self.assertEqual(response.status_code, 400)
        self.assertEqual(CleanedSong.objects.count(), 0)

    def test_credits_are_station_scoped_not_cross_station_edits(self):
        song = CleanedSong.objects.create(
            station="power_fm", artist="Artist feat. Guest",
            title="Track", canonical_name="Artist feat. Guest - Track",
        )
        response = self.post(f"{self.url}/{song.pk}/review", {
            "action": "edit", "artist": "Different", "title": "Track",
            "featured_artists": "New Guest",
        })
        self.assertEqual(response.status_code, 404)
        song.refresh_from_db()
        self.assertEqual(song.artist, "Artist feat. Guest")
