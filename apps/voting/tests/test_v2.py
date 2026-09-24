from datetime import timedelta
from django.test import TestCase, override_settings
from django.contrib.auth import get_user_model
from django.utils import timezone
from apps.voting.models import (
    InboundEvent,
    RawVote,
    VoteJob,
    CleanedSong,
    CleanedSongTally,
    OutboundMessage,
)
from apps.voting.worker import run_one
from apps.voting.pipeline import review_song
from apps.voting.services import VotingService
from apps.charts.publishing import publish_week
from desktop.launcher import validate_url


@override_settings(
    SECURE_SSL_REDIRECT=False,
    TELEGRAM_WEBHOOK_SECRET="test-secret",
    AUTO_AI_MATCH=False,
)
class TestVersionTwo(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser(
            "operator", password="test-only-password"
        )

    def vote(self, day=None):
        VotingService("telegram", "listener-1").handle_incoming_text(
            "Example Artist - Example Song", vote_date=day
        )
        self.assertTrue(run_one(VoteJob))
        return CleanedSong.objects.get(station="radio_zimbabwe")

    def test_receipt_is_authenticated_and_deduplicated(self):
        payload = {
            "update_id": 123,
            "message": {
                "chat": {"id": 123, "type": "private"},
                "from": {"id": 123},
                "text": "Example Artist - Example Song",
            },
        }
        url = "/webhook/telegram/"
        self.assertEqual(
            self.client.post(url, payload, content_type="application/json").status_code,
            403,
        )
        for status in (202, 200):
            self.assertEqual(
                self.client.post(
                    url,
                    payload,
                    content_type="application/json",
                    HTTP_X_TELEGRAM_BOT_API_SECRET_TOKEN="test-secret",
                ).status_code,
                status,
            )
        self.assertEqual(InboundEvent.objects.count(), 1)
        self.assertEqual(RawVote.objects.count(), 0)
        self.assertTrue(run_one(InboundEvent))
        self.assertEqual(RawVote.objects.count(), 1)
        self.assertEqual(OutboundMessage.objects.count(), 1)
        self.assertFalse(run_one(InboundEvent))

    def test_review_controls_totals_without_losing_raw_votes(self):
        song = self.vote()
        self.assertEqual(song.status, "pending")
        self.assertEqual(CleanedSongTally.objects.count(), 0)
        review_song("radio_zimbabwe", self.admin, song.id, "verified")
        self.assertEqual(CleanedSongTally.objects.get().count, 1)
        review_song("radio_zimbabwe", self.admin, song.id, "rejected")
        self.assertEqual(CleanedSongTally.objects.count(), 0)
        self.assertEqual(RawVote.objects.count(), 1)

    def test_archives_are_idempotent_and_preserve_titles(self):
        today = timezone.localdate()
        monday = today - timedelta(days=today.weekday() + 7)
        song = self.vote(monday)
        review_song("radio_zimbabwe", self.admin, song.id, "verified")
        chart, created = publish_week("radio_zimbabwe", monday, self.admin)
        self.assertTrue(created)
        self.assertFalse(publish_week("radio_zimbabwe", monday, self.admin)[1])
        review_song(
            "radio_zimbabwe",
            self.admin,
            song.id,
            "edit",
            artist="Updated",
            title="Changed",
        )
        self.assertEqual(chart.entries.get().title, "Example Song")
        self.assertEqual(chart.total_votes, 1)

    def test_current_week_cannot_be_published(self):
        today = timezone.localdate()
        with self.assertRaises(ValueError):
            publish_week(
                "radio_zimbabwe", today - timedelta(days=today.weekday()), self.admin
            )

    def test_workspace_requires_login(self):
        self.assertEqual(self.client.get("/api/workspace/overview").status_code, 401)
        self.assertEqual(self.client.get("/").status_code, 302)

    def test_read_only_user_cannot_review(self):
        user = get_user_model().objects.create_user(
            "viewer", password="test-only-password"
        )
        self.client.force_login(user)
        self.assertEqual(
            self.client.post(
                "/api/workspace/songs/1/review",
                {"action": "verified"},
                content_type="application/json",
            ).status_code,
            403,
        )

    def test_cross_station_review_refused(self):
        self.client.force_login(self.admin)
        song = CleanedSong.objects.create(
            station="national_fm",
            artist="Other",
            title="Song",
            canonical_name="Other - Song",
        )
        self.assertEqual(
            self.client.post(
                f"/api/workspace/songs/{song.id}/review",
                {"action": "verified"},
                content_type="application/json",
            ).status_code,
            404,
        )
        self.assertEqual(
            self.client.get("/api/workspace/songs?status=all").json()["total"], 0
        )

    def test_workspace_reads_and_template(self):
        self.client.force_login(self.admin)
        self.assertContains(self.client.get("/"), "workspace/studio.js")
        for path in ["overview", "songs", "incoming", "health", "audit", "export"]:
            self.assertEqual(
                self.client.get("/api/workspace/" + path).status_code, 200, path
            )
        for path in ["chart/today", "chart/archives"]:
            self.assertEqual(self.client.get("/api/" + path).status_code, 200, path)

    def test_csrf_required(self):
        from django.test import Client

        client = Client(enforce_csrf_checks=True)
        client.force_login(self.admin)
        self.assertEqual(
            client.post(
                "/api/workspace/retry", {}, content_type="application/json"
            ).status_code,
            403,
        )

    def test_desktop_rejects_unsafe_urls(self):
        for value in [
            "http://example.org",
            "file:///tmp/test",
            "https://user:pass@example.org",
            "https://example.org/?token=x",
        ]:
            with self.assertRaises(ValueError):
                validate_url(value)
        self.assertEqual(
            validate_url("https://station.example.org"), "https://station.example.org/"
        )
