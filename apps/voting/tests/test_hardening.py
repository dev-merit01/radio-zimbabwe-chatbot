import base64
import hashlib
import hmac
import json
import time
from datetime import timedelta
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, override_settings
from django.utils import timezone
from apps.accounts.models import AccountProfile
from apps.charts.tasks import compute_daily_chart
from apps.voting.models import (
    CleanedSong,
    CleanedSongTally,
    InboundEvent,
    OutboundMessage,
    VoteJob,
    RawVote,
)
from apps.voting.pipeline import review_song
from apps.voting.worker import claim, run_one
from apps.voting.services import VotingService


@override_settings(SECURE_SSL_REDIRECT=False, AUTO_AI_MATCH=False)
class TestHardening(TestCase):
    def setUp(self):
        cache.clear()
        self.user = get_user_model().objects.create_superuser(
            "operator", password="correct-password"
        )
        self.client.force_login(self.user)

    def test_bad_json_shapes_are_400(self):
        for data in [[], None, "text", 42]:
            response = self.client.post(
                "/api/workspace/songs/add",
                json.dumps(data),
                content_type="application/json",
            )
            self.assertEqual(response.status_code, 400)
        response = self.client.post(
            "/api/workspace/songs/add",
            {"artist": None, "title": ["x"]},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)

    def test_whitespace_edit_rejected(self):
        song = CleanedSong.objects.create(
            artist="Artist", title="Song", canonical_name="Artist - Song"
        )
        with self.assertRaises(ValueError):
            review_song(
                "radio_zimbabwe", self.user, song.id, "edit", artist=" ", title="Song"
            )

    def test_case_insensitive_duplicate_edit_rejected(self):
        song = CleanedSong.objects.create(
            artist="Artist", title="Song", canonical_name="Artist - Song"
        )
        CleanedSong.objects.create(
            artist="Other", title="Song", canonical_name="Other - Song"
        )
        with self.assertRaises(ValueError):
            review_song(
                "radio_zimbabwe",
                self.user,
                song.id,
                "edit",
                artist="OTHER",
                title="SONG",
            )

    def test_review_does_not_rewrite_unrelated_tallies(self):
        song = CleanedSong.objects.create(
            artist="Artist", title="Song", canonical_name="Artist - Song"
        )
        other = CleanedSong.objects.create(
            artist="Other",
            title="Song",
            canonical_name="Other - Song",
            status="verified",
        )
        tally = CleanedSongTally.objects.create(
            station="radio_zimbabwe",
            cleaned_song=other,
            date=timezone.localdate(),
            count=17,
        )
        review_song("radio_zimbabwe", self.user, song.id, "verified")
        tally.refresh_from_db()
        self.assertEqual(tally.count, 17)

    def test_missing_station_fails_closed(self):
        viewer = get_user_model().objects.create_user("viewer")
        AccountProfile.objects.filter(user=viewer).delete()
        self.client.force_login(viewer)
        self.assertEqual(self.client.get("/api/workspace/overview").status_code, 403)

    def test_private_responses_not_cached(self):
        response = self.client.get("/")
        self.assertIn("no-store", response["Cache-Control"])
        self.assertIn("script-src 'self'", response["Content-Security-Policy"])

    def test_admin_login_uses_throttled_entrypoint(self):
        self.client.logout()
        self.assertRedirects(
            self.client.get("/admin/login/"),
            "/accounts/login/?next=/admin/",
            fetch_redirect_response=False,
        )

    def test_login_throttle_and_safe_redirect(self):
        self.client.logout()
        for _ in range(10):
            self.client.post(
                "/accounts/login/", {"username": "operator", "password": "wrong"}
            )
        self.assertEqual(
            self.client.post(
                "/accounts/login/", {"username": "operator", "password": "wrong"}
            ).status_code,
            429,
        )
        cache.clear()
        response = self.client.post(
            "/accounts/login/",
            {
                "username": "operator",
                "password": "correct-password",
                "next": "https://evil.example/",
            },
        )
        self.assertEqual(response["Location"], "/")

    def test_chart_task_requires_station(self):
        with self.assertRaises(ValueError):
            compute_daily_chart()

    def test_retired_commands_cannot_mutate(self):
        for name in ["llm_match", "process_votes", "clear_database"]:
            with self.assertRaises(CommandError):
                call_command(name)

    def test_uncertain_reply_is_not_automatically_resent(self):
        event = InboundEvent.objects.create(
            provider="telegram",
            station="radio_zimbabwe",
            message_id="1",
            sender="123",
            text="hello",
        )
        reply = OutboundMessage.objects.create(
            event=event,
            text="response",
            state="running",
            lease_until=timezone.now() - timedelta(seconds=1),
        )
        self.assertIsNone(claim(OutboundMessage))
        reply.refresh_from_db()
        self.assertEqual(reply.state, "uncertain")

    def test_ambiguous_http_success_is_uncertain(self):
        event = InboundEvent.objects.create(
            provider="telegram",
            station="radio_zimbabwe",
            message_id="1",
            sender="123",
            text="hello",
        )
        reply = OutboundMessage.objects.create(event=event, text="response")
        with patch(
            "apps.voting.worker.send", side_effect=ValueError("malformed success")
        ):
            run_one(OutboundMessage)
        reply.refresh_from_db()
        self.assertEqual(reply.state, "uncertain")

    def test_exhausted_worker_lease_stops_retrying(self):
        event = InboundEvent.objects.create(
            provider="telegram",
            station="radio_zimbabwe",
            message_id="1",
            sender="123",
            state="running",
            attempts=6,
            lease_until=timezone.now() - timedelta(seconds=1),
        )
        self.assertIsNone(claim(InboundEvent))
        event.refresh_from_db()
        self.assertEqual(event.state, "failed")

    def test_daily_limit_still_applies_after_matching(self):
        for i in range(8):
            VotingService("telegram", "same-listener").handle_incoming_text(
                f"Example Artist - Track {i}"
            )
        self.assertEqual(RawVote.objects.count(), 5)
        self.assertEqual(VoteJob.objects.count(), 5)

    @override_settings(
        BIRD_WEBHOOK_SECRET="whsec_" + base64.b64encode(b"test-signing-key").decode()
    )
    def test_bird_signature_and_replay_window(self):
        body = json.dumps(
            {
                "event": "whatsapp.inbound",
                "payload": {
                    "direction": "incoming",
                    "id": "abc",
                    "body": {"type": "text", "text": {"text": "Artist - Song"}},
                    "sender": {"contact": {"identifierValue": "+263771111111"}},
                },
            }
        ).encode()
        for offset, expected in [(0, 202), (-600, 403)]:
            stamp = str(int(time.time()) + offset)
            signature = base64.b64encode(
                hmac.new(
                    b"test-signing-key",
                    b"event." + stamp.encode() + b"." + body,
                    hashlib.sha256,
                ).digest()
            ).decode()
            response = self.client.post(
                "/webhook/bird/",
                body,
                content_type="application/json",
                HTTP_WEBHOOK_ID="event",
                HTTP_WEBHOOK_TIMESTAMP=stamp,
                HTTP_WEBHOOK_SIGNATURE="v1," + signature,
            )
            self.assertEqual(response.status_code, expected, response.content)
