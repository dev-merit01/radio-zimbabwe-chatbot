"""Suspected spam is held before the voting worker or AI can count it."""
import base64
import hashlib
import hmac
import json
import time
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from apps.accounts.models import AccountProfile
from apps.charts.publishing import publish_edition
from apps.voting.models import StationVotingConfig
from apps.voting.models import (
    ChartShow, InboundEvent, OutboundMessage, RawVote,
    ReviewAudit, SuspectedSpamVote, VoteJob, WeeklyChart,
)
from apps.voting.worker import run_one


@override_settings(
    SECURE_SSL_REDIRECT=False,
    BIRD_SILENT_VOTING=True,
    AI_ALL_VOTES=False,
    AUTO_AI_MATCH=False,
    BIRD_WEBHOOK_SECRET=base64.b64encode(b"flagging-test-key").decode(),
    BIRD_NUMBER_STATION_MAP={
        "263790573340": "national_fm",
        "263790563340": "power_fm",
    },
)
class SuspectedSpamVoteTests(TestCase):
    def setUp(self):
        # This suite explicitly tests backward-compatible code-free intake.
        # Production's established stations now require programme codes.
        StationVotingConfig.objects.filter(
            station__in=("radio_zimbabwe", "national_fm", "power_fm",
                        "classic_263", "central_radio", "khulumani_fm",
                        "luzibo_radio")
        ).update(require_codes=False)
        self.admin = get_user_model().objects.create_superuser(
            username="spam-admin", password="Only-Testing-Password-78!"
        )
        self.client.force_login(self.admin)
        self.client.post("/accounts/switch-station/", {"station": "national_fm"})
        self.show = ChartShow.objects.create(
            station="national_fm", name="Saturday Vote Chart", code="SAT",
            schedule_mode="manual", manual_open=True, is_enabled=True,
        )

    def send_vote(self, msg, message_id, sender="+263771112222",
                  number="+263790573340"):
        payload = {
            "type": "whatsapp.received",
            "data": {
                "whatsapp_id": message_id, "direction": "inbound",
                "from": {"phone_number": sender},
                "to": {"phone_number": number},
                "text": {"body": msg},
            },
        }
        body = json.dumps(payload)
        timestamp = str(int(time.time()))
        hook_id = "spamming-" + message_id
        signature = base64.b64encode(hmac.new(
            b"flagging-test-key",
            (hook_id + "." + timestamp + "." + body).encode(),
            hashlib.sha256,
        ).digest()).decode()
        return self.client.post(
            "/webhook/bird/", body, content_type="application/json",
            HTTP_WEBHOOK_ID=hook_id,
            HTTP_WEBHOOK_TIMESTAMP=timestamp,
            HTTP_WEBHOOK_SIGNATURE="v1," + signature,
        )

    def review(self, vote_id, action):
        return self.client.post(
            "/api/workspace/spam/review",
            json.dumps({"id": vote_id, "action": action}),
            content_type="application/json",
        )

    def test_repeated_identical_sender_flags_third_and_holds_before_ai(self):
        for msg_id in ("first", "second"):
            self.assertEqual(self.send_vote(
                "SAT River Artist - Morning Song", msg_id
            ).status_code, 202)
        flagged = self.send_vote("SAT River Artist - Morning Song", "third")
        self.assertEqual(flagged.status_code, 202)
        self.assertTrue(flagged.json()["flagged"])
        self.assertEqual(InboundEvent.objects.count(), 2)
        self.assertEqual(SuspectedSpamVote.objects.count(), 1)
        flagged = SuspectedSpamVote.objects.get()
        self.assertEqual(flagged.reason, "repeated_identical_vote")
        self.assertEqual(flagged.chart_show_id, self.show.pk)
        self.assertFalse(RawVote.objects.exists())
        self.assertFalse(VoteJob.objects.exists())
        self.assertFalse(OutboundMessage.objects.exists())
        self.assertEqual(self.send_vote(
            "SAT River Artist - Morning Song", "third"
        ).json(), {"ok": True, "duplicate": True, "flagged": True})
        self.assertEqual(SuspectedSpamVote.objects.count(), 1)

    def test_rapid_burst_is_flagged_even_when_song_titles_differ(self):
        for i in range(5):
            self.assertEqual(self.send_vote(
                f"SAT River Artist - Song Number {i}", f"burst-{i}"
            ).status_code, 202)
        response = self.send_vote(
            "SAT River Artist - Another Good Song", "burst-sixth"
        )
        self.assertEqual(response.status_code, 202)
        self.assertTrue(response.json()["flagged"])
        self.assertEqual(
            SuspectedSpamVote.objects.get().reason, "rapid_burst"
        )
        self.assertEqual(InboundEvent.objects.count(), 5)

    def test_popular_song_from_different_listeners_is_never_spam(self):
        for i in range(9):
            response = self.send_vote(
                "SAT River Artist - Morning Song", f"listener-{i}",
                sender=f"+2637710000{i:02d}",
            )
            self.assertEqual(response.status_code, 202)
            self.assertFalse(response.json().get("flagged"))
        self.assertEqual(InboundEvent.objects.count(), 9)
        self.assertFalse(SuspectedSpamVote.objects.exists())

    def test_admin_can_review_and_release_with_original_routing(self):
        for i in range(3):
            self.send_vote("SAT River Artist - Morning Song", f"release-{i}")
        flagged = SuspectedSpamVote.objects.get()
        # Show may be paused after receipt. Safe release is tied to its
        # original programme, not to current settings.
        self.show.is_enabled = False
        self.show.save(update_fields=["is_enabled"])
        queue = self.client.get("/api/workspace/spam")
        self.assertEqual(queue.status_code, 200)
        self.assertEqual(queue.json()["pending"], 1)
        self.assertEqual(queue.json()["items"][0]["show"], self.show.name)
        self.assertNotIn(flagged.sender, json.dumps(queue.json()))
        response = self.review(flagged.id, "release")
        self.assertEqual(response.status_code, 200, response.content)
        flagged.refresh_from_db()
        self.assertEqual(flagged.state, "released")
        event = InboundEvent.objects.get(message_id=flagged.message_id)
        self.assertEqual(event.chart_show_id, self.show.pk)
        self.assertEqual(event.vote_text, flagged.vote_text)
        self.assertEqual(event.received_at, flagged.received_at)
        self.assertTrue(run_one(InboundEvent))
        # Existing duplicate vote protections still apply to the released vote.
        self.assertEqual(RawVote.objects.filter(chart_show=self.show).count(), 1)
        self.assertEqual(self.review(flagged.pk, "release").status_code, 404)
        self.assertTrue(ReviewAudit.objects.filter(
            station="national_fm", action="spam_released"
        ).exists())
        self.assertFalse(OutboundMessage.objects.exists())

    def test_admin_can_dismiss_and_station_user_cannot_access_or_review(self):
        for i in range(3):
            self.send_vote("SAT River Artist - Morning Song", f"dismiss-{i}")
        flagged = SuspectedSpamVote.objects.get()
        user = get_user_model().objects.create_user(
            username="station-operator", password="Testing-Only-983!"
        )
        AccountProfile.objects.update_or_create(
            user=user, defaults={"station": "national_fm"}
        )
        client = Client()
        client.force_login(user)
        self.assertEqual(client.get("/api/workspace/spam").status_code, 403)
        self.assertEqual(client.post(
            "/api/workspace/spam/review",
            json.dumps({"id": flagged.pk, "action": "release"}),
            content_type="application/json",
        ).status_code, 403)
        self.assertEqual(self.review(flagged.pk, "dismiss").status_code, 200)
        flagged.refresh_from_db()
        self.assertEqual(flagged.state, "dismissed")
        self.assertFalse(InboundEvent.objects.filter(
            message_id=flagged.message_id
        ).exists())
        self.assertTrue(ReviewAudit.objects.filter(
            station="national_fm", action="spam_dismissed"
        ).exists())

    def test_cannot_release_after_chart_period_was_published(self):
        for i in range(3):
            self.send_vote("SAT River Artist - Morning Song", f"locked-{i}")
        flagged = SuspectedSpamVote.objects.get()
        day = timezone.localtime(flagged.received_at).date()
        WeeklyChart.objects.create(
            station="national_fm", chart_show=self.show,
            year=day.isocalendar().year, week_number=day.isocalendar().week,
            week_start=day-timedelta(days=6),
            week_end=day,
            is_finalized=True, chart_size=20,
        )
        response = self.review(flagged.pk, "release")
        self.assertEqual(response.status_code, 400)
        self.assertIn("published chart", response.json()["error"])
        flagged.refresh_from_db()
        self.assertEqual(flagged.state, "pending")
        self.assertFalse(InboundEvent.objects.filter(
            message_id=flagged.message_id
        ).exists())

    def test_stations_are_isolated_even_when_sender_is_the_same(self):
        for i in range(3):
            self.send_vote("SAT River Artist - Morning Song", f"station-{i}")
        flagged = SuspectedSpamVote.objects.get()
        self.client.post("/accounts/switch-station/", {"station": "power_fm"})
        self.assertEqual(self.client.get("/api/workspace/spam").json()["pending"], 0)
        self.assertEqual(self.review(flagged.pk, "release").status_code, 404)
        power = ChartShow.objects.get(station="power_fm", code="P40")
        power.is_enabled = True
        power.schedule_mode = "manual"
        power.manual_open = True
        power.save(update_fields=["is_enabled", "schedule_mode", "manual_open"])
        self.assertEqual(self.send_vote(
            "P40 Another Artist - Song Title", "power-first",
            number="+263790563340"
        ).status_code, 202)
        self.assertFalse(InboundEvent.objects.get(
            message_id="power-first"
        ).chart_show is None)

    def test_cannot_publish_until_flagged_votes_are_reviewed(self):
        self.show.broadcast_weekday = timezone.localdate().weekday()
        self.show.save(update_fields=["broadcast_weekday"])
        for i in range(3):
            self.send_vote("SAT River Artist - Morning Song", f"publish-{i}")
        while run_one(InboundEvent):
            pass
        while run_one(VoteJob):
            pass
        with self.assertRaisesMessage(ValueError, "flagged votes"):
            publish_edition(
                "national_fm", timezone.localdate(), self.admin,
                20, chart_show=self.show,
            )
        self.assertEqual(SuspectedSpamVote.objects.filter(
            station="national_fm", state="pending",
        ).count(), 1)
