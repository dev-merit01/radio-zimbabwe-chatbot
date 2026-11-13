"""Per-station quotas, duplicate-song policy, and Admin boundaries."""
import json
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.voting.models import ChartShow, RawVote, ReviewAudit, StationVotingConfig
from apps.voting.services import VotingService
from apps.voting.station_vote_policy import voting_rules_for_station


@override_settings(SECURE_SSL_REDIRECT=False, AI_ALL_VOTES=True, ALLOW_REPEAT_SONG=False)
class StationVotingRulesTests(TestCase):
    def setUp(self):
        self.saturday = ChartShow.objects.create(
            station="national_fm", name="Voting limits Saturday", code="LMS1",
            kind="weekly", schedule_mode="manual", manual_open=True,
            is_enabled=True, daily_limit=3,
        )
        self.sunday = ChartShow.objects.create(
            station="national_fm", name="Voting limits Sunday", code="LMS2",
            kind="weekly", schedule_mode="manual", manual_open=True,
            is_enabled=True, daily_limit=3,
        )
        self.admin = get_user_model().objects.create_superuser(
            username="vote-rules-admin", password="Test-only-Vote-Rules-583!",
        )
        self.client.force_login(self.admin)
        self.client.post("/accounts/switch-station/", {"station": "national_fm"})

    def submit(self, text, show=None, channel="whatsapp", listener="263771234567", day=None, station="national_fm"):
        show = show or self.saturday
        return VotingService(channel, listener, station).handle_incoming_text(
            text, chart_show=show, submitted_code=show.code,
            vote_date=day,
        )

    def rules(self, *, limit=None, repeat="block"):
        StationVotingConfig.objects.update_or_create(
            station="national_fm",
            defaults={"station_daily_limit": limit, "repeat_song_policy": repeat},
        )

    def test_existing_per_show_behavior_is_preserved_by_default(self):
        self.assertEqual(voting_rules_for_station("national_fm")["repeat_scope"], "show")
        self.assertIn("Submission received", self.submit("Artist One - Song One"))
        self.assertIn("Submission received", self.submit(
            "Artist One - Song One", show=self.sunday,
        ))
        self.assertEqual(RawVote.objects.filter(station="national_fm").count(), 2)

    def test_station_daily_cap_counts_across_different_shows(self):
        self.rules(limit=2, repeat="block")
        self.assertIn("Submission received", self.submit("Artist One - Song One"))
        self.assertIn("Submission received", self.submit("Artist Two - Song Two", show=self.sunday))
        self.assertIn("used all 2 votes for this station", self.submit(
            "Artist Three - Song Three",
        ))
        self.assertEqual(RawVote.objects.count(), 2)

    def test_repeat_song_is_blocked_across_chart_shows_and_channels(self):
        self.rules(limit=10, repeat="block")
        first = self.submit("Artist One - Song One", channel="whatsapp")
        second = self.submit("Artist One - Song One", show=self.sunday, channel="sms")
        self.assertIn("Submission received", first)
        self.assertIn("already voted for this song", second)
        self.assertEqual(RawVote.objects.count(), 1)

    def test_allow_repeat_song_respects_station_cap(self):
        self.rules(limit=2, repeat="allow")
        self.assertIn("Submission received", self.submit("Artist One - Song One"))
        self.assertIn("Submission received", self.submit("Artist One - Song One", show=self.sunday))
        self.assertIn("used all 2 votes for this station", self.submit(
            "Artist One - Song One",
        ))
        self.assertEqual(RawVote.objects.count(), 2)

    def test_per_show_limit_still_applies_beneath_station_cap(self):
        self.rules(limit=10, repeat="allow")
        self.saturday.daily_limit = 1
        self.saturday.save(update_fields=["daily_limit"])
        self.assertIn("Submission received", self.submit("Artist One - Song One"))
        self.assertIn("used all 1 votes for this chart show", self.submit(
            "Artist Two - Song Two",
        ))
        self.assertIn("Submission received", self.submit(
            "Artist Two - Song Two", show=self.sunday,
        ))

    def test_station_limit_resets_by_vote_date(self):
        self.rules(limit=1, repeat="block")
        yesterday = timezone.localdate() - timedelta(days=1)
        self.assertIn("Submission received", self.submit("Artist One - Song One", day=yesterday))
        self.assertIn("Submission received", self.submit("Artist One - Song One"))
        self.assertEqual(RawVote.objects.count(), 2)

    def test_admin_can_save_policy_per_station_without_rewriting_existing_votes(self):
        self.assertIn("Submission received", self.submit("Artist One - Song One"))
        response = self.client.post(
            "/api/workspace/intelligence/voting-policy",
            json.dumps({"station_daily_limit": 7, "repeat_song_policy": "block"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()["vote_policy"]["station_daily_limit"], 7)
        self.assertEqual(RawVote.objects.count(), 1)
        self.assertEqual(StationVotingConfig.objects.get(station="national_fm").repeat_song_policy, "block")
        self.assertIsNone(StationVotingConfig.objects.get(station="power_fm").station_daily_limit)
        self.assertTrue(ReviewAudit.objects.filter(action="station_vote_policy").exists())

    def test_invalid_limits_and_permissions_are_rejected(self):
        for payload in (
            {"station_daily_limit": 0, "repeat_song_policy": "block"},
            {"station_daily_limit": 101, "repeat_song_policy": "block"},
            {"station_daily_limit": True, "repeat_song_policy": "block"},
            {"station_daily_limit": 3, "repeat_song_policy": "unknown"},
        ):
            with self.subTest(payload=payload):
                response = self.client.post(
                    "/api/workspace/intelligence/voting-policy",
                    json.dumps(payload), content_type="application/json",
                )
                self.assertEqual(response.status_code, 400, response.content)
        presenter = get_user_model().objects.create_user(
            username="vote-rule-presenter", password="Test-only-Access-537!",
        )
        self.client.force_login(presenter)
        response = self.client.post(
            "/api/workspace/intelligence/voting-policy",
            json.dumps({"station_daily_limit": 3, "repeat_song_policy": "block"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(voting_rules_for_station("national_fm")["repeat_song_policy"], "inherit")
