"""Multi-chart AirVote: routing, isolation, silent delivery, and immutable archives."""
import base64
import hashlib
import hmac
import json
import time
from datetime import date, timedelta

from django.contrib.auth import get_user_model
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from apps.accounts.models import AccountProfile
from apps.charts.publishing import publish_edition
from apps.charts.routing import route_vote
from apps.charts.show_rankings import counts_for_show
from apps.voting.models import (
    ChartShow, CleanedSong, InboundEvent, OutboundMessage,
    RawSongTally, RawVote, StationVotingConfig, UnassignedVote, VoteJob,
)
from apps.voting.pipeline import review_song
from apps.voting.services import VotingService
from apps.voting.worker import run_one


@override_settings(
    SECURE_SSL_REDIRECT=False, AI_ALL_VOTES=False, AUTO_AI_MATCH=False,
    BIRD_SILENT_VOTING=True,
    BIRD_WEBHOOK_SECRET=base64.b64encode(b"chart-test-key").decode(),
    BIRD_NUMBER_STATION_MAP={
        "263790573340": "national_fm",
        "263790563340": "power_fm",
    },
)
class MultiChartShowTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser(
            username="chart-admin", password="Only-For-Testing-637!"
        )
        self.client.force_login(self.admin)
        self.client.post("/accounts/switch-station/", {"station": "national_fm"})
        self.saturday = ChartShow.objects.create(
            station="national_fm", name="Saturday Top 20", code="SAT",
            kind="weekly", schedule_mode="manual", manual_open=True,
            is_enabled=True, broadcast_weekday=5,
        )
        self.gospel = ChartShow.objects.create(
            station="national_fm", name="Sunday Gospel Top 20", code="GOS",
            kind="weekly", schedule_mode="manual", manual_open=True,
            is_enabled=True, broadcast_weekday=6,
        )
        StationVotingConfig.objects.filter(
            station="national_fm"
        ).update(require_codes=True)

    def bird(self, text, message_id="wam-1", number="+263790573340"):
        payload = {
            "type": "whatsapp.received",
            "data": {
                "whatsapp_id": message_id, "direction": "inbound",
                "from": {"phone_number": "+263771112222"},
                "to": {"phone_number": number},
                "text": {"body": text},
            },
        }
        body = json.dumps(payload)
        timestamp = str(int(time.time()))
        webhook_id = "chart-" + message_id
        mac = hmac.new(
            b"chart-test-key",
            (webhook_id + "." + timestamp + "." + body).encode(),
            hashlib.sha256,
        ).digest()
        return self.client.post(
            "/webhook/bird/", body, content_type="application/json",
            HTTP_WEBHOOK_ID=webhook_id, HTTP_WEBHOOK_TIMESTAMP=timestamp,
            HTTP_WEBHOOK_SIGNATURE="v1," + base64.b64encode(mac).decode(),
        )

    def post(self, endpoint, data):
        return self.client.post(
            "/api/workspace/" + endpoint, data,
            content_type="application/json",
        )

    def consume(self):
        while run_one(InboundEvent):
            pass
        while run_one(VoteJob):
            pass

    def verify_song(self):
        song = CleanedSong.objects.get(
            station="national_fm", canonical_name="River Artist - Morning Song"
        )
        review_song("national_fm", self.admin, song.pk, "verified")
        return song

    def test_same_number_separate_codes_and_silent_reply(self):
        self.assertEqual(self.bird("SAT River Artist - Morning Song", "a").status_code, 202)
        self.assertEqual(self.bird("GOS River Artist - Morning Song", "b").status_code, 202)
        events = list(InboundEvent.objects.order_by("message_id"))
        self.assertEqual([e.chart_show_id for e in events], [
            self.saturday.pk, self.gospel.pk
        ])
        self.assertEqual([e.vote_text for e in events], [
            "River Artist - Morning Song", "River Artist - Morning Song"
        ])
        self.consume()
        self.assertEqual(OutboundMessage.objects.count(), 0)
        self.assertEqual(RawVote.objects.count(), 2)
        self.assertEqual(set(RawVote.objects.values_list("submitted_code", flat=True)),
                         {"SAT", "GOS"})
        self.assertEqual(RawSongTally.objects.filter(station="national_fm").count(), 0)
        song = self.verify_song()
        today = timezone.localdate()
        self.assertEqual(counts_for_show(self.saturday, today, today), {song.pk: 1})
        self.assertEqual(counts_for_show(self.gospel, today, today), {song.pk: 1})
        for show in (self.saturday, self.gospel):
            result = self.client.get("/api/chart/today", {"show_id": show.pk})
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json()["total_votes"], 1)
            self.assertEqual(result.json()["top100"][0]["count"], 1)

    def test_unknown_and_closed_codes_are_held_without_counting(self):
        self.assertEqual(self.bird("UNKNOWN River Artist - Morning Song", "unknown").status_code, 202)
        self.gospel.manual_open = False
        self.gospel.save()
        self.assertEqual(self.bird("GOS River Artist - Morning Song", "closed").status_code, 202)
        self.assertEqual(UnassignedVote.objects.count(), 2)
        self.assertEqual(InboundEvent.objects.count(), 0)
        self.assertFalse(RawVote.objects.exists())
        self.assertEqual(
            set(UnassignedVote.objects.values_list("reason", flat=True)),
            {"missing_or_unknown_code", "show_closed"},
        )

    def test_unassigned_manual_assignment_only_to_open_station_show(self):
        self.bird("WRONG River Artist - Morning Song", "wrong-code")
        unresolved = UnassignedVote.objects.get()
        response = self.post("unassigned/assign", {
            "id": unresolved.pk, "show_id": self.saturday.pk,
            "vote_text": "River Artist - Morning Song",
        })
        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(unresolved.__class__.objects.get(pk=unresolved.pk).state,
                         "assigned")
        self.consume()
        song = self.verify_song()
        today = timezone.localdate()
        self.assertEqual(counts_for_show(self.saturday, today, today), {song.pk: 1})
        self.assertEqual(self.post("unassigned/assign", {
            "id": unresolved.pk, "show_id": self.saturday.pk,
            "vote_text": "River Artist - Morning Song",
        }).status_code, 404)

    def test_code_optional_during_safe_rollout_then_required(self):
        StationVotingConfig.objects.filter(station="national_fm").update(require_codes=False)
        self.assertEqual(self.bird("River Artist - Morning Song", "legacy").status_code, 202)
        event = InboundEvent.objects.get()
        self.assertEqual(event.chart_show.kind, "legacy")
        self.assertEqual(event.vote_text, "River Artist - Morning Song")
        self.assertEqual(self.post("shows/routing", {"require_codes": True}).status_code, 200)
        self.assertEqual(self.bird("River Artist - Morning Song", "missing").status_code, 202)
        self.assertEqual(UnassignedVote.objects.count(), 1)
        self.assertEqual(InboundEvent.objects.count(), 1)

    def test_separate_station_can_reuse_code_but_cannot_cross_station(self):
        power = ChartShow.objects.create(
            station="power_fm", name="Power Saturday", code="SAT",
            is_enabled=True, schedule_mode="manual", manual_open=True,
        )
        StationVotingConfig.objects.filter(station="power_fm").update(require_codes=True)
        self.assertEqual(self.bird(
            "SAT River Artist - Morning Song", "power", "+263790563340"
        ).status_code, 202)
        self.assertEqual(InboundEvent.objects.get().chart_show_id, power.pk)
        self.assertEqual(InboundEvent.objects.get().station, "power_fm")
        self.assertNotEqual(self.saturday.pk, power.pk)

    def test_station_user_cannot_edit_or_view_other_station_show(self):
        reader = get_user_model().objects.create_user(
            username="chart-reader", password="Only-For-Testing-819!"
        )
        AccountProfile.objects.update_or_create(
            user=reader, defaults={"station": "power_fm"}
        )
        client = Client()
        client.force_login(reader)
        self.assertEqual(
            client.get("/api/chart/today", {"show_id": self.saturday.pk}).status_code,
            400,
        )
        self.assertEqual(
            client.post("/api/workspace/shows/save", {
                "name": "Hacked", "code": "HACK"
            }, content_type="application/json").status_code, 403,
        )

    def test_edit_show_code_retains_old_vote_and_archive(self):
        self.assertEqual(self.bird("SAT River Artist - Morning Song", "before").status_code, 202)
        self.consume()
        self.verify_song()
        today = timezone.localdate()
        # Archiving an existing completed edition must never be overwritten.
        chart_date = today - timedelta(days=(today.weekday() - 5) % 7)
        # Record vote on the selected broadcast day in the programme.
        VotingService("whatsapp", "other-listener", "national_fm").handle_incoming_text(
            "River Artist - Morning Song", vote_date=chart_date,
            chart_show=self.saturday, submitted_code="SAT",
        )
        self.assertTrue(run_one(VoteJob))
        chart, created = publish_edition(
            "national_fm", chart_date, self.admin, 20,
            chart_show=self.saturday,
        )
        self.assertTrue(created)
        self.assertEqual(chart.chart_show_id, self.saturday.pk)
        old_rank = list(chart.entries.values_list("rank", "title", "vote_count"))
        show = self.saturday
        show.name = "Saturday Music Countdown"
        show.code = "WEEK"
        show.save()
        chart.refresh_from_db()
        self.assertEqual(chart.chart_show_id, show.pk)
        self.assertEqual(list(chart.entries.values_list("rank", "title", "vote_count")), old_rank)
        self.assertEqual(
            route_vote("national_fm", "WEEK River Artist - Morning Song")[0].pk,
            show.pk,
        )
        self.assertEqual(
            route_vote("national_fm", "SAT River Artist - Morning Song")[3],
            "missing_or_unknown_code",
        )

    def test_daily_limit_applies_per_show_not_across_shows(self):
        self.saturday.daily_limit = 1
        self.saturday.save()
        self.gospel.daily_limit = 1
        self.gospel.save()
        svc = VotingService("whatsapp", "same-listener", "national_fm")
        self.assertIn("Submission received", svc.handle_incoming_text(
            "River Artist - Morning Song", chart_show=self.saturday
        ))
        self.assertIn("Submission received", svc.handle_incoming_text(
            "River Artist - Morning Song", chart_show=self.gospel
        ))
        self.assertIn("used all", svc.handle_incoming_text(
            "Other Artist - Different Song", chart_show=self.saturday
        ))
        self.assertEqual(RawVote.objects.count(), 2)

    def test_retried_bad_code_remains_unassigned_after_admin_changes_policy(self):
        self.assertEqual(
            self.bird("BADCODE River Artist - Morning Song", "immutable").status_code, 202
        )
        self.assertEqual(UnassignedVote.objects.count(), 1)
        StationVotingConfig.objects.filter(station="national_fm").update(
            require_codes=False
        )
        self.assertEqual(
            self.bird("BADCODE River Artist - Morning Song", "immutable").status_code, 200
        )
        self.assertEqual(UnassignedVote.objects.count(), 1)
        self.assertFalse(InboundEvent.objects.exists())

    def test_worker_uses_receipt_time_show_even_when_show_is_later_paused(self):
        self.assertEqual(
            self.bird("SAT River Artist - Morning Song", "before-pause").status_code, 202
        )
        self.saturday.is_enabled = False
        self.saturday.save(update_fields=["is_enabled"])
        self.consume()
        vote = RawVote.objects.get()
        self.assertEqual(vote.chart_show_id, self.saturday.pk)
        self.assertEqual(vote.submitted_code, "SAT")
        self.assertEqual(OutboundMessage.objects.count(), 0)

    def test_annual_campaign_only_counts_annual_votes(self):
        from datetime import datetime
        from unittest.mock import patch

        self.assertEqual(
            self.bird("SAT River Artist - Morning Song", "weekly-vote").status_code, 202
        )
        self.consume()
        self.verify_song()
        annual = ChartShow.objects.get(
            station="national_fm", code="EOY"
        )
        annual.campaign_start = timezone.make_aware(datetime(2026, 12, 1, 0, 0))
        annual.campaign_end = timezone.make_aware(datetime(2027, 1, 1, 0, 0))
        annual.save()
        # Programme-specific year-end votes, recorded on a December day.
        VotingService("whatsapp", "annual-listener", "national_fm").handle_incoming_text(
            "River Artist - Morning Song", vote_date=date(2026, 12, 20),
            chart_show=annual, submitted_code="EOY",
        )
        self.assertTrue(run_one(VoteJob))
        with patch("apps.charts.publishing.timezone.localdate", return_value=date(2026, 12, 31)):
            chart, created = publish_edition(
                "national_fm", date(2026, 12, 31), self.admin,
                50, kind="year_end", chart_show=annual,
            )
        self.assertTrue(created)
        self.assertEqual(chart.total_votes, 1)
        self.assertEqual(chart.entries.first().vote_count, 1)
        self.assertEqual(chart.chart_show_id, annual.pk)
        self.assertEqual(chart.chart_size, 50)
