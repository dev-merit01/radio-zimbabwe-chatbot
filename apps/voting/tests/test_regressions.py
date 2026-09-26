"""Regression coverage for intake, delivery, reconciliation and chart boundaries."""

from datetime import date, datetime, timedelta, timezone as dt_timezone
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.db import IntegrityError
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.charts.publishing import publish_week
from apps.voting.models import (
    CleanedSong,
    CleanedSongTally,
    InboundEvent,
    MatchKeyMapping,
    OutboundMessage,
    RawSongTally,
    RawVote,
    VoteJob,
)
from apps.voting.pipeline import rebuild_tallies
from apps.voting.services import VotingService
from apps.voting.worker import run_one


@override_settings(
    SECURE_SSL_REDIRECT=False,
    AUTO_AI_MATCH=False,
    ALLOW_REPEAT_SONG=False,
    TELEGRAM_WEBHOOK_SECRET="test-secret",
)
class TestPipelineRegressions(TestCase):
    def song(self, artist="Example Artist", title="Example Song", **kwargs):
        return CleanedSong.objects.create(
            artist=artist,
            title=title,
            canonical_name=f"{artist} - {title}",
            status="verified",
            **kwargs,
        )

    def test_known_alias_and_unmapped_canonical_vote_cannot_count_twice(self):
        song = self.song()
        MatchKeyMapping.objects.create(
            station="radio_zimbabwe",
            match_key="example artist::alternate title",
            cleaned_song=song,
        )
        for listener, titles in [
            ("one", ["Example Song", "Alternate Title"]),
            ("two", ["Alternate Title", "Example Song"]),
        ]:
            service = VotingService("telegram", listener)
            self.assertIn(
                "Vote recorded!",
                service.handle_incoming_text(f"Example Artist - {titles[0]}"),
            )
            self.assertIn(
                "already voted",
                service.handle_incoming_text(f"Example Artist - {titles[1]}"),
            )
        self.assertEqual(RawVote.objects.count(), 2)
        self.assertEqual(VoteJob.objects.count(), 2)

    @override_settings(ALLOW_REPEAT_SONG=True)
    def test_repeat_policy_still_allows_known_aliases(self):
        song = self.song()
        MatchKeyMapping.objects.create(
            station="radio_zimbabwe",
            match_key="example artist::alternate title",
            cleaned_song=song,
        )
        service = VotingService("telegram", "repeat-listener")
        for title in ["Example Song", "Alternate Title", "Example Song"]:
            self.assertIn(
                "Vote recorded!",
                service.handle_incoming_text(f"Example Artist - {title}"),
            )
        self.assertEqual(RawVote.objects.count(), 3)

    def test_aliases_from_another_station_do_not_reject_votes(self):
        song = self.song(station="national_fm")
        MatchKeyMapping.objects.create(
            station="national_fm",
            match_key="example artist::alternate title",
            cleaned_song=song,
        )
        service = VotingService("telegram", "station-listener")
        for title in ["Example Song", "Alternate Title"]:
            self.assertIn(
                "Vote recorded!",
                service.handle_incoming_text(f"Example Artist - {title}"),
            )
        self.assertEqual(RawVote.objects.count(), 2)

    def test_malformed_successful_delivery_is_not_retried(self):
        event = InboundEvent.objects.create(
            provider="telegram",
            station="radio_zimbabwe",
            message_id="delivery",
            sender="123",
            text="hello",
        )
        for payload in [
            {"ok": True, "result": None},
            {"ok": True},
            {"ok": True, "result": {"message_id": []}},
            {"ok": True, "result": {"message_id": True}},
        ]:
            with self.subTest(payload=payload):
                OutboundMessage.objects.all().delete()
                reply = OutboundMessage.objects.create(event=event, text="response")
                client = Mock()
                client.send_text.return_value = payload
                with patch("apps.bot.telegram_client.get_client", return_value=client):
                    self.assertTrue(run_one(OutboundMessage))
                    reply.refresh_from_db()
                    self.assertEqual(reply.state, "uncertain")
                    self.assertFalse(run_one(OutboundMessage))
                client.send_text.assert_called_once()

    def test_valid_delivery_records_message_id(self):
        event = InboundEvent.objects.create(
            provider="telegram",
            station="radio_zimbabwe",
            message_id="delivery",
            sender="123",
        )
        reply = OutboundMessage.objects.create(event=event, text="response")
        client = Mock()
        client.send_text.return_value = {"ok": True, "result": {"message_id": 42}}
        with patch("apps.bot.telegram_client.get_client", return_value=client):
            run_one(OutboundMessage)
        reply.refresh_from_db()
        self.assertEqual((reply.state, reply.provider_message_id), ("done", "42"))

    def test_non_ascii_webhook_secret_header_is_rejected(self):
        response = self.client.post(
            "/webhook/telegram/",
            {},
            content_type="application/json",
            HTTP_X_TELEGRAM_BOT_API_SECRET_TOKEN="invalid-\u00e9",
        )
        self.assertEqual(response.status_code, 403)

    def test_structured_telegram_identifiers_are_rejected(self):
        for message_id, sender in [([], 123), (123, {}), (True, 123), (123, True)]:
            response = self.client.post(
                "/webhook/telegram/",
                {
                    "update_id": message_id,
                    "message": {
                        "chat": {"id": sender, "type": "private"},
                        "text": "Artist - Song",
                    },
                },
                content_type="application/json",
                HTTP_X_TELEGRAM_BOT_API_SECRET_TOKEN="test-secret",
            )
            self.assertEqual(response.status_code, 400)
        self.assertEqual(InboundEvent.objects.count(), 0)

    def test_chart_year_uses_iso_week_year(self):
        user = get_user_model().objects.create_superuser("chart-reader")
        self.client.force_login(user)
        with patch("django.utils.timezone.localdate", return_value=date(2027, 1, 1)):
            data = self.client.get("/api/chart/today").json()
            self.assertEqual((data["year"], data["week_number"]), (2026, 53))
            self.assertEqual(
                self.client.get("/api/chart/archives").json()["year"], 2026
            )

    def test_backfilled_chart_peak_does_not_include_future_weeks(self):
        today = timezone.localdate()
        monday = today - timedelta(days=today.weekday() + 21)
        song = self.song()
        other = self.song(artist="Other Artist")
        for s, n in [(song, 1), (other, 2)]:
            key = f"key-{s.pk}"
            MatchKeyMapping.objects.create(
                station="radio_zimbabwe", match_key=key, cleaned_song=s
            )
            RawSongTally.objects.create(
                station="radio_zimbabwe", date=monday, match_key=key, count=n
            )
        RawSongTally.objects.create(
            station="radio_zimbabwe",
            date=monday + timedelta(days=7),
            match_key=f"key-{song.pk}",
            count=3,
        )
        publish_week("radio_zimbabwe", monday + timedelta(days=7), None)
        chart, _ = publish_week("radio_zimbabwe", monday, None)
        entry = chart.entries.get(cleaned_song=song)
        self.assertEqual((entry.rank, entry.peak_rank), (2, 2))

    def test_rebuild_scopes_dates_stations_and_sums_aliases(self):
        song = self.song()
        other = self.song(artist="Other Artist")
        day = date(2026, 8, 3)
        for key, n in [("alias-a", 2), ("alias-b", 3)]:
            MatchKeyMapping.objects.create(
                station="radio_zimbabwe", match_key=key, cleaned_song=song
            )
            RawSongTally.objects.create(
                station="radio_zimbabwe", date=day, match_key=key, count=n
            )
            RawSongTally.objects.create(
                station="national_fm", date=day, match_key=key, count=100
            )
        stale = CleanedSongTally.objects.create(
            station="radio_zimbabwe", date=day, cleaned_song=song, count=999
        )
        preserved = CleanedSongTally.objects.create(
            station="radio_zimbabwe", date=day, cleaned_song=other, count=7
        )
        past = CleanedSongTally.objects.create(
            station="radio_zimbabwe",
            date=day - timedelta(days=1),
            cleaned_song=song,
            count=9,
        )
        for _ in range(2):
            rebuild_tallies("radio_zimbabwe", song_ids=[song.pk], date_range=(day, day))
            self.assertEqual(
                CleanedSongTally.objects.get(cleaned_song=song, date=day).count, 5
            )
        self.assertFalse(CleanedSongTally.objects.filter(pk=stale.pk).exists())
        preserved.refresh_from_db()
        past.refresh_from_db()
        self.assertEqual((preserved.count, past.count), (7, 9))
        song.status = "rejected"
        song.save()
        rebuild_tallies("radio_zimbabwe", song_ids=[song.pk], date_range=(day, day))
        self.assertFalse(
            CleanedSongTally.objects.filter(cleaned_song=song, date=day).exists()
        )

    def test_intake_uses_local_receipt_day_when_worker_runs_later(self):
        event = InboundEvent.objects.create(
            provider="telegram",
            station="radio_zimbabwe",
            message_id="late",
            sender="123",
            text="Example Artist - Example Song",
            received_at=datetime(2026, 8, 2, 22, 30, tzinfo=dt_timezone.utc),
        )
        run_one(InboundEvent)
        self.assertEqual(RawVote.objects.get().vote_date, date(2026, 8, 3))
        event.refresh_from_db()
        self.assertEqual(event.state, "done")

    def test_rebuild_batches_and_rolls_back_a_partial_failure(self):
        song = self.song()
        MatchKeyMapping.objects.create(
            station="radio_zimbabwe", match_key="batch-key", cleaned_song=song
        )
        day = date(2024, 1, 1)
        RawSongTally.objects.bulk_create(
            [
                RawSongTally(
                    station="radio_zimbabwe",
                    date=day + timedelta(days=i),
                    match_key="batch-key",
                    count=2,
                )
                for i in range(501)
            ]
        )
        original = CleanedSongTally.objects.create(
            station="radio_zimbabwe", date=day, cleaned_song=song, count=99
        )
        bulk_create = CleanedSongTally.objects.bulk_create
        sizes = []

        def fail_second_batch(batch, **kwargs):
            sizes.append(len(batch))
            if len(sizes) == 2:
                raise IntegrityError("simulated insert failure")
            return bulk_create(batch, **kwargs)

        with patch.object(
            CleanedSongTally.objects, "bulk_create", side_effect=fail_second_batch
        ):
            with self.assertRaises(IntegrityError):
                rebuild_tallies("radio_zimbabwe")
        original.refresh_from_db()
        self.assertEqual(original.count, 99)
        self.assertEqual(CleanedSongTally.objects.count(), 1)
        self.assertEqual(sizes, [500, 1])
        rebuild_tallies("radio_zimbabwe")
        self.assertEqual(CleanedSongTally.objects.count(), 501)
        self.assertFalse(CleanedSongTally.objects.exclude(count=2).exists())
