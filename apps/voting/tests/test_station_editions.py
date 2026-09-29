from datetime import date
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.cache import cache
from django.test import TestCase, Client, override_settings

from apps.accounts.models import AccountProfile
from apps.charts.periods import week_dates
from apps.charts.publishing import publish_edition
from apps.voting.models import (
    CleanedSong,
    CleanedSongTally,
    MatchKeyMapping,
    RawSongTally,
    InboundEvent,
    WeeklyChart,
)
from apps.voting.pipeline import review_song, rebuild_tallies


@override_settings(SECURE_SSL_REDIRECT=False)
class StationEditionTests(TestCase):
    def setUp(self):
        cache.clear()
        self.admin = get_user_model().objects.create_superuser(
            "edition-admin", password="Admin-example-492!"
        )
        self.reader = get_user_model().objects.create_user(
            "edition-reader", password="Reader-example-482!"
        )
        AccountProfile.objects.create(user=self.reader, station="radio_zimbabwe")
        self.client.force_login(self.admin)

    def post(self, path, data):
        return self.client.post(
            "/api/workspace/" + path, data, content_type="application/json"
        )

    def tally(self, station, day, count=1, title="Song"):
        song, _ = CleanedSong.objects.get_or_create(
            station=station,
            canonical_name="Artist - " + title,
            defaults={"artist": "Artist", "title": title, "status": "verified"},
        )
        key = "artist::" + title.lower()
        MatchKeyMapping.objects.get_or_create(
            station=station, match_key=key, defaults={"cleaned_song": song}
        )
        RawSongTally.objects.update_or_create(
            station=station, date=day, match_key=key, defaults={"count": count}
        )
        rebuild_tallies(station)
        return song

    def test_register_approve_sign_in_and_station_scope(self):
        self.client.logout()
        data = {
            "username": "new-member",
            "first_name": "New",
            "last_name": "Member",
            "station": "national_fm",
            "password": "Secure-unique-739!",
            "confirm_password": "Secure-unique-739!",
            "is_superuser": "true",
        }
        self.assertEqual(self.client.get("/accounts/register/").status_code, 200)
        self.assertEqual(self.client.post("/accounts/register/", data).status_code, 302)
        user = get_user_model().objects.get(username="new-member")
        self.assertFalse(user.is_active)
        self.assertFalse(user.is_staff)
        self.assertFalse(user.is_superuser)
        self.assertEqual(user.profile.station, "national_fm")
        self.assertFalse(
            self.client.login(username="new-member", password=data["password"])
        )
        self.client.force_login(self.reader)
        self.assertEqual(self.post(f"accounts/{user.pk}/approve", {}).status_code, 403)
        self.client.force_login(self.admin)
        self.assertEqual(self.post(f"accounts/{user.pk}/approve", {}).status_code, 200)
        self.client.logout()
        self.assertTrue(
            self.client.login(username="new-member", password=data["password"])
        )
        self.assertEqual(
            self.client.get("/api/workspace/overview").json()["station"], "national_fm"
        )
        self.assertFalse(
            self.client.get("/api/workspace/overview").json()["can_review"]
        )
        self.assertEqual(
            self.client.get("/api/workspace/administration").status_code, 403
        )

    def test_registration_validates_password_station_and_csrf(self):
        self.client.logout()
        data = {
            "username": "new-member",
            "first_name": "New",
            "last_name": "Member",
            "station": "bad",
            "password": "123",
            "confirm_password": "123",
        }
        response = self.client.post("/accounts/register/", data)
        self.assertContains(response, "Select a valid choice")
        self.assertContains(response, "too short")
        self.assertFalse(
            get_user_model().objects.filter(username="new-member").exists()
        )
        self.assertEqual(
            Client(enforce_csrf_checks=True)
            .post("/accounts/register/", data)
            .status_code,
            403,
        )

    def test_granted_django_permissions_do_not_enable_admin_song_actions(self):
        song = self.tally("radio_zimbabwe", date(2026, 9, 19))
        self.reader.user_permissions.add(
            *Permission.objects.filter(
                codename__in=[
                    "change_cleanedsong",
                    "add_cleanedsong",
                    "add_weeklychart",
                ]
            )
        )
        self.client.force_login(self.reader)
        for action in ["edit", "verified", "rejected", "merge"]:
            with self.subTest(action=action):
                self.assertEqual(
                    self.post(
                        f"songs/{song.pk}/review",
                        {
                            "action": action,
                            "artist": "X",
                            "title": "Y",
                            "target_id": song.pk,
                        },
                    ).status_code,
                    403,
                )
        self.assertEqual(
            self.post("songs/add", {"artist": "New", "title": "Song"}).status_code, 403
        )
        self.assertEqual(
            self.post("publish", {"chart_date": "2026-09-19"}).status_code, 403
        )
        overview = self.client.get("/api/workspace/overview").json()
        self.assertFalse(
            overview["can_review"] or overview["can_add"] or overview["can_publish"]
        )

    def test_station_catalogue_and_moderation_do_not_leak(self):
        local = self.tally("radio_zimbabwe", date(2026, 9, 19), 3)
        other = self.tally("national_fm", date(2026, 9, 19), 99)
        self.assertNotEqual(local.pk, other.pk)
        self.assertEqual(
            self.post(f"songs/{other.pk}/review", {"action": "rejected"}).status_code,
            404,
        )
        self.assertEqual(
            self.post(
                f"songs/{local.pk}/review", {"action": "merge", "target_id": other.pk}
            ).status_code,
            404,
        )
        self.assertEqual(
            self.post(
                f"songs/{local.pk}/review",
                {"action": "edit", "artist": "Local", "title": "Edited"},
            ).status_code,
            200,
        )
        other.refresh_from_db()
        self.assertEqual((other.title, other.status), ("Song", "verified"))
        items = self.client.get(
            "/api/workspace/songs?status=all&station=national_fm"
        ).json()["items"]
        self.assertEqual([s["id"] for s in items], [local.pk])
        self.assertEqual(CleanedSongTally.objects.get(station="national_fm").count, 99)

    @patch("django.utils.timezone.localdate", return_value=date(2026, 9, 19))
    def test_saturday_snapshot_period_idempotency_and_immutability(self, _):
        song = self.tally("radio_zimbabwe", date(2026, 9, 13), 2)
        self.tally("radio_zimbabwe", date(2026, 9, 19), 3)
        self.tally("radio_zimbabwe", date(2026, 9, 12), 100)
        self.tally("national_fm", date(2026, 9, 19), 99)
        data = {"chart_date": "2026-09-19", "size": 20}
        response = self.post("publish", data)
        self.assertEqual(response.status_code, 200, response.content)
        chart = WeeklyChart.objects.get(pk=response.json()["id"])
        self.assertEqual(
            (chart.week_start, chart.week_end, chart.total_votes),
            (date(2026, 9, 13), date(2026, 9, 19), 5),
        )
        self.assertFalse(self.post("publish", data).json()["created"])
        self.assertEqual(self.post("publish", {**data, "size": 50}).status_code, 400)
        review_song(
            "radio_zimbabwe",
            self.admin,
            song.pk,
            "edit",
            artist="Changed",
            title="Changed",
        )
        review_song("radio_zimbabwe", self.admin, song.pk, "rejected")
        entry = chart.entries.get()
        self.assertEqual((entry.title, entry.vote_count), ("Song", 5))
        self.client.post("/accounts/switch-station/", {"station": "national_fm"})
        self.assertEqual(self.client.get(f"/api/chart/{chart.pk}").status_code, 404)
        self.assertEqual(
            self.client.get(f"/api/workspace/export?archive={chart.pk}").status_code,
            404,
        )
        self.assertEqual(
            self.client.get("/api/chart/archives?year=2026").json()["charts"], []
        )

    @patch("django.utils.timezone.localdate", return_value=date(2026, 12, 26))
    def test_year_end_and_saturday_are_separate_and_annual_period_is_correct(self, _):
        self.tally("radio_zimbabwe", date(2026, 1, 1), 7)
        self.tally("radio_zimbabwe", date(2026, 12, 26), 3)
        self.tally("radio_zimbabwe", date(2025, 12, 31), 100)
        self.tally("radio_zimbabwe", date(2026, 12, 27), 100)
        weekly, _ = publish_edition("radio_zimbabwe", date(2026, 12, 26), self.admin)
        annual, _ = publish_edition(
            "radio_zimbabwe", date(2026, 12, 26), self.admin, 50, "year_end"
        )
        self.assertEqual((weekly.total_votes, annual.total_votes), (3, 10))
        self.assertNotEqual(weekly.pk, annual.pk)
        self.assertTrue(annual.is_year_end)
        self.assertFalse(
            publish_edition(
                "radio_zimbabwe", date(2026, 12, 26), self.admin, 50, "year_end"
            )[1]
        )
        with self.assertRaises(ValueError):
            publish_edition(
                "radio_zimbabwe", date(2026, 12, 25), self.admin, 50, "year_end"
            )
        self.assertEqual(
            len(self.client.get("/api/chart/archives?year=2026").json()["charts"]), 2
        )

    def test_rejects_future_wrong_day_and_invalid_year_end(self):
        with patch("django.utils.timezone.localdate", return_value=date(2026, 9, 19)):
            for data in [
                {"chart_date": "2026-09-26"},
                {"chart_date": "2026-09-18"},
                {"chart_date": "2026-09-19", "size": 100},
                {"chart_date": "2026-09-19", "kind": "year_end", "size": 50},
                {"chart_date": "not-a-date"},
            ]:
                self.assertEqual(self.post("publish", data).status_code, 400)
        with patch("django.utils.timezone.localdate", return_value=date(2026, 12, 31)):
            self.assertEqual(
                self.post(
                    "publish",
                    {"chart_date": "2026-12-31", "kind": "year_end", "size": 20},
                ).status_code,
                400,
            )

    @patch("django.utils.timezone.localdate", return_value=date(2026, 9, 19))
    def test_unfinished_votes_block_publication(self, _):
        self.tally("radio_zimbabwe", date(2026, 9, 19))
        from django.utils import timezone
        from datetime import datetime

        InboundEvent.objects.create(
            provider="manual",
            station="radio_zimbabwe",
            message_id="pending",
            sender="test",
            received_at=timezone.make_aware(datetime(2026, 9, 19, 12)),
        )
        self.assertEqual(
            self.post("publish", {"chart_date": "2026-09-19"}).status_code, 400
        )
        self.assertFalse(WeeklyChart.objects.exists())

    @patch("django.utils.timezone.localdate", return_value=date(2026, 9, 19))
    def test_top_twenty_fifty_and_csv_match(self, _):
        for i in range(55):
            song = CleanedSong.objects.create(
                station="radio_zimbabwe",
                artist="Artist",
                title=f"{i:02}",
                status="verified",
            )
            CleanedSongTally.objects.create(
                station="radio_zimbabwe",
                date=date(2026, 9, 19),
                cleaned_song=song,
                count=i + 1,
            )
            MatchKeyMapping.objects.create(
                station="radio_zimbabwe", cleaned_song=song, match_key=f"key-{i}"
            )
            RawSongTally.objects.create(
                station="radio_zimbabwe",
                date=date(2026, 9, 19),
                match_key=f"key-{i}",
                count=i + 1,
            )
        for size in (20, 50):
            rows = self.client.get(f"/api/chart/today?limit={size}").json()["top100"]
            self.assertEqual(len(rows), size)
            self.assertEqual(rows[0]["title"], "54")
            csv = self.client.get(f"/api/workspace/export?limit={size}").content.decode(
                "utf-8-sig"
            )
            self.assertEqual(len(csv.splitlines()), size + 1)
        archive, _ = publish_edition(
            "radio_zimbabwe", date(2026, 9, 19), self.admin, 20
        )
        self.assertEqual(archive.entries.count(), 20)
        self.assertEqual((archive.total_votes, archive.unique_songs), (1540, 55))
        self.assertEqual(
            week_dates(date(2026, 9, 20)), (date(2026, 9, 20), date(2026, 9, 26))
        )

    def test_publication_requires_csrf(self):
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.admin)
        self.assertEqual(
            client.post(
                "/api/workspace/publish",
                {"chart_date": "2026-09-19"},
                content_type="application/json",
            ).status_code,
            403,
        )
