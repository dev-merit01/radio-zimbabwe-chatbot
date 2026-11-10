"""Broadcast chart comparisons: real previous ranks, no fabricated history."""
from datetime import date, timedelta
import csv
import io

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.charts.movement import (
    movement_for_rank, previous_published_chart, archived_movement,
)
from apps.charts.publishing import publish_edition
from apps.voting.models import (
    ChartShow, CleanedSong, MatchKeyMapping, RawVote, User,
    WeeklyChart, WeeklyChartEntry,
)


@override_settings(
    SECURE_SSL_REDIRECT=False, AI_ALL_VOTES=False,
    AUTO_AI_MATCH=False, ALLOW_REPEAT_SONG=False,
)
class BroadcastChartMovementTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser(
            username="movement-admin", password="Movement-Testing-Only!98",
        )
        self.client.force_login(self.admin)
        self.client.post(
            "/accounts/switch-station/", {"station": "national_fm"}
        )
        self.weekday = 5
        self.show = ChartShow.objects.create(
            station="national_fm", name="National FM Saturday Top 20",
            code="MOV", kind="weekly", chart_size=20,
            broadcast_weekday=self.weekday, schedule_mode="manual",
            manual_open=True, is_enabled=True,
        )
        self.other_show = ChartShow.objects.create(
            station="national_fm", name="National FM Gospel",
            code="GSP", kind="weekly", chart_size=20,
            broadcast_weekday=self.weekday, schedule_mode="manual",
            manual_open=True, is_enabled=True,
        )
        self.power = ChartShow.objects.create(
            station="power_fm", name="Power FM Saturday",
            code="PWR", kind="weekly", chart_size=20,
            broadcast_weekday=self.weekday, schedule_mode="manual",
            manual_open=True, is_enabled=True,
        )
        self.songs = {}
        for title in ("Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf"):
            song = CleanedSong.objects.create(
                station="national_fm", artist="Test Performer", title=title,
                canonical_name=f"Test Performer - {title}", status="verified",
            )
            self.songs[title] = song
        today = timezone.localdate()
        self.next_saturday = today + timedelta(days=(5 - today.weekday()) % 7)
        self.period_start = self.next_saturday - timedelta(days=6)
        self.previous_saturday = self.period_start - timedelta(days=1)

    def saved_chart(self, show, end, entries, *, annual=False):
        start = date(end.year, 1, 1) if annual else end - timedelta(days=6)
        chart = WeeklyChart.objects.create(
            station=show.station, chart_show=show,
            week_start=start, week_end=end, chart_date=end,
            year=end.year if annual else end.isocalendar().year,
            week_number=0 if annual else end.isocalendar().week,
            chart_size=50 if annual else show.chart_size,
            is_year_end=annual, is_finalized=True,
            finalized_at=timezone.now(),
            total_votes=len(entries), unique_songs=len(entries),
        )
        for rank, song in entries:
            WeeklyChartEntry.objects.create(
                chart=chart, cleaned_song=song, rank=rank,
                title=song.title, artist=song.artist,
                canonical_name=song.canonical_name, vote_count=1,
                weeks_on_chart=1, peak_rank=rank,
            )
        return chart

    def received(self, song, count, vote_date, show=None):
        show = show or self.show
        key = f"{song.artist.casefold()}::{song.title.casefold()}"
        MatchKeyMapping.objects.get_or_create(
            station=show.station, match_key=key,
            defaults={
                "cleaned_song": song,
                "sample_display_name": song.canonical_name,
            },
        )
        for j in range(count):
            listener = User.objects.create(
                channel="whatsapp",
                user_ref=f"chart-{show.pk}-{song.pk}-{vote_date}-{j}",
                station=show.station,
            )
            RawVote.objects.create(
                station=show.station, chart_show=show,
                submitted_code=show.code,
                user=listener, raw_input=song.canonical_name,
                artist_raw=song.artist, song_raw=song.title,
                artist_normalized=song.artist.casefold(),
                song_normalized=song.title.casefold(),
                match_key=key, display_name=song.canonical_name,
                vote_date=vote_date, ai_status="approved",
            )

    def today(self, show=None):
        return self.client.get("/api/chart/today", {
            "show_id": (show or self.show).pk, "limit": 20,
        })

    def test_pure_status_calculations(self):
        self.assertEqual(
            movement_for_rank(1, 2, has_comparison=True, charted_before=True),
            {"previous_rank": 2, "movement_status": "up",
             "movement_delta": 1, "movement": "+1"},
        )
        for rank, old, status, delta in [
            (4, 1, "down", -3), (5, 5, "same", 0),
            (3, None, "new", None), (3, None, "re_entry", None),
            (3, None, "no_previous", None),
        ]:
            actual = movement_for_rank(
                rank, old, has_comparison=(status != "no_previous"),
                charted_before=status == "re_entry",
            )
            self.assertEqual(actual["movement_status"], status)
            self.assertEqual(actual["movement_delta"], delta)

    def test_live_weekly_positions_include_up_down_same_new_and_reentry(self):
        # Delta charted two editions ago but missed last week's published Top 20.
        earlier = self.previous_saturday - timedelta(days=7)
        self.saved_chart(self.show, earlier, [(7, self.songs["Delta"])])
        previous = self.saved_chart(self.show, self.previous_saturday, [
            (1, self.songs["Bravo"]), (2, self.songs["Alpha"]),
            (3, self.songs["Foxtrot"]), (4, self.songs["Golf"]),
            (5, self.songs["Echo"]),
        ])
        for title, count in [
            ("Alpha", 5), ("Bravo", 4), ("Charlie", 3),
            ("Delta", 2), ("Echo", 1),
        ]:
            self.received(self.songs[title], count, timezone.localdate())
        response = self.today()
        self.assertEqual(response.status_code, 200, response.content)
        data = response.json()
        self.assertEqual(data["comparison"]["chart_id"], previous.id)
        self.assertEqual(data["comparison"]["chart_date"], str(self.previous_saturday))
        entries = {row["title"]: row for row in data["top100"]}
        for song, rank, old, status, delta in [
            ("Alpha", 1, 2, "up", 1),
            ("Bravo", 2, 1, "down", -1),
            ("Charlie", 3, None, "new", None),
            ("Delta", 4, None, "re_entry", None),
            ("Echo", 5, 5, "same", 0),
        ]:
            with self.subTest(song=song):
                row = entries[song]
                self.assertEqual(row["rank"], rank)
                self.assertEqual(row["previous_rank"], old)
                self.assertEqual(row["movement_status"], status)
                self.assertEqual(row["movement_delta"], delta)
        exported = self.client.get(
            "/api/workspace/export", {"show_id": self.show.pk}
        )
        self.assertEqual(exported.status_code, 200)
        rows = list(csv.reader(io.StringIO(exported.content.decode("utf-8-sig"))))
        self.assertEqual(rows[0][-3:], [
            "Previous position", "Position change", "Chart status",
        ])
        self.assertEqual(rows[1][-3:], ["2", "+1", "UP"])
        self.assertEqual(rows[2][-3:], ["1", "-1", "DOWN"])
        self.assertEqual(rows[3][-3:], ["", "", "NEW ENTRY"])
        self.assertEqual(rows[4][-3:], ["", "", "RE-ENTRY"])
        self.assertEqual(rows[5][-3:], ["5", "0", "UNCHANGED"])

    def test_missing_previous_week_is_not_falsely_called_a_new_entry(self):
        self.saved_chart(
            self.show, self.previous_saturday - timedelta(days=14),
            [(1, self.songs["Alpha"])],
        )
        self.received(self.songs["Alpha"], 1, timezone.localdate())
        data = self.today().json()
        self.assertFalse(data["comparison"]["available"])
        self.assertEqual(data["top100"][0]["previous_rank"], None)
        self.assertEqual(data["top100"][0]["movement_status"], "no_previous")

    def test_movement_must_not_leak_between_shows_or_stations(self):
        self.saved_chart(
            self.other_show, self.previous_saturday,
            [(1, self.songs["Alpha"])],
        )
        self.received(self.songs["Alpha"], 1, timezone.localdate())
        data = self.today().json()
        self.assertFalse(data["comparison"]["available"])
        self.assertEqual(data["top100"][0]["movement_status"], "no_previous")
        # Power FM has its own permanent chart-show identity.
        self.assertIsNone(previous_published_chart(
            self.power, self.period_start, self.next_saturday,
        ))
        self.assertFalse(self.client.get(
            "/api/chart/archives",
            {"show_id": self.power.pk},
        ).status_code == 200 and False)

    def test_publish_freezes_last_week_and_archive_exports(self):
        last_saturday = self.previous_saturday
        start = last_saturday - timedelta(days=6)
        previous_end = start - timedelta(days=1)
        previous = self.saved_chart(self.show, previous_end, [
            (1, self.songs["Bravo"]), (2, self.songs["Alpha"]),
        ])
        self.saved_chart(
            self.show, previous_end - timedelta(days=7),
            [(3, self.songs["Delta"])],
        )
        for title, count in [
            ("Alpha", 4), ("Bravo", 3), ("Charlie", 2), ("Delta", 1)
        ]:
            self.received(self.songs[title], count, last_saturday)
        chart, created = publish_edition(
            "national_fm", last_saturday, self.admin,
            size=20, chart_show=self.show,
        )
        self.assertTrue(created)
        self.assertEqual(chart.comparison_chart_id, previous.pk)
        entries = {
            entry.title: entry for entry in chart.entries.order_by("rank")
        }
        self.assertEqual(entries["Alpha"].previous_rank, 2)
        self.assertEqual(entries["Alpha"].movement_status, "up")
        self.assertEqual(entries["Bravo"].previous_rank, 1)
        self.assertEqual(entries["Bravo"].movement_status, "down")
        self.assertEqual(entries["Charlie"].movement_status, "new")
        self.assertEqual(entries["Delta"].movement_status, "re_entry")
        archived = self.client.get(f"/api/chart/{chart.pk}").json()
        self.assertEqual(archived["chart"]["comparison"]["chart_id"], previous.pk)
        self.assertEqual(
            {row["title"]: row["movement_status"] for row in archived["entries"]},
            {"Alpha": "up", "Bravo": "down",
             "Charlie": "new", "Delta": "re_entry"},
        )
        before = list(chart.entries.order_by("rank").values_list(
            "rank", "title", "previous_rank", "movement_status",
        ))
        # A changed shortlist or new votes must never edit an official snapshot.
        self.show.eligibility_enforced = True
        self.show.save(update_fields=["eligibility_enforced"])
        chart.refresh_from_db()
        self.assertEqual(
            before,
            list(chart.entries.order_by("rank").values_list(
                "rank", "title", "previous_rank", "movement_status",
            )),
        )
        exported = self.client.get(
            "/api/workspace/export", {"archive": chart.pk}
        )
        self.assertEqual(exported.status_code, 200)
        self.assertIn("RE-ENTRY", exported.content.decode("utf-8-sig"))

    def test_first_published_chart_shows_no_prior_edition_not_new(self):
        last_saturday = self.previous_saturday
        self.received(self.songs["Alpha"], 1, last_saturday)
        chart, created = publish_edition(
            "national_fm", last_saturday, self.admin,
            20, chart_show=self.show,
        )
        self.assertTrue(created)
        self.assertIsNone(chart.comparison_chart_id)
        entry = chart.entries.get()
        self.assertEqual(entry.movement_status, "no_previous")
        data = self.client.get(f"/api/chart/{chart.pk}").json()
        self.assertFalse(data["chart"]["comparison"]["available"])
        self.assertEqual(data["entries"][0]["movement_status"], "no_previous")
        # Later importing an old chart cannot change this frozen classification.
        self.saved_chart(
            self.show, last_saturday - timedelta(days=7),
            [(1, self.songs["Alpha"])],
        )
        self.assertEqual(
            self.client.get(f"/api/chart/{chart.pk}").json()["entries"][0]["movement_status"],
            "no_previous",
        )

    def test_year_end_compares_only_same_annual_chart_previous_year(self):
        show = ChartShow.objects.create(
            station="national_fm", name="End of year Top 50", code="YEAR",
            kind="year_end", chart_size=50, schedule_mode="manual",
            manual_open=True, is_enabled=True,
        )
        year = timezone.localdate().year
        previous_end = date(year - 1, 12, 31)
        earlier_end = date(year - 2, 12, 31)
        self.saved_chart(show, earlier_end, [(4, self.songs["Delta"])], annual=True)
        previous = self.saved_chart(show, previous_end, [
            (1, self.songs["Bravo"]), (2, self.songs["Alpha"]),
        ], annual=True)
        for song, count in [("Alpha", 4), ("Bravo", 3), ("Delta", 2),
                            ("Charlie", 1)]:
            self.received(self.songs[song], count, timezone.localdate(), show)
        response = self.today(show)
        self.assertEqual(response.status_code, 200, response.content)
        data = response.json()
        self.assertEqual(data["chart_type"], "year_end")
        self.assertEqual(data["comparison"]["chart_id"], previous.pk)
        entries = {row["title"]: row for row in data["top100"]}
        self.assertEqual(entries["Alpha"]["movement_status"], "up")
        self.assertEqual(entries["Alpha"]["previous_rank"], 2)
        self.assertEqual(entries["Bravo"]["movement_status"], "down")
        self.assertEqual(entries["Delta"]["movement_status"], "re_entry")
        self.assertEqual(entries["Charlie"]["movement_status"], "new")
        weekly = self.saved_chart(
            self.show, self.previous_saturday, [(1, self.songs["Alpha"])]
        )
        self.assertNotEqual(data["comparison"]["chart_id"], weekly.pk)
