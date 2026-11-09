"""The uploaded official ZBC lineup is complete, safe to rerun and editable."""
import importlib
import json
from datetime import date

from apps.charts.periods import campaign_local_dates

from django.apps import apps
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.charts.routing import route_vote
from apps.voting.models import ChartShow, RawVote, User


LINEUP = importlib.import_module(
    "apps.voting.migrations.0020_zbc_official_chart_shows"
)


@override_settings(SECURE_SSL_REDIRECT=False)
class ZBCOfficialChartLineupTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.admin = get_user_model().objects.create_superuser(
            username="zbc-lineup-admin", password="lineup-test-password!79"
        )

    def setUp(self):
        self.client.force_login(self.admin)
        self.client.post("/accounts/switch-station/", {"station": "national_fm"})

    def post_show(self, show, **updates):
        from apps.dashboard.show_management import serialize
        data = serialize(show)
        data.update(updates)
        return self.client.post(
            "/api/workspace/shows/save", json.dumps({
                "id": show.pk,
                "name": data["name"], "code": data["code"],
                "presenter_name": data["presenter_name"],
                "broadcast_start": data["broadcast_start"],
                "broadcast_end": data["broadcast_end"],
                "kind": data["kind"], "schedule_mode": data["schedule_mode"],
                "chart_size": data["chart_size"], "daily_limit": data["daily_limit"],
                "broadcast_weekday": data["broadcast_weekday"],
                "opens_weekday": data["opens_weekday"],
                "closes_weekday": data["closes_weekday"],
                "opens_time": data["opens_time"], "closes_time": data["closes_time"],
                "campaign_start": data["campaign_start"],
                "campaign_end": data["campaign_end"],
                "is_enabled": data["is_enabled"],
                "manual_open": data["manual_open"],
                "confirm_live_code_change": data.get("confirm_live_code_change", False),
            }), content_type="application/json",
        )

    def test_document_contains_18_unique_six_station_programmes(self):
        self.assertEqual(len(LINEUP.OFFICIAL), 18)
        stations = set()
        for (station, name, code, kind, size, day, broadcast_start,
             broadcast_end, presenter, template, december) in LINEUP.OFFICIAL:
            stations.add(station)
            show = ChartShow.objects.get(station=station, name=name)
            self.assertEqual(show.kind, kind)
            self.assertEqual(show.chart_size, size)
            self.assertEqual(show.presenter_name, presenter)
            self.assertEqual(show.broadcast_weekday, day)
            self.assertEqual(
                show.broadcast_start.strftime("%H:%M") if show.broadcast_start else None,
                broadcast_start,
            )
            self.assertEqual(
                show.broadcast_end.strftime("%H:%M") if show.broadcast_end else None,
                broadcast_end,
            )
            self.assertFalse(show.is_enabled)
            self.assertFalse(show.manual_open)
            self.assertEqual(len(show.code), len(show.code.strip()))
            self.assertTrue(2 <= len(show.code) <= 16)
            if december:
                self.assertEqual(
                    campaign_local_dates(show),
                    (date(2026, 12, 1), date(2026, 12, 31)),
                )
            elif kind == "year_end":
                self.assertIsNone(show.campaign_start)
                self.assertIsNone(show.campaign_end)
        self.assertEqual(stations, {
            "radio_zimbabwe", "national_fm", "classic_263",
            "power_fm", "khulumani_fm", "central_radio",
        })

    def test_codes_unique_per_station_but_editable(self):
        for station in {item[0] for item in LINEUP.OFFICIAL}:
            codes = list(ChartShow.objects.filter(
                station=station, is_archived=False
            ).exclude(code="").values_list("code", flat=True))
            self.assertEqual(len(codes), len(set(codes)))
        gospel = ChartShow.objects.get(station="national_fm", name="National FM Gospel 10")
        before_id = gospel.pk
        response = self.post_show(gospel, code="ng11")
        self.assertEqual(response.status_code, 200, response.content)
        gospel.refresh_from_db()
        self.assertEqual(gospel.pk, before_id)
        self.assertEqual(gospel.code, "NG11")
        conflict = self.post_show(gospel, code="N20")
        self.assertEqual(conflict.status_code, 400)
        gospel.refresh_from_db()
        self.assertEqual(gospel.code, "NG11")
        self.assertEqual(self.post_show(gospel, code="a").status_code, 400)
        self.assertEqual(self.post_show(gospel, code="G O").status_code, 400)

    def test_top10_top40_and_presenter_fields_can_be_saved(self):
        gospel = ChartShow.objects.get(station="national_fm", name="National FM Gospel 10")
        response = self.post_show(
            gospel, chart_size=10, broadcast_start="09:30",
            broadcast_end="", presenter_name="New Presenter",
        )
        self.assertEqual(response.status_code, 200, response.content)
        gospel.refresh_from_db()
        self.assertEqual(gospel.chart_size, 10)
        self.assertEqual(gospel.presenter_name, "New Presenter")
        self.assertEqual(gospel.broadcast_start.strftime("%H:%M"), "09:30")
        self.assertIsNone(gospel.broadcast_end)
        self.assertEqual(self.post_show(gospel, chart_size=30).status_code, 400)

    def test_chart_codes_route_to_proper_station_without_listener_replies(self):
        gospel = ChartShow.objects.get(station="national_fm", name="National FM Gospel 10")
        gospel.schedule_mode = "manual"
        gospel.manual_open = True
        gospel.is_enabled = True
        gospel.save(update_fields=["schedule_mode", "manual_open", "is_enabled"])
        show, text, code, error = route_vote(
            "national_fm", "ng10 Artist - New Song"
        )
        self.assertIsNone(error)
        self.assertEqual(show.pk, gospel.pk)
        self.assertEqual(text, "Artist - New Song")
        self.assertEqual(code, "NG10")
        other_show, _, _, _ = route_vote("power_fm", "NG10 Artist - New Song")
        self.assertNotEqual(getattr(other_show, "pk", None), gospel.pk)

    def test_idempotent_reconciliation_keeps_existing_ids_and_codes(self):
        shows = {
            (station, name): ChartShow.objects.get(station=station, name=name).pk
            for station, name, *_ in LINEUP.OFFICIAL
        }
        gospel = ChartShow.objects.get(station="national_fm", name="National FM Gospel 10")
        gospel.code = "ALT10"
        gospel.is_enabled = True
        gospel.save(update_fields=["code", "is_enabled"])
        LINEUP.seed_official_chart_shows(apps, None)
        after = {
            (station, name): ChartShow.objects.get(station=station, name=name).pk
            for station, name, *_ in LINEUP.OFFICIAL
        }
        self.assertEqual(shows, after)
        gospel.refresh_from_db()
        self.assertEqual(gospel.code, "ALT10")
        self.assertTrue(gospel.is_enabled)

    def test_live_code_changes_require_explicit_admin_confirmation(self):
        gospel = ChartShow.objects.get(
            station="national_fm", name="National FM Gospel 10"
        )
        gospel.schedule_mode = "manual"
        gospel.manual_open = True
        gospel.is_enabled = True
        gospel.save(update_fields=["schedule_mode", "manual_open", "is_enabled"])
        refused = self.post_show(gospel, code="NG11")
        self.assertEqual(refused.status_code, 400)
        gospel.refresh_from_db()
        self.assertEqual(gospel.code, "NG10")
        approved = self.post_show(
            gospel, code="NG11", confirm_live_code_change=True
        )
        self.assertEqual(approved.status_code, 200, approved.content)
        gospel.refresh_from_db()
        self.assertEqual(gospel.code, "NG11")
        self.assertTrue(gospel.accepts_at(timezone.now()))

    def test_power_fm_top40_rankings_and_publication_size(self):
        from datetime import timedelta
        from django.utils import timezone
        from apps.charts.publishing import publish_edition

        self.client.post("/accounts/switch-station/", {"station": "power_fm"})
        show = ChartShow.objects.get(station="power_fm", name="Power FM Top 40")
        response = self.client.get("/api/chart/today", {
            "show_id": show.pk, "limit": 100,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["limit"], 40)
        most_recent_saturday = timezone.localdate() - timedelta(
            days=(timezone.localdate().weekday() - 5) % 7
        )
        with self.assertRaisesMessage(ValueError, "configured as Top 40"):
            publish_edition(
                "power_fm", most_recent_saturday, self.admin,
                20, chart_show=show,
            )
        with self.assertRaisesMessage(ValueError, "No verified votes"):
            publish_edition(
                "power_fm", most_recent_saturday, self.admin,
                40, chart_show=show,
            )

    def test_used_legacy_template_is_never_repurposed_or_reassigned(self):
        existing = ChartShow.objects.get(
            station="central_radio", name="Central Radio Top 20 Chart Show"
        )
        original_pk = existing.pk
        # Mimic a live customised legacy template with an attached vote.
        existing.name = "Saturday Top 20"
        existing.code = "SAT20"
        existing.is_enabled = True
        existing.save(update_fields=["name", "code", "is_enabled"])
        listener = User.objects.create(
            channel="whatsapp", user_ref="live-c203", station="central_radio"
        )
        vote = RawVote.objects.create(
            user=listener, station="central_radio", chart_show=existing,
            submitted_code="SAT20", raw_input="Artist - Song",
            artist_raw="Artist", song_raw="Song",
            artist_normalized="artist", song_normalized="song",
            match_key="artist::song", display_name="Artist - Song",
            ai_status="review", vote_date=timezone.localdate(),
        )
        LINEUP.seed_official_chart_shows(apps, None)
        existing.refresh_from_db()
        vote.refresh_from_db()
        replacement = ChartShow.objects.get(
            station="central_radio", name="Central Radio Top 20 Chart Show"
        )
        self.assertEqual(existing.pk, original_pk)
        self.assertEqual(existing.name, "Saturday Top 20")
        self.assertEqual(existing.code, "SAT20")
        self.assertEqual(vote.chart_show_id, original_pk)
        self.assertNotEqual(replacement.pk, original_pk)
        self.assertFalse(replacement.is_enabled)

    def test_unconfirmed_annual_dates_are_not_invented(self):
        self.assertIsNone(ChartShow.objects.get(
            station="classic_263", name="Gospel End-of-Year Top 50"
        ).campaign_start)
        self.assertIsNone(ChartShow.objects.get(
            station="power_fm", name="Power FM Gospel 50"
        ).campaign_start)
        khul = ChartShow.objects.get(station="khulumani_fm", name="Gospel 10")
        self.assertEqual(khul.broadcast_start.strftime("%H:%M"), "19:00")
        self.assertIsNone(khul.broadcast_end)
        self.assertTrue(ChartShow.objects.filter(
            station="luzibo_radio", kind="legacy",
        ).exists())
        self.assertFalse(ChartShow.objects.filter(
            station="luzibo_radio", name__icontains="Magnificent",
        ).exists())
