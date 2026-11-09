"""The uploaded official ZBC lineup is complete, safe to rerun and editable."""
import importlib
import json
from datetime import date

from apps.charts.periods import campaign_local_dates

from django.apps import apps
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from apps.charts.routing import route_vote
from apps.voting.models import ChartShow


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
