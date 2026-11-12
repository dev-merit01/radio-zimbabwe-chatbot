"""National broadcaster code enforcement: no silent historical-chart routing."""
import json

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from apps.charts.routing import route_vote
from apps.voting.models import ChartShow, RawVote, StationVotingConfig


@override_settings(SECURE_SSL_REDIRECT=False)
class CodeEnforcementRolloutTests(TestCase):
    def test_existing_stations_require_codes_but_shows_remain_closed(self):
        stations = (
            "radio_zimbabwe", "national_fm", "power_fm", "classic_263",
            "central_radio", "khulumani_fm", "luzibo_radio",
        )
        for station in stations:
            with self.subTest(station=station):
                self.assertTrue(
                    StationVotingConfig.objects.get(station=station).require_codes
                )
                self.assertFalse(
                    ChartShow.objects.filter(
                        station=station, kind__in=("weekly", "year_end"),
                        is_enabled=True,
                    ).exists()
                )

    def test_gp20_g20_closed_votes_are_held_not_counted(self):
        for code, title in [
            ("GP20", "Sugar Sugar - Mudododo"),
            ("G20", "Mathius Mhere - Ndiperekedze"),
        ]:
            with self.subTest(code=code):
                show, vote, submitted, reason = route_vote(
                    "radio_zimbabwe", f"{code} {title}"
                )
                self.assertIsNone(show)
                self.assertEqual(vote, title)
                self.assertEqual(submitted, code)
                self.assertEqual(reason, "show_closed")
        self.assertFalse(RawVote.objects.exists())

    def test_uncoded_votes_never_fall_back_to_historical_chart(self):
        show, text, submitted, reason = route_vote(
            "radio_zimbabwe", "Sugar Sugar - Mudododo",
        )
        self.assertIsNone(show)
        self.assertEqual(text, "")
        self.assertEqual(submitted, "")
        self.assertEqual(reason, "missing_or_unknown_code")
        self.assertFalse(RawVote.objects.exists())

    def test_admin_can_require_codes_before_opening_any_chart_show(self):
        user = get_user_model().objects.create_superuser(
            username="station-code-audit",
            password="Station-Code-Audit-Test-Only!923",
        )
        self.client.force_login(user)
        StationVotingConfig.objects.filter(station="radio_zimbabwe").update(
            require_codes=False,
        )
        self.client.post(
            "/accounts/switch-station/", {"station": "radio_zimbabwe"},
        )
        response = self.client.post(
            "/api/workspace/shows/routing",
            json.dumps({"require_codes": True}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200, response.content)
        self.assertTrue(
            StationVotingConfig.objects.get(station="radio_zimbabwe").require_codes
        )

    def test_correct_code_routes_only_if_programme_open(self):
        gospel = ChartShow.objects.get(
            station="radio_zimbabwe", code="G20",
        )
        gospel.is_enabled = True
        gospel.schedule_mode = "manual"
        gospel.manual_open = True
        gospel.save(update_fields=["is_enabled", "schedule_mode", "manual_open"])
        show, text, submitted, reason = route_vote(
            "radio_zimbabwe", "g20 Mathius Mhere - Ndiperekedze",
        )
        self.assertIsNone(reason)
        self.assertEqual(show.pk, gospel.pk)
        self.assertEqual(submitted, "G20")
        self.assertEqual(text, "Mathius Mhere - Ndiperekedze")
        # Same code on another station never inherits Radio Zimbabwe's window.
        other, _, _, reason = route_vote(
            "national_fm", "G20 Mathius Mhere - Ndiperekedze",
        )
        self.assertIsNone(other)
        self.assertEqual(reason, "missing_or_unknown_code")
