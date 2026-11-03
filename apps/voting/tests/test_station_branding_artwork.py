"""Station branding must resolve the correct artwork without mixing stations."""

from types import SimpleNamespace

from django.test import SimpleTestCase

from apps.accounts.context_processors import station_branding
from apps.accounts.models import Station


class StationBrandingArtworkTests(SimpleTestCase):
    def get_branding(self, station):
        request = SimpleNamespace(
            session={},
            user=SimpleNamespace(
                is_authenticated=True,
                is_superuser=False,
                profile=SimpleNamespace(station=station),
            ),
        )
        return station_branding(request)

    def test_non_radio_zimbabwe_stations_have_correct_logo_sources(self):
        expected = {
            Station.NATIONAL_FM: "nationalfm.jpg",
            Station.POWER_FM: "powerfm.jpg",
            Station.CLASSIC_263: "classic-263.jpg",
            Station.KHULUMANI_FM: "khulumani-fm.jpg",
        }
        for station, ending in expected.items():
            with self.subTest(station=station):
                result = self.get_branding(station)
                self.assertTrue(result["branding_station_logo_url"].startswith("https://"))
                self.assertTrue(result["branding_station_logo_url"].endswith(ending))
                self.assertEqual(result["active_station"], station)
                self.assertTrue(result["branding_station_logo_fallback_url"].startswith("/static/images/"))

    def test_central_radio_keeps_local_badge_until_authentic_logo_available(self):
        result = self.get_branding(Station.CENTRAL_RADIO)
        self.assertEqual(result["branding_station_logo_url"], "/static/images/central_radio.svg")
        self.assertEqual(result["branding_station_logo_fallback_url"], result["branding_station_logo_url"])

    def test_radio_zimbabwe_keeps_existing_logo(self):
        result = self.get_branding(Station.RADIO_ZIMBABWE)
        self.assertEqual(result["branding_station_logo_url"], "/static/images/radioZim.jpg")
        self.assertEqual(result["branding_station_logo_fallback_url"], result["branding_station_logo_url"])
