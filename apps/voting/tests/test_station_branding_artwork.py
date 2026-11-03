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

    def test_supplied_central_and_power_logos_are_locally_hosted(self):
        expected = {
            Station.CENTRAL_RADIO: "central_radio.webp",
            Station.POWER_FM: "power_fm.webp",
        }
        for station, filename in expected.items():
            with self.subTest(station=station):
                result = self.get_branding(station)
                local_url = "/static/images/" + filename
                self.assertEqual(result["branding_station_logo_url"], local_url)
                self.assertEqual(result["branding_station_logo_fallback_url"], local_url)
                self.assertEqual(result["active_station"], station)

    def test_supplied_logos_are_valid_webp_static_assets(self):
        from pathlib import Path
        from django.contrib.staticfiles import finders

        for filename in ("central_radio.webp", "power_fm.webp"):
            with self.subTest(filename=filename):
                path = finders.find("images/" + filename)
                self.assertIsNotNone(path)
                data = Path(path).read_bytes()
                self.assertEqual(data[:4], b"RIFF")
                self.assertEqual(data[8:12], b"WEBP")

    def test_radio_zimbabwe_keeps_existing_logo(self):
        result = self.get_branding(Station.RADIO_ZIMBABWE)
        self.assertEqual(result["branding_station_logo_url"], "/static/images/radioZim.jpg")
        self.assertEqual(result["branding_station_logo_fallback_url"], result["branding_station_logo_url"])
