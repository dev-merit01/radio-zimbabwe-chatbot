"""The retired public portal cannot accept traffic or expose voter endpoints."""
from django.test import SimpleTestCase, override_settings

from apps.voting.channels import source_label, vote_source


@override_settings(SECURE_SSL_REDIRECT=False)
class RetiredPublicPortalTests(SimpleTestCase):
    def test_public_portal_and_all_previous_public_api_routes_are_not_found(self):
        urls = [
            "/vote/",
            "/vote/api/stations/",
            "/vote/api/shows/1/",
            "/vote/api/verify/start/",
            "/vote/api/verify/check/",
            "/vote/api/cast/",
        ]
        for url in urls:
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 404)
                response = self.client.post(url)
                self.assertEqual(response.status_code, 404)

    def test_historical_channel_names_and_queued_web_receipts_remain_supported(self):
        # Removing intake must not rewrite records or disable already queued events.
        self.assertEqual(source_label("web"), "Online portal")
        self.assertEqual(vote_source("web"), "web")
        self.assertEqual(source_label("whatsapp"), "WhatsApp")
        self.assertEqual(vote_source("bird"), "whatsapp")
        self.assertEqual(vote_source("sms"), "sms")
        self.assertEqual(vote_source("manual"), "manual")
