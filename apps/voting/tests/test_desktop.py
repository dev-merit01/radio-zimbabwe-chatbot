from django.test import TestCase, override_settings


@override_settings(SECURE_SSL_REDIRECT=False)
class TestDesktopHandshake(TestCase):
    def test_handshake_needs_no_login_or_database_queries(self):
        with self.assertNumQueries(0):
            response = self.client.get("/api/desktop/status")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "application": "radio-zimbabwe-voting-studio",
                "desktop_api": 1,
            },
        )
        self.assertIn("no-store", response["Cache-Control"])

    def test_handshake_is_read_only(self):
        self.assertEqual(self.client.post("/api/desktop/status").status_code, 405)
