from unittest.mock import patch
from django.test import TestCase, override_settings


@override_settings(SECURE_SSL_REDIRECT=True, ALLOWED_HOSTS=["healthcheck.railway.app", "testserver"])
class ReadinessTests(TestCase):
    def test_internal_probe_succeeds_without_login_or_https_redirect(self):
        response = self.client.get('/healthz/', HTTP_HOST='healthcheck.railway.app')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'status': 'ok'})
        self.assertEqual(self.client.get('/accounts/login/').status_code, 301)

    def test_dependency_failures_are_unavailable_without_details(self):
        for dependency in ('connection.cursor', 'cache.set'):
            with self.subTest(dependency=dependency), patch(
                'apps.dashboard.readiness.' + dependency,
                side_effect=RuntimeError('private-credential-must-not-leak'),
            ):
                response = self.client.get('/healthz/')
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.json(), {'status': 'unavailable'})

    def test_cache_must_retrieve_written_value(self):
        with patch('apps.dashboard.readiness.cache.get', return_value=None):
            self.assertEqual(self.client.get('/healthz/').status_code, 503)
