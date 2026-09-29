import base64
import hashlib
import hmac
import json
import time

from django.contrib.auth import get_user_model
from django.test import Client, TestCase, override_settings
from apps.accounts.models import AccountProfile
from apps.voting.models import InboundEvent


@override_settings(SECURE_SSL_REDIRECT=False)
class ProfileTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user('profile-reader', password='Old-secure-password-295!')
        AccountProfile.objects.create(user=self.user, station='national_fm')

    def test_requires_login_and_edits_only_own_details(self):
        self.assertEqual(self.client.get('/accounts/profile/').status_code, 302)
        self.client.force_login(self.user)
        self.assertContains(self.client.get('/accounts/profile/'), 'National FM')
        response = self.client.post('/accounts/profile/', {
            'action': 'profile', 'first_name': 'New', 'last_name': 'Name', 'email': 'test@example.com',
            'station': 'radio_zimbabwe', 'is_superuser': 'true', 'is_staff': 'true', 'username': 'admin',
        })
        self.assertEqual(response.status_code, 302)
        self.user.refresh_from_db()
        self.assertEqual(self.user.first_name, 'New')
        self.assertEqual(self.user.username, 'profile-reader')
        self.assertFalse(self.user.is_superuser or self.user.is_staff)
        self.assertEqual(self.user.profile.station, 'national_fm')

    def test_password_requires_old_password_and_keeps_current_session(self):
        self.client.force_login(self.user)
        second = Client()
        second.force_login(self.user)
        values = {'action': 'password', 'old_password': 'wrong', 'new_password1': 'New-secure-password-629!', 'new_password2': 'New-secure-password-629!'}
        self.assertContains(self.client.post('/accounts/profile/', values), 'old password was entered incorrectly')
        values['old_password'] = 'Old-secure-password-295!'
        self.assertEqual(self.client.post('/accounts/profile/', values).status_code, 302)
        self.assertEqual(self.client.get('/accounts/profile/').status_code, 200)
        self.assertEqual(second.get('/accounts/profile/').status_code, 302)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password(values['new_password1']))

    def test_profile_requires_csrf(self):
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.user)
        self.assertEqual(client.post('/accounts/profile/', {'action': 'profile', 'first_name': 'Forged'}).status_code, 403)

    def test_overview_markup_has_no_breadcrumb_and_profile_link(self):
        self.client.force_login(self.user)
        response = self.client.get('/')
        self.assertNotContains(response, 'breadcrumb-page')
        self.assertContains(response, 'href="/accounts/profile/"')
        self.assertNotContains(response, 'id="refresh"')


@override_settings(SECURE_SSL_REDIRECT=False, ALLOWED_HOSTS=['testserver', 'station.ngrok-free.dev'],
                   LOCAL_WEBHOOK_HOSTS=['station.ngrok-free.dev'],
                   BIRD_WEBHOOK_SECRET='whsec_' + base64.b64encode(b'tunnel-test').decode())
class TunnelTests(TestCase):
    def test_tunnel_accepts_signed_bird_requests_but_hides_workspace(self):
        for path in ['/', '/admin/', '/accounts/login/', '/accounts/profile/', '/api/workspace/overview', '/missing']:
            self.assertEqual(self.client.get(path, HTTP_HOST='station.ngrok-free.dev').status_code, 404)
        self.assertEqual(self.client.get('/accounts/login/').status_code, 200)
        payload = json.dumps({'type':'whatsapp.received', 'data':{'whatsapp_id':'tunnel-vote', 'direction':'inbound', 'from':{'phone_number':'+263770000011'}, 'text':{'body':'Artist - Song'}}})
        stamp = str(int(time.time()))
        sig = base64.b64encode(hmac.new(b'tunnel-test', f'delivery.{stamp}.{payload}'.encode(), hashlib.sha256).digest()).decode()
        headers = {'HTTP_HOST':'station.ngrok-free.dev', 'HTTP_WEBHOOK_ID':'delivery', 'HTTP_WEBHOOK_TIMESTAMP':stamp, 'HTTP_WEBHOOK_SIGNATURE':'v1,' + sig}
        self.assertEqual(self.client.post('/webhook/bird/', payload, content_type='application/json', **headers).status_code, 202)
        self.assertEqual(InboundEvent.objects.count(), 1)
        headers['HTTP_WEBHOOK_SIGNATURE'] = 'v1,invalid'
        self.assertEqual(self.client.post('/webhook/bird/', payload, content_type='application/json', **headers).status_code, 403)

    def test_public_tunnel_never_receives_debug_error_html(self):
        from django.test import RequestFactory
        from django.http import HttpResponse
        from radio_zimbabwe.middleware import LocalWebhookHostMiddleware
        request = RequestFactory().post('/webhook/bird/', HTTP_HOST='station.ngrok-free.dev')
        for original, expected in [(400, 400), (500, 503)]:
            middleware = LocalWebhookHostMiddleware(lambda request: HttpResponse('private debug traceback', status=original))
            response = middleware(request)
            self.assertEqual(response.status_code, expected)
            self.assertNotIn(b'private debug', response.content)
