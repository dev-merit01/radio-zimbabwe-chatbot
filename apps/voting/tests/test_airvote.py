import base64
import hashlib
import hmac
import json
import time
import uuid
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.contrib.auth.hashers import check_password
from django.contrib.auth.models import Permission
from django.core.cache import cache
from django.test import TestCase, Client, override_settings
from apps.accounts.models import StationAccess, AccountProfile
from apps.voting.models import InboundEvent, RawVote, VoteJob, OutboundMessage, ReviewAudit, CleanedSong
from apps.voting.worker import run_one
from apps.voting.pipeline import review_song


@override_settings(SECURE_SSL_REDIRECT=False, AUTO_AI_MATCH=False)
class AirVoteTests(TestCase):
    def setUp(self):
        cache.clear()
        self.admin = get_user_model().objects.create_superuser('admin', password='test-admin-only')
        self.staff = get_user_model().objects.create_user('staff', password='test-staff-only')
        AccountProfile.objects.create(user=self.staff, station='radio_zimbabwe')
        self.client.force_login(self.admin)
        self.password = 'Station-access-427!'

    def post(self, path, data):
        return self.client.post('/api/workspace/' + path, data, content_type='application/json')

    def provision(self, station='national_fm'):
        self.client.force_login(self.admin)
        self.client.post('/accounts/switch-station/', {'station': station})
        self.assertEqual(self.post('station-password', {'password': self.password}).status_code, 200)
        self.client.force_login(self.staff)

    def station(self):
        return self.client.get('/api/workspace/overview').json()['station']

    def test_password_gate_admin_bypass_and_rotation(self):
        self.provision()
        access = StationAccess.objects.get()
        self.assertNotEqual(access.password_hash, self.password)
        self.assertTrue(check_password(self.password, access.password_hash))
        for password in ['', 'wrong']:
            self.client.post('/accounts/switch-station/', {'station': 'national_fm', 'password': password})
            self.assertEqual(self.station(), 'radio_zimbabwe')
        self.client.post('/accounts/switch-station/', {'station': 'national_fm', 'password': self.password})
        self.assertEqual(self.station(), 'national_fm')
        session = self.client.session
        self.assertNotIn(self.password, json.dumps(dict(session)))
        other = Client()
        other.force_login(self.admin)
        other.post('/accounts/switch-station/', {'station': 'national_fm'})
        self.assertEqual(other.post('/api/workspace/station-password', {'password': 'Changed-station-892!'}, content_type='application/json').status_code, 200)
        self.assertEqual(self.station(), 'radio_zimbabwe')
        self.assertNotIn(self.password, json.dumps(list(ReviewAudit.objects.values('details'))))

    def test_forged_session_and_unset_password_are_denied(self):
        self.client.force_login(self.staff)
        self.client.post('/accounts/switch-station/', {'station': 'national_fm', 'password': self.password})
        self.assertEqual(self.station(), 'radio_zimbabwe')
        session = self.client.session
        session['switched_station'] = 'national_fm'
        session['station_access_version'] = str(uuid.uuid4())
        session.save()
        self.assertEqual(self.station(), 'radio_zimbabwe')
        self.assertEqual(self.post('station-password', {'password': self.password}).status_code, 403)

    def test_password_attempts_throttled_and_csrf_required(self):
        self.provision()
        for _ in range(10):
            self.client.post('/accounts/switch-station/', {'station': 'national_fm', 'password': 'wrong'})
        self.client.post('/accounts/switch-station/', {'station': 'national_fm', 'password': self.password})
        self.assertEqual(self.station(), 'radio_zimbabwe')
        strict = Client(enforce_csrf_checks=True)
        strict.force_login(self.admin)
        self.assertEqual(strict.post('/accounts/switch-station/', {'station': 'national_fm'}).status_code, 403)
        self.assertEqual(strict.post('/api/workspace/station-password', {'password': self.password}, content_type='application/json').status_code, 403)

    def test_manual_vote_deduplicates_and_reaches_dashboard_and_chart(self):
        data = {'listener': 'listener-1', 'text': 'River Artist - Morning Song', 'request_id': str(uuid.uuid4())}
        for code in [202, 200]:
            self.assertEqual(self.post('votes', data).status_code, code)
        self.assertEqual(InboundEvent.objects.count(), 1)
        self.assertTrue(run_one(InboundEvent))
        self.assertEqual(OutboundMessage.objects.count(), 0)
        self.assertEqual(self.client.get('/api/workspace/overview').json()['received'], 1)
        self.assertTrue(run_one(VoteJob))
        song = CleanedSong.objects.get()
        self.assertEqual(self.client.get('/api/workspace/overview').json()['verified_votes'], 0)
        review_song('radio_zimbabwe', self.admin, song.pk, 'verified')
        self.assertEqual(self.client.get('/api/workspace/overview').json()['verified_votes'], 1)
        self.assertEqual(self.client.get('/api/chart/today').json()['top100'][0]['count'], 1)
        self.assertIn('Vote recorded', self.client.get('/api/workspace/incoming').json()['submissions'][0]['reply'])
        data['request_id'] = str(uuid.uuid4())
        self.post('votes', data)
        run_one(InboundEvent)
        self.assertEqual(RawVote.objects.count(), 1)
        self.assertIn('already voted', InboundEvent.objects.latest('id').reply)

    def test_manual_permissions_station_scope_and_changed_retry(self):
        data = {'listener': 'listener-1', 'text': 'River Artist - Morning Song', 'request_id': str(uuid.uuid4()), 'station': 'national_fm'}
        self.client.force_login(self.staff)
        self.assertEqual(self.post('votes', data).status_code, 403)
        self.staff.user_permissions.add(Permission.objects.get(codename='add_rawvote'))
        self.assertEqual(self.post('votes', data).status_code, 202)
        self.assertEqual(InboundEvent.objects.get().station, 'radio_zimbabwe')
        data['text'] = 'Different Artist - Other Song'
        self.assertEqual(self.post('votes', data).status_code, 409)
        self.assertEqual(InboundEvent.objects.count(), 1)

    @override_settings(TELEGRAM_WEBHOOK_SECRET='telegram-test', TELEGRAM_STATION='national_fm',
                       ONEMSG_WEBHOOK_SECRET='onemsg-test', ONEMSG_STATION='national_fm',
                       BIRD_WEBHOOK_SECRET=base64.b64encode(b'bird-test-key').decode(), BIRD_STATION='national_fm',
                       BIRD_ACCESS_KEY='test', BIRD_WORKSPACE_ID='test', BIRD_CHANNEL_ID='test')
    def test_provider_receipts_process_once_and_reach_correct_dashboard(self):
        text = 'River Artist - Morning Song'
        payloads = [
            ('telegram', {'update_id': 882, 'station': 'power_fm', 'message': {'chat': {'id': 881, 'type': 'private'}, 'text': text}}, {'HTTP_X_TELEGRAM_BOT_API_SECRET_TOKEN': 'telegram-test'}),
            ('whatsapp', {'id': 'one-1', 'sender': '263770000002', 'payload': {'conversation': text}}, {'HTTP_X_WEBHOOK_TOKEN': 'onemsg-test'}),
        ]
        bird = json.dumps({'event': 'whatsapp.inbound', 'payload': {'id': 'bird-1', 'direction': 'incoming', 'sender': {'contact': {'identifierValue': '+263770000003'}}, 'body': {'type': 'text', 'text': {'text': text}}}})
        timestamp = str(int(time.time()))
        signature = base64.b64encode(hmac.new(b'bird-test-key', ('bird-event.' + timestamp + '.' + bird).encode(), hashlib.sha256).digest()).decode()
        payloads.append(('bird', bird, {'HTTP_WEBHOOK_ID': 'bird-event', 'HTTP_WEBHOOK_TIMESTAMP': timestamp, 'HTTP_WEBHOOK_SIGNATURE': 'v1,' + signature}))
        for provider, payload, headers in payloads:
            with self.subTest(provider=provider):
                for code in [202, 200]:
                    self.assertEqual(self.client.post('/webhook/' + provider + '/', payload, content_type='application/json', **headers).status_code, code)
                self.assertTrue(run_one(InboundEvent))
                self.assertTrue(run_one(VoteJob))
        self.assertEqual(self.client.get('/api/workspace/overview').json()['received'], 0)
        self.client.post('/accounts/switch-station/', {'station': 'national_fm'})
        self.assertEqual(self.client.get('/api/workspace/overview').json()['received'], 3)
        song = CleanedSong.objects.get(station='national_fm')
        review_song('national_fm', self.admin, song.pk, 'verified')
        self.assertEqual(self.client.get('/api/chart/today').json()['top100'][0]['count'], 3)
        for module, response in [('telegram_client', {'result': {'message_id': 9}}), ('whatsapp_client', {'id': 'one-reply'}), ('bird_client', {'id': 'bird-reply'})]:
            with patch('apps.bot.' + module + '.get_client') as client:
                client.return_value.send_text.return_value = response
                self.assertTrue(run_one(OutboundMessage))
        self.assertEqual(OutboundMessage.objects.filter(state='done').count(), 3)
