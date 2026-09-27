"""Current Bird reception must not depend on the legacy outbound client."""
import base64
import hashlib
import hmac
import json
import time

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from apps.voting.models import InboundEvent, OutboundMessage, RawVote, VoteJob, CleanedSong
from apps.voting.pipeline import review_song
from apps.voting.worker import run_one


@override_settings(SECURE_SSL_REDIRECT=False, AUTO_AI_MATCH=False,
                   BIRD_WEBHOOK_SECRET='whsec_' + base64.b64encode(b'test-key').decode(),
                   BIRD_ACCESS_KEY='', BIRD_WORKSPACE_ID='', BIRD_CHANNEL_ID='',
                   BIRD_STATION='radio_zimbabwe')
class BirdCurrentTests(TestCase):
    def post_event(self, data=None, event_type='whatsapp.received', signed=True, delivery='delivery-1'):
        data = data if data is not None else {
            'whatsapp_id': 'wam-test-1', 'direction': 'inbound',
            'from': {'phone_number': '+263770000010'},
            'text': {'body': 'Test Artist - Test Song'},
        }
        body = json.dumps({'type': event_type, 'data': data})
        stamp = str(int(time.time()))
        sig = base64.b64encode(hmac.new(b'test-key', f'{delivery}.{stamp}.{body}'.encode(), hashlib.sha256).digest()).decode()
        return self.client.post('/webhook/bird/', body, content_type='application/json',
                                HTTP_WEBHOOK_ID=delivery, HTTP_WEBHOOK_TIMESTAMP=stamp,
                                HTTP_WEBHOOK_SIGNATURE='v1,' + sig if signed else 'v1,invalid')

    def test_receive_only_reaches_dashboard_once_without_outbound_jobs(self):
        self.assertEqual(self.post_event(signed=False).status_code, 403)
        self.assertFalse(InboundEvent.objects.exists())
        self.assertEqual(self.post_event().status_code, 202)
        self.assertEqual(self.post_event(delivery='different-delivery').status_code, 200)
        self.assertTrue(run_one(InboundEvent))
        self.assertTrue(run_one(VoteJob))
        self.assertEqual(RawVote.objects.count(), 1)
        self.assertFalse(OutboundMessage.objects.exists())
        admin = get_user_model().objects.create_superuser('bird-admin', password='test-only')
        self.client.force_login(admin)
        self.assertEqual(self.client.get('/api/workspace/overview').json()['received'], 1)
        review_song('radio_zimbabwe', admin, CleanedSong.objects.get().pk, 'verified')
        self.assertEqual(self.client.get('/api/chart/today').json()['top100'][0]['count'], 1)

    def test_outbound_status_and_malformed_text_cannot_count(self):
        self.assertTrue(self.post_event({'direction': 'outbound'}, event_type='whatsapp.sent').json()['ignored'])
        self.assertEqual(self.post_event({'direction': 'inbound', 'from': {'phone_number': '263770000010'}, 'text': {'body': 123}}).status_code, 400)
        self.assertFalse(InboundEvent.objects.exists())
