"""Bird multi-station routing: protect stations from each other's WhatsApp votes."""

import base64
import hashlib
import hmac
import json
import time

from django.test import TestCase, override_settings

from apps.voting.models import InboundEvent


@override_settings(
    SECURE_SSL_REDIRECT=False,
    BIRD_WEBHOOK_SECRET=base64.b64encode(b"test-bird-signing-key").decode(),
    BIRD_STATION="radio_zimbabwe",
    BIRD_NUMBER_STATION_MAP={"263790573340": "national_fm"},
)
class BirdNumberRoutingTests(TestCase):
    def send_event(self, receiving_number="+263790573340", message_id="wam-1", extra=None):
        data = {
            "whatsapp_id": message_id,
            "direction": "inbound",
            "from": {"phone_number": "+263771112222"},
            "text": {"body": "River Artist - Morning Song"},
            "station": "radio_zimbabwe",  # An untrusted value must not affect routing.
        }
        if receiving_number is not None:
            data["to"] = {"phone_number": receiving_number}
        if extra:
            data.update(extra)
        body = json.dumps({"type": "whatsapp.received", "data": data})
        timestamp = str(int(time.time()))
        webhook_id = "event-" + message_id
        signature = base64.b64encode(hmac.new(
            b"test-bird-signing-key",
            (webhook_id + "." + timestamp + "." + body).encode(),
            hashlib.sha256,
        ).digest()).decode()
        return self.client.post(
            "/webhook/bird/", body, content_type="application/json",
            HTTP_WEBHOOK_ID=webhook_id,
            HTTP_WEBHOOK_TIMESTAMP=timestamp,
            HTTP_WEBHOOK_SIGNATURE="v1," + signature,
        )

    def test_nfm_number_is_routed_to_national_fm(self):
        self.assertEqual(self.send_event().status_code, 202)
        event = InboundEvent.objects.get()
        self.assertEqual(event.station, "national_fm")
        self.assertEqual(event.sender, "263771112222")
        self.assertEqual(self.send_event().status_code, 200)
        self.assertEqual(InboundEvent.objects.count(), 1)

    def test_nfm_number_works_without_country_prefix_symbol(self):
        self.assertEqual(self.send_event("263790573340").status_code, 202)
        self.assertEqual(InboundEvent.objects.get().station, "national_fm")

    def test_another_bird_number_cannot_enter_nfm(self):
        for number in ("+263790583340", "+263790593340", "+263791912204"):
            result = self.send_event(number)
            self.assertEqual(result.status_code, 200)
            self.assertTrue(result.json()["ignored"])
        self.assertFalse(InboundEvent.objects.exists())

    def test_missing_or_invalid_destination_is_ignored(self):
        for number in (None, "", "not-a-phone", {"phone_number": "263790573340"}):
            result = self.send_event(number)
            self.assertEqual(result.status_code, 200)
            self.assertTrue(result.json()["ignored"])
        self.assertFalse(InboundEvent.objects.exists())

    def test_unrecognised_station_name_cannot_be_used(self):
        with override_settings(BIRD_NUMBER_STATION_MAP={"263790573340": "unrecognised"}):
            self.assertEqual(self.send_event().status_code, 503)
            self.assertFalse(InboundEvent.objects.exists())

    def test_legacy_single_station_still_works_when_map_unset(self):
        with override_settings(BIRD_NUMBER_STATION_MAP={}):
            self.assertEqual(self.send_event(receiving_number=None).status_code, 202)
            self.assertEqual(InboundEvent.objects.get().station, "radio_zimbabwe")
