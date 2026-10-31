"""Bird WhatsApp recipient-number routing for all seven ZBC radio stations."""
import base64
import hashlib
import hmac
import json
import time

from django.test import TestCase, override_settings

from apps.accounts.models import Station
from apps.voting.models import InboundEvent, OutboundMessage, RawVote
from apps.voting.worker import run_one


NUMBER_ROUTES = {
    "radio_zimbabwe": "+263770000001",
    "national_fm": "+263770000002",
    "power_fm": "+263770000003",
    "classic_263": "+263770000004",
    "central_radio": "+263770000005",
    "khulumani_fm": "+263770000006",
    "luzibo_radio": "+263770000007",
}
ROUTE_CONFIG = ",".join(f"{number}:{station}" for station, number in NUMBER_ROUTES.items())


@override_settings(
    SECURE_SSL_REDIRECT=False,
    AUTO_AI_MATCH=False,
    BIRD_WEBHOOK_SECRET="whsec_" + base64.b64encode(b"seven-station-test").decode(),
    BIRD_STATION="radio_zimbabwe",
    BIRD_NUMBER_STATIONS=ROUTE_CONFIG,
    # A legacy sender must not send replies from the wrong station.
    BIRD_ACCESS_KEY="legacy-access",
    BIRD_WORKSPACE_ID="legacy-workspace",
    BIRD_CHANNEL_ID="legacy-channel",
)
class SevenStationBirdTests(TestCase):
    def send_event(self, data, *, event_id="webhook-1", valid_signature=True):
        body = json.dumps({"type": "whatsapp.received", "data": data})
        timestamp = str(int(time.time()))
        signature = base64.b64encode(hmac.new(
            b"seven-station-test",
            f"{event_id}.{timestamp}.{body}".encode(),
            hashlib.sha256,
        ).digest()).decode()
        return self.client.post(
            "/webhook/bird/", body, content_type="application/json",
            HTTP_WEBHOOK_ID=event_id,
            HTTP_WEBHOOK_TIMESTAMP=timestamp,
            HTTP_WEBHOOK_SIGNATURE="v1," + (signature if valid_signature else "invalid"),
        )

    def event(self, station, message_id):
        return {
            "whatsapp_id": message_id,
            "direction": "inbound",
            "from": {"phone_number": "+263771111111"},
            "to": {"phone_number": NUMBER_ROUTES[station]},
            "text": {"body": "Example Artist - Example Track"},
            # Any client-supplied station field is not authoritative.
            "station": "radio_zimbabwe",
        }

    def test_seven_numbers_route_to_seven_independent_vote_queues(self):
        self.assertEqual(len(Station.values), 7)
        for station in Station.values:
            with self.subTest(station=station):
                event = self.event(station, "wam-" + station)
                self.assertEqual(
                    self.send_event(event, event_id="delivery-" + station).status_code,
                    202,
                )
                self.assertEqual(InboundEvent.objects.get(message_id=event["whatsapp_id"]).station, station)
        for _ in NUMBER_ROUTES:
            self.assertTrue(run_one(InboundEvent))
        self.assertEqual(RawVote.objects.count(), 7)
        self.assertEqual(set(RawVote.objects.values_list("station", flat=True)), set(Station.values))
        self.assertFalse(OutboundMessage.objects.exists())

    def test_unknown_or_missing_destination_is_never_attributed_to_default_station(self):
        for i, destination in enumerate([{}, {"to": {}},
                                          {"to": {"phone_number": "+263779999999"}},
                                          {"to": {"phone_number": "invalid"}}]):
            with self.subTest(destination=destination):
                event = self.event("radio_zimbabwe", f"wam-unknown-{i}")
                event.pop("to")
                event.update(destination)
                self.assertEqual(self.send_event(event, event_id=f"unknown-{i}").status_code, 503)
        self.assertFalse(InboundEvent.objects.exists())

    def test_signature_and_replay_protection_still_apply(self):
        event = self.event("luzibo_radio", "wam-luzibo-1")
        self.assertEqual(self.send_event(event, valid_signature=False).status_code, 403)
        self.assertFalse(InboundEvent.objects.exists())
        self.assertEqual(self.send_event(event).status_code, 202)
        self.assertEqual(self.send_event(event, event_id="another-delivery").status_code, 200)
        self.assertEqual(InboundEvent.objects.count(), 1)
        self.assertEqual(InboundEvent.objects.get().station, "luzibo_radio")

    def test_invalid_routes_fail_closed(self):
        configurations = [
            "+263770000001:nonexistent",
            "+263770000001:radio_zimbabwe,+263770000001:power_fm",
            "+263770000001:radio_zimbabwe,+263770000002:radio_zimbabwe",
            "invalid:radio_zimbabwe",
            "+263770000001=radio_zimbabwe",
        ]
        for config in configurations:
            with self.subTest(config=config), override_settings(BIRD_NUMBER_STATIONS=config):
                self.assertEqual(self.send_event(self.event("radio_zimbabwe", "wam-bad")).status_code, 503)
        self.assertFalse(InboundEvent.objects.exists())

    @override_settings(BIRD_NUMBER_STATIONS="")
    def test_legacy_one_station_configuration_still_works(self):
        event = self.event("national_fm", "wam-old")
        self.assertEqual(self.send_event(event).status_code, 202)
        self.assertEqual(InboundEvent.objects.get().station, "radio_zimbabwe")
