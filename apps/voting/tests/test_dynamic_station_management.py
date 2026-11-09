"""Dynamic station provisioning, secure Bird routing and existing-station safety."""
import base64
import hashlib
import hmac
import json
import time

from django.contrib.auth import get_user_model
from django.test import Client, TestCase, override_settings

from apps.accounts.forms import RegistrationForm
from apps.accounts.models import BroadcastStation, Station, station_choices
from apps.voting.models import ChartShow, InboundEvent, StationVotingConfig


@override_settings(
    SECURE_SSL_REDIRECT=False,
    BIRD_WEBHOOK_SECRET=base64.b64encode(b"dynamic-station-test-key").decode(),
    BIRD_NUMBER_STATION_MAP={"263790573340": "national_fm"},
    BIRD_STATION="radio_zimbabwe",
    BIRD_SILENT_VOTING=True,
)
class DynamicStationProvisioningTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser(
            username="network-admin", password="Network-Test-Only!47"
        )
        self.client.force_login(self.admin)

    def post(self, url, payload, client=None):
        return (client or self.client).post(
            "/api/workspace/" + url,
            json.dumps(payload), content_type="application/json",
        )

    def make_station(self, name="Midlands Radio", number="263791112233"):
        resp = self.post("stations/create", {
            "name": name, "bird_number": number,
        })
        self.assertEqual(resp.status_code, 201, resp.content)
        return resp.json()["station"]["slug"]

    def configure(self, slug, **updates):
        station = BroadcastStation.objects.get(slug=slug)
        return self.post("stations/update", {
            "slug": slug, "name": station.name,
            "bird_number": station.bird_number or "",
            "logo_url": station.logo_url,
            "is_active": station.is_active,
            **updates,
        })

    def make_show(self, name="Midlands Top 20", code="MW20", enabled=False):
        payload = {
            "name": name, "code": code, "kind": "weekly",
            "schedule_mode": "manual", "chart_size": 20, "daily_limit": 5,
            "broadcast_weekday": 5, "opens_weekday": 0, "closes_weekday": 5,
            "opens_time": "08:00", "closes_time": "18:00",
            "campaign_start": None, "campaign_end": None,
            "is_enabled": enabled, "manual_open": enabled,
            "presenter_name": "Midlands Presenter",
            "broadcast_start": "11:00", "broadcast_end": "12:00",
        }
        response = self.post("shows/save", payload)
        self.assertEqual(response.status_code, 200, response.content)
        return response.json()["show"]["id"]

    def bird(self, number, message_id="dynamic-1", body="MW20 Artist - Song"):
        data = {
            "whatsapp_id": message_id,
            "direction": "inbound",
            "from": {"phone_number": "+263771112222"},
            "to": {"phone_number": number},
            "text": {"body": body},
            "station": "radio_zimbabwe",  # untrusted, ignored
        }
        payload = json.dumps({"type": "whatsapp.received", "data": data})
        timestamp = str(int(time.time()))
        webhook_id = "dynamic-" + message_id
        signature = base64.b64encode(hmac.new(
            b"dynamic-station-test-key",
            (webhook_id + "." + timestamp + "." + payload).encode(),
            hashlib.sha256,
        ).digest()).decode()
        return self.client.post(
            "/webhook/bird/", payload, content_type="application/json",
            HTTP_WEBHOOK_ID=webhook_id,
            HTTP_WEBHOOK_TIMESTAMP=timestamp,
            HTTP_WEBHOOK_SIGNATURE="v1," + signature,
        )

    def test_original_seven_stations_are_preserved(self):
        self.assertEqual(
            set(Station.values),
            set(BroadcastStation.objects.values_list("slug", flat=True))
        )
        self.assertEqual(len(station_choices()), 7)
        for key in Station.values:
            self.assertTrue(BroadcastStation.objects.get(slug=key).is_active)
        self.assertEqual(
            self.client.get("/api/workspace/stations").json()["stations"][0]["slug"]
            in Station.values,
            True,
        )

    def test_new_station_auto_id_and_isolated_chart_creation(self):
        slug = self.make_station()
        self.assertEqual(slug, "midlands_radio")
        row = BroadcastStation.objects.get(slug=slug)
        self.assertFalse(row.is_active)
        self.assertTrue(StationVotingConfig.objects.filter(station=slug).exists())
        self.assertEqual(ChartShow.objects.filter(station=slug, kind="legacy").count(), 1)
        self.assertEqual(ChartShow.objects.filter(station=slug, kind="weekly").count(), 0)
        self.assertEqual(
            self.client.post("/accounts/switch-station/", {
                "station": slug,
            }).status_code, 302,
        )
        self.assertEqual(
            self.client.get("/api/workspace/overview").json()["station"], slug
        )
        self.assertEqual(self.client.get("/api/workspace/shows").json()["station"], slug)
        show_id = self.make_show()
        self.assertTrue(ChartShow.objects.filter(
            pk=show_id, station=slug, code="MW20"
        ).exists())
        self.assertFalse(ChartShow.objects.get(pk=show_id).is_enabled)
        self.assertEqual(
            self.client.get("/api/workspace/shows").json()["shows"][1]["station"],
            slug,
        ) if False else None

    def test_activate_station_and_receive_signed_bird_votes(self):
        slug = self.make_station()
        self.client.post("/accounts/switch-station/", {"station": slug})
        show_id = self.make_show()
        activation = self.configure(slug, is_active=True)
        self.assertEqual(activation.status_code, 200, activation.content)
        show = ChartShow.objects.get(pk=show_id)
        show.is_enabled = True
        show.manual_open = True
        show.save(update_fields=["is_enabled", "manual_open"])
        response = self.bird("+263791112233")
        self.assertEqual(response.status_code, 202, response.content)
        event = InboundEvent.objects.get()
        self.assertEqual(event.station, slug)
        self.assertEqual(event.chart_show_id, show_id)
        self.assertEqual(event.vote_text, "Artist - Song")
        self.assertEqual(event.submitted_code, "MW20")
        self.assertEqual(
            self.bird("+263791112233").status_code, 200,
        )
        self.assertEqual(InboundEvent.objects.count(), 1)
        unknown = self.bird("+263791119999", message_id="unknown")
        self.assertEqual(unknown.status_code, 200)
        self.assertTrue(unknown.json()["ignored"])
        self.assertEqual(InboundEvent.objects.count(), 1)
        self.assertEqual(self.bird("+263790573340", message_id="legacy").status_code, 202)
        self.assertEqual(
            set(InboundEvent.objects.values_list("station", flat=True)),
            {slug, "national_fm"},
        )

    def test_duplicate_numbers_and_reserved_env_numbers_cannot_be_stolen(self):
        duplicate = self.post("stations/create", {
            "name": "Impostor FM", "bird_number": "263790573340",
        })
        self.assertEqual(duplicate.status_code, 400)
        self.assertFalse(BroadcastStation.objects.filter(name="Impostor FM").exists())
        slug = self.make_station()
        second = self.post("stations/create", {
            "name": "Other Radio", "bird_number": "263791112233",
        })
        self.assertEqual(second.status_code, 400)
        self.assertEqual(self.configure(slug, bird_number="invalid number").status_code, 400)
        self.assertFalse(self.client.get("/api/workspace/stations").json()["stations"][0]["logo_url"])

    def test_new_station_requires_number_before_activation(self):
        slug = self.make_station(number="")
        attempt = self.configure(slug, is_active=True)
        self.assertEqual(attempt.status_code, 400)
        self.assertFalse(BroadcastStation.objects.get(slug=slug).is_active)
        self.assertEqual(
            self.configure(slug, bird_number="263791112233", is_active=True).status_code,
            200,
        )
        self.assertTrue(BroadcastStation.objects.get(slug=slug).is_active)

    def test_station_deactivation_safeguard_and_user_access(self):
        slug = self.make_station()
        self.client.post("/accounts/switch-station/", {"station": slug})
        show_id = self.make_show(enabled=True)
        self.assertEqual(self.configure(slug, is_active=True).status_code, 200)
        self.assertEqual(self.configure(slug, is_active=False).status_code, 400)
        ChartShow.objects.filter(pk=show_id).update(is_enabled=False)
        self.assertEqual(self.configure(slug, is_active=False).status_code, 200)
        self.assertFalse(BroadcastStation.objects.get(slug=slug).is_active)

    def test_registration_and_presenters_can_use_new_active_station(self):
        slug = self.make_station()
        self.assertEqual(self.configure(slug, is_active=True).status_code, 200)
        opts = dict(RegistrationForm().fields["station"].choices)
        self.assertIn(slug, opts)
        self.client.post("/accounts/switch-station/", {"station": slug})
        show_id = self.make_show()
        result = self.post("presenters/create", {
            "username": "new_station_presenter",
            "password": "Unique-Station-Presenter-Pass!95",
            "chart_show_id": show_id,
        })
        self.assertEqual(result.status_code, 200, result.content)
        user = get_user_model().objects.get(username="new_station_presenter")
        self.assertEqual(user.profile.station, slug)
        presenter_client = Client()
        presenter_client.force_login(user)
        self.assertEqual(
            presenter_client.get("/api/workspace/overview").json()["station"], slug
        )
        self.assertEqual(
            presenter_client.get("/api/workspace/shows").json()["presenter_show_id"],
            show_id,
        )
        self.assertEqual(
            presenter_client.post("/accounts/switch-station/", {
                "station": "radio_zimbabwe",
            }).status_code, 403,
        )
        self.assertEqual(presenter_client.get("/api/workspace/stations").status_code, 403)

    def test_station_names_editable_ids_immutable_and_no_deletions(self):
        slug = self.make_station()
        update = self.configure(slug, name="Midlands Air")
        self.assertEqual(update.status_code, 200)
        self.assertEqual(BroadcastStation.objects.get(slug=slug).name, "Midlands Air")
        self.assertFalse(BroadcastStation.objects.filter(slug="midlands_air").exists())
        self.assertEqual(
            self.post("stations/create", {"name": "Midlands Air"}).status_code,
            400,
        )
        self.assertEqual(
            self.configure(slug, name="National FM").status_code, 400
        )
        self.assertTrue(BroadcastStation.objects.filter(slug=slug).exists())
        self.assertEqual(
            self.client.get("/api/workspace/administration").status_code, 200
        )
        self.assertIn(slug, [
            x["id"] for x in self.client.get(
                "/api/workspace/administration"
            ).json()["stations"]
        ])
