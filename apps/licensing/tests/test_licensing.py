import base64
import json
from datetime import timedelta
import uuid

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.licensing.models import StationLicence
from apps.licensing.verification import LicenceError, decode_and_verify, licence_status


def _b64(data):
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


class AirVoteLicensingTests(TestCase):
    def setUp(self):
        self.private = Ed25519PrivateKey.generate()
        self.public = base64.b64encode(self.private.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        )).decode()
        self.admin = get_user_model().objects.create_superuser(
            username="licence-test-admin", password="Only-A-Unit-Test-Password-2026!"
        )
        self.client.force_login(self.admin)
        self.today = timezone.now().date()

    def token(self, station="radio_zimbabwe", *, expiry_days=30, grace_days=3):
        claims = {
            "issuer": "kuiperdatalabs.co.zw", "audience": "airvote",
            "licence_id": str(uuid.uuid4()), "station": station,
            "customer": "Demo Broadcaster", "plan": "annual",
            "starts_on": (self.today - timedelta(days=1)).isoformat(),
            "expires_on": (self.today + timedelta(days=expiry_days)).isoformat(),
            "grace_days": grace_days,
        }
        raw = json.dumps(claims, separators=(",", ":"), sort_keys=True).encode()
        return "AV1." + _b64(raw) + "." + _b64(self.private.sign(raw))

    def test_signature_and_station_cannot_be_forged(self):
        token = self.token()
        self.assertEqual(decode_and_verify(token, self.public)["station"], "radio_zimbabwe")
        altered = token[:-2] + ("A" if token[-2] != "A" else "B") + token[-1]
        with self.assertRaises(LicenceError):
            decode_and_verify(altered, self.public)
        with override_settings(AIRVOTE_LICENCE_PUBLIC_KEY=self.public):
            StationLicence.objects.create(station="national_fm", signed_token=token)
            self.assertEqual(licence_status("national_fm")["state"], "invalid")

    def test_status_active_grace_expired_and_revoked(self):
        with override_settings(AIRVOTE_LICENCE_PUBLIC_KEY=self.public):
            record = StationLicence.objects.create(
                station="radio_zimbabwe", signed_token=self.token(expiry_days=1)
            )
            self.assertEqual(licence_status("radio_zimbabwe")["state"], "active")
            record.signed_token = self.token(expiry_days=-1, grace_days=3)
            record.save()
            self.assertEqual(licence_status("radio_zimbabwe")["state"], "grace")
            record.signed_token = self.token(expiry_days=-5, grace_days=3)
            record.save()
            self.assertEqual(licence_status("radio_zimbabwe")["state"], "expired")
            record.revoked_at = timezone.now()
            record.save()
            self.assertEqual(licence_status("radio_zimbabwe")["state"], "revoked")

    def test_enforcement_blocks_staff_but_preserves_intake_and_health(self):
        with override_settings(
            AIRVOTE_LICENSING_ENFORCED=True, AIRVOTE_LICENCE_PUBLIC_KEY=self.public,
            SECURE_SSL_REDIRECT=False,
        ):
            blocked = self.client.get("/")
            self.assertEqual(blocked.status_code, 402)
            self.assertContains(blocked, "licence", status_code=402)
            api = self.client.get("/api/workspace/overview")
            self.assertEqual(api.status_code, 402)
            self.assertEqual(api.json()["code"], "LICENCE_REQUIRED")
            self.assertEqual(self.client.get("/api/desktop/status").status_code, 200)
            self.assertNotEqual(self.client.get("/webhook/bird/").status_code, 402)
            StationLicence.objects.create(
                station="radio_zimbabwe", signed_token=self.token()
            )
            self.assertNotEqual(self.client.get("/").status_code, 402)
            self.assertTrue(self.client.get("/api/licensing/status").json()["permitted"])

    def test_feature_disabled_leaves_existing_staff_access_unchanged(self):
        with override_settings(AIRVOTE_LICENSING_ENFORCED=False, SECURE_SSL_REDIRECT=False):
            self.assertNotEqual(self.client.get("/").status_code, 402)

    def test_activation_accepts_vendor_token_and_rejects_revoked_reuse(self):
        token = self.token()
        with override_settings(
            AIRVOTE_LICENSING_ENFORCED=True, AIRVOTE_LICENCE_PUBLIC_KEY=self.public,
            SECURE_SSL_REDIRECT=False,
        ):
            response = self.client.post("/licensing/activate/", {"licence_token": token})
            self.assertEqual(response.status_code, 200)
            self.assertTrue(StationLicence.objects.filter(station="radio_zimbabwe").exists())
            record = StationLicence.objects.get(station="radio_zimbabwe")
            record.revoked_at = timezone.now()
            record.save(update_fields=["revoked_at"])
            response = self.client.post("/licensing/activate/", {"licence_token": token})
            self.assertContains(response, "revoked")
            self.assertIsNotNone(StationLicence.objects.get(station="radio_zimbabwe").revoked_at)
