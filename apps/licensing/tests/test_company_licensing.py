"""Company licences cover all owned stations and never transfer across companies."""
import base64
from datetime import timedelta
import json
import uuid

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.accounts.models import BroadcastStation
from apps.licensing.models import BroadcastCompany, CompanyLicence, CompanyStation
from apps.licensing.company_verification import (
    company_licence_status, company_status_for_station, decode_company_licence,
)
from apps.licensing.verification import LicenceError, licence_status

INSTALLATION = "31b64b0f-6efb-496f-bf4a-f5136632dff1"


def b64(data):
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


@override_settings(
    SECURE_SSL_REDIRECT=False,
    AIRVOTE_COMPANY_LICENSING_ENFORCED=True,
    AIRVOTE_LICENCE_INSTALLATION_ID=INSTALLATION,
    AIRVOTE_COMPANY_BOOTSTRAP_LICENCES={},
)
class CompanyWideLicensingTests(TestCase):
    def setUp(self):
        self.company, _ = BroadcastCompany.objects.get_or_create(
            slug="existing-airvote", defaults={"name": "Existing broadcaster"},
        )
        for slug in ("national_fm", "power_fm", "radio_zimbabwe"):
            BroadcastStation.objects.get_or_create(
                slug=slug, defaults={"name": slug, "is_active": True}
            )
            CompanyStation.objects.update_or_create(
                station_slug=slug, defaults={"company": self.company},
            )
        self.rival = BroadcastCompany.objects.create(
            slug="another-broadcaster", name="Another Broadcaster"
        )
        BroadcastStation.objects.create(slug="other_radio", name="Other Radio", is_active=True)
        CompanyStation.objects.create(station_slug="other_radio", company=self.rival)
        self.private = Ed25519PrivateKey.generate()
        self.public = base64.b64encode(self.private.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw,
        )).decode()
        self.admin = get_user_model().objects.create_superuser(
            username="company-licence-test", password="UnitTestOnlyCompanyPassword99!"
        )
        self.client.force_login(self.admin)
        self.today = timezone.now().date()

    def token(self, *, company="existing-airvote", installation=INSTALLATION,
              end_offset=30, start_offset=-30, grace=3):
        claims = {
            "issuer": "kuiperdatalabs.co.zw", "audience": "airvote-company",
            "licence_id": str(uuid.uuid4()), "company": company,
            "installation_id": installation, "customer": "Example Licensed Company",
            "plan": "enterprise",
            "starts_on": (self.today + timedelta(days=start_offset)).isoformat(),
            "expires_on": (self.today + timedelta(days=end_offset)).isoformat(),
            "grace_days": grace, "issued_at": timezone.now().isoformat(),
        }
        raw = json.dumps(claims, sort_keys=True, separators=(",", ":")).encode()
        return f"AV2.{b64(raw)}.{b64(self.private.sign(raw))}"

    def test_one_licence_unblocks_all_company_stations_but_not_rival(self):
        token = self.token()
        with override_settings(AIRVOTE_LICENCE_PUBLIC_KEY=self.public,
                               AIRVOTE_COMPANY_BOOTSTRAP_LICENCES={
                                   "existing-airvote": token,
                               }):
            for station in ("national_fm", "power_fm", "radio_zimbabwe"):
                self.assertTrue(licence_status(station)["permitted"], station)
                self.assertEqual(licence_status(station)["company"], "existing-airvote")
            self.assertEqual(licence_status("other_radio")["state"], "unlicensed")
            self.assertFalse(licence_status("other_radio")["permitted"])
            BroadcastStation.objects.create(slug="new_company_fm", name="New Company FM")
            CompanyStation.objects.create(
                station_slug="new_company_fm", company=self.company
            )
            self.assertTrue(licence_status("new_company_fm")["permitted"])

    def test_signature_tampering_and_cross_installation_rejected(self):
        valid = self.token()
        with override_settings(AIRVOTE_LICENCE_PUBLIC_KEY=self.public):
            self.assertEqual(decode_company_licence(valid, self.public)["company"], "existing-airvote")
            bad = valid[:-3] + ("X" if valid[-3] != "X" else "Y") + valid[-2:]
            with self.assertRaises(LicenceError):
                decode_company_licence(bad, self.public)
            swapped = self.token(installation="df999e7e-75d7-4cf7-a0bc-af4c968a6aa8")
            response = self.client.post("/licensing/activate/", {"licence_token": swapped})
            self.assertEqual(response.status_code, 400)
            self.assertFalse(CompanyLicence.objects.filter(company=self.company).exists())
            self.assertEqual(company_licence_status("existing-airvote")["state"], "unlicensed")

    def test_expiry_grace_and_revocation_apply_to_every_station(self):
        with override_settings(AIRVOTE_LICENCE_PUBLIC_KEY=self.public):
            licence = CompanyLicence.objects.create(
                company=self.company, signed_token=self.token(end_offset=-1),
            )
            self.assertEqual(licence_status("national_fm")["state"], "grace")
            self.assertEqual(licence_status("power_fm")["state"], "grace")
            licence.signed_token = self.token(end_offset=-10)
            licence.save(update_fields=["signed_token"])
            self.assertEqual(licence_status("power_fm")["state"], "expired")
            licence.signed_token = self.token()
            licence.save(update_fields=["signed_token"])
            call_command("revoke_airvote_licence", self.company.slug, reason="Contract ended")
            self.assertEqual(licence_status("national_fm")["state"], "revoked")
            self.assertEqual(licence_status("power_fm")["state"], "revoked")

    def test_company_activation_rejects_station_token_and_accepts_one_av2(self):
        with override_settings(AIRVOTE_LICENCE_PUBLIC_KEY=self.public):
            bad = self.client.post("/licensing/activate/", {
                "licence_token": "AV1.invalid.station",
            })
            self.assertEqual(bad.status_code, 400)
            good = self.client.post("/licensing/activate/", {
                "licence_token": self.token()
            })
            self.assertEqual(good.status_code, 200)
            self.assertEqual(CompanyLicence.objects.count(), 1)
            self.assertTrue(licence_status("national_fm")["permitted"])
            self.assertTrue(licence_status("power_fm")["permitted"])
            self.assertEqual(self.client.get("/api/licensing/status").json()["scope"], "company")

    def test_bootstrap_revocation_cannot_fall_back_to_signed_grant(self):
        token = self.token()
        with override_settings(AIRVOTE_LICENCE_PUBLIC_KEY=self.public,
                               AIRVOTE_COMPANY_BOOTSTRAP_LICENCES={
                                   "existing-airvote": token,
                               }):
            self.assertTrue(company_licence_status(self.company.slug)["permitted"])
            call_command("revoke_airvote_licence", self.company.slug, reason="Revoked test")
            self.assertEqual(company_licence_status(self.company.slug)["state"], "revoked")

    def test_company_disabled_and_unassigned_station_fail_closed(self):
        with override_settings(AIRVOTE_LICENCE_PUBLIC_KEY=self.public,
                               AIRVOTE_COMPANY_BOOTSTRAP_LICENCES={
                                   "existing-airvote": self.token()
                               }):
            self.company.is_active = False
            self.company.save(update_fields=["is_active"])
            self.assertFalse(company_status_for_station("national_fm")["permitted"])
            BroadcastStation.objects.create(slug="orphan_radio", name="Orphan Radio")
            self.assertEqual(company_status_for_station("orphan_radio")["state"],
                             "unassigned_company")

    def test_staff_gating_restricts_others_while_preserving_webhooks(self):
        with override_settings(AIRVOTE_LICENCE_PUBLIC_KEY=self.public,
                               AIRVOTE_COMPANY_BOOTSTRAP_LICENCES={
                                   "existing-airvote": self.token()
                               }):
            self.assertNotEqual(self.client.get("/").status_code, 402)
            self.client.post("/accounts/switch-station/", {"station": "other_radio"})
            blocked = self.client.get("/api/workspace/overview")
            self.assertEqual(blocked.status_code, 402)
            self.assertEqual(blocked.json()["code"], "LICENCE_REQUIRED")
            self.assertNotEqual(self.client.get("/webhook/bird/").status_code, 402)

    def test_admin_station_creation_assigns_company_and_shares_licence(self):
        with override_settings(AIRVOTE_LICENCE_PUBLIC_KEY=self.public,
                               AIRVOTE_COMPANY_BOOTSTRAP_LICENCES={
                                   "existing-airvote": self.token()
                               }):
            response = self.client.post(
                "/api/workspace/stations/create",
                json.dumps({"name": "Company New Radio", "company_slug": self.company.slug,
                            "bird_number": ""}),
                content_type="application/json",
            )
            self.assertEqual(response.status_code, 201, response.content)
            slug = response.json()["station"]["slug"]
            self.assertEqual(CompanyStation.objects.get(station_slug=slug).company, self.company)
            self.assertTrue(licence_status(slug)["permitted"])
            self.assertFalse(CompanyLicence.objects.exists())


    def test_company_admin_can_create_company_and_assign_new_station(self):
        with override_settings(
            AIRVOTE_LICENCE_PUBLIC_KEY=self.public,
            AIRVOTE_COMPANY_BOOTSTRAP_LICENCES={
                "existing-airvote": self.token()
            },
        ):
            new_company = self.client.post(
                "/api/workspace/companies/create",
                json.dumps({"name": "Regional Broadcasting Network"}),
                content_type="application/json",
            )
            self.assertEqual(new_company.status_code, 201, new_company.content)
            slug = new_company.json()["company"]["slug"]
            self.assertEqual(slug, "regional-broadcasting-network")
            station = self.client.post(
                "/api/workspace/stations/create",
                json.dumps({"name": "Regional News Radio", "company_slug": slug,
                            "bird_number": ""}),
                content_type="application/json",
            )
            self.assertEqual(station.status_code, 201, station.content)
            station_slug = station.json()["station"]["slug"]
            self.assertEqual(licence_status(station_slug)["state"], "unlicensed")
            company_token = self.token(company=slug)
            activated = self.client.post(
                "/licensing/activate/", {"licence_token": company_token},
            )
            self.assertEqual(activated.status_code, 200, activated.content)
            self.assertTrue(licence_status(station_slug)["permitted"])
            self.assertEqual(CompanyLicence.objects.count(), 1)
            self.assertTrue(licence_status("national_fm")["permitted"])

    def test_presenters_cannot_create_or_reassign_companies(self):
        presenter = get_user_model().objects.create_user(
            username="company-presenter", password="UnitTestCompanyPresenterOnly99!",
        )
        self.client.force_login(presenter)
        forbidden = self.client.post(
            "/api/workspace/companies/create",
            json.dumps({"name": "Unapproved Broadcaster"}),
            content_type="application/json",
        )
        self.assertEqual(forbidden.status_code, 403)
        self.assertFalse(BroadcastCompany.objects.filter(
            slug="unapproved-broadcaster"
        ).exists())
