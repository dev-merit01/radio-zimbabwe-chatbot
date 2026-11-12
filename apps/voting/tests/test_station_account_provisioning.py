"""Seven dedicated station logins: identity, scope and safe provisioning."""
import json
import os
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import Client, TestCase, override_settings

from apps.accounts.management.commands.provision_station_accounts import STATION_USERS
from apps.accounts.models import AccountProfile, Station


@override_settings(SECURE_SSL_REDIRECT=False)
class StationAccountProvisioningTests(TestCase):
    def payload(self):
        return [
            {
                "username": username,
                "station": station,
                "password": f"Testing-Unique-{index}-Secure!",
            }
            for index, (username, station) in enumerate(STATION_USERS.items())
        ]

    def provision(self, entries=None):
        with patch.dict(os.environ, {
            "AIRVOTE_STATION_ACCOUNTS_JSON": json.dumps(
                self.payload() if entries is None else entries
            )
        }):
            call_command("provision_station_accounts")

    def test_seven_accounts_sign_in_to_only_their_assigned_station(self):
        entries = self.payload()
        self.provision(entries)
        self.assertEqual(len(entries), 7)
        for entry in entries:
            with self.subTest(station=entry["station"]):
                user = get_user_model().objects.get(username=entry["username"])
                self.assertFalse(user.is_staff)
                self.assertFalse(user.is_superuser)
                self.assertTrue(user.is_active)
                self.assertNotEqual(user.password, entry["password"])
                self.assertTrue(user.check_password(entry["password"]))
                self.assertEqual(user.profile.station, entry["station"])
                client = Client()
                self.assertTrue(client.login(
                    username=entry["username"], password=entry["password"]
                ))
                # Dedicated station credentials are not presenter identities.
                # No station's chart shows or votes are exposed until an admin
                # assigns this account to a specific programme.
                shows = client.get("/api/workspace/shows")
                self.assertEqual(shows.status_code, 200)
                self.assertEqual(shows.json()["station"], entry["station"])
                self.assertTrue(shows.json()["assignment_required"])
                self.assertEqual(shows.json()["shows"], [])
                self.assertEqual(
                    client.get("/api/workspace/overview").status_code, 403
                )
                self.assertEqual(
                    client.get("/api/workspace/overview?station=radio_zimbabwe").status_code,
                    403,
                )
        self.assertEqual(
            AccountProfile.objects.filter(
                user__username__in=STATION_USERS
            ).count(), 7
        )
        self.assertIn(Station.LUZIBO_RADIO, Station.values)

    def test_idempotent_provisioning_never_adds_duplicate_accounts(self):
        self.provision()
        self.provision()
        self.assertEqual(
            get_user_model().objects.filter(username__in=STATION_USERS).count(), 7
        )

    def test_incomplete_configuration_does_not_create_partial_accounts(self):
        with self.assertRaises(CommandError):
            self.provision(self.payload()[:6])
        self.assertFalse(
            get_user_model().objects.filter(username__in=STATION_USERS).exists()
        )

    def test_existing_superuser_username_cannot_be_overwritten(self):
        admin = get_user_model().objects.create_superuser(
            username="classic", password="Existing-Secure-Admin!"
        )
        original = admin.password
        with self.assertRaises(CommandError):
            self.provision()
        admin.refresh_from_db()
        self.assertEqual(admin.password, original)
        self.assertTrue(admin.is_superuser)
        self.assertFalse(
            get_user_model().objects.filter(username="radiozim").exists()
        )
