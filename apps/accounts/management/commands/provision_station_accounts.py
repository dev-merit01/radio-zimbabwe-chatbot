"""One-off, idempotent provisioning of station-scoped accounts.

Pass account passwords through the private Railway variable
AIRVOTE_STATION_ACCOUNTS_JSON, not through source files or command arguments.
Never print submitted passwords or include them in exception messages.
"""
import json
import os

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from apps.accounts.models import AccountProfile, Station


STATION_USERS = {
    "radiozim": Station.RADIO_ZIMBABWE,
    "nationalfm": Station.NATIONAL_FM,
    "powerfm": Station.POWER_FM,
    "classic": Station.CLASSIC_263,
    "khulumanifm": Station.KHULUMANI_FM,
    "centralradio": Station.CENTRAL_RADIO,
    "luziboradio": Station.LUZIBO_RADIO,
}


class Command(BaseCommand):
    help = "Provision seven assigned station accounts from a private one-time environment variable."

    def handle(self, *args, **options):
        raw = os.environ.get("AIRVOTE_STATION_ACCOUNTS_JSON", "")
        if not raw:
            raise CommandError("Station account provisioning variable is not set.")
        try:
            entries = json.loads(raw)
        except (ValueError, TypeError) as exc:
            raise CommandError("Invalid private station accounts configuration.") from exc
        if not isinstance(entries, list) or len(entries) != len(STATION_USERS):
            raise CommandError("Expected exactly seven station accounts.")
        passwords = {}
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {
                "username", "station", "password"
            }:
                raise CommandError("Invalid account fields.")
            username = entry["username"]
            station = entry["station"]
            password = entry["password"]
            if (
                not isinstance(username, str)
                or not isinstance(station, str)
                or not isinstance(password, str)
                or STATION_USERS.get(username) != station
                or username in passwords
                or not 8 <= len(password) <= 128
            ):
                raise CommandError("Invalid account or station assignment.")
            passwords[username] = password
        if set(passwords) != set(STATION_USERS):
            raise CommandError("Station accounts list is incomplete.")

        User = get_user_model()
        with transaction.atomic():
            # First validate every existing username, avoiding partial writes.
            existing = {}
            for username, station in STATION_USERS.items():
                matches = list(
                    User.objects.select_for_update().filter(username__iexact=username)
                )
                if len(matches) > 1:
                    raise CommandError("Duplicate case-insensitive account name.")
                user = matches[0] if matches else None
                if user and (user.is_superuser or user.is_staff):
                    raise CommandError("A privileged account has a reserved station username.")
                if user and user.username != username:
                    raise CommandError("A case-variant account already exists.")
                if user and hasattr(user, "profile") and user.profile.station != station:
                    raise CommandError("A station username is already assigned elsewhere.")
                existing[username] = user

            for username, station in STATION_USERS.items():
                user = existing[username]
                if user is None:
                    user = User(username=username, is_active=True, is_staff=False,
                                is_superuser=False)
                user.set_password(passwords[username])
                user.is_active = True
                user.save()
                AccountProfile.objects.update_or_create(
                    user=user, defaults={"station": station}
                )
        self.stdout.write(
            self.style.SUCCESS("Seven station accounts provisioned and assigned.")
        )
