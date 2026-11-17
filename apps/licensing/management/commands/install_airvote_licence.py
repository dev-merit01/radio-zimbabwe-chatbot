"""Install a vendor-signed station licence without any signing secrets on the server."""
from pathlib import Path
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from apps.accounts.models import station_exists
from apps.licensing.models import StationLicence
from apps.licensing.verification import LicenceError, decode_and_verify


class Command(BaseCommand):
    help = "Install a signed Kuiper Data Labs .lic file for one AirVote station."

    def add_arguments(self, parser):
        parser.add_argument("licence_file", type=Path)

    def handle(self, *args, **options):
        key = settings.AIRVOTE_LICENCE_PUBLIC_KEY
        if not key:
            raise CommandError("AIRVOTE_LICENCE_PUBLIC_KEY must be configured first.")
        try:
            token = options["licence_file"].read_text(encoding="utf-8").strip()
            claims = decode_and_verify(token, key)
        except (OSError, UnicodeError, LicenceError) as exc:
            raise CommandError("Licence file is unreadable or invalid.") from exc
        station = claims["station"]
        if not station_exists(station):
            raise CommandError("Create the licensed station before installing its licence.")
        with transaction.atomic():
            record = StationLicence.objects.select_for_update().filter(station=station).first()
            if record and record.revoked_at:
                if datetime.fromisoformat(claims["issued_at"]) <= record.revoked_at:
                    raise CommandError("Licence predates revocation. Obtain a newly issued licence.")
            StationLicence.objects.update_or_create(
                station=station,
                defaults={"signed_token": token, "revoked_at": None, "revocation_reason": ""},
            )
        self.stdout.write(self.style.SUCCESS(
            f"Licence installed for {station}: {claims['customer']} ({claims['plan']}), "
            f"valid through {claims['expires_on']} UTC."
        ))
