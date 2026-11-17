"""Locally revoke a licence. A new vendor-signed token is needed to reinstate."""
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from apps.licensing.models import StationLicence


class Command(BaseCommand):
    help = "Revoke an AirVote station licence (does not delete station votes)."

    def add_arguments(self, parser):
        parser.add_argument("station")
        parser.add_argument("--reason", required=True, max_length=200)

    def handle(self, *args, **options):
        updated = StationLicence.objects.filter(station=options["station"]).update(
            revoked_at=timezone.now(), revocation_reason=options["reason"][:200]
        )
        if not updated:
            raise CommandError("No licence is installed for that station.")
        self.stdout.write(self.style.SUCCESS(
            f"Revoked licence for {options['station']}."
        ))
