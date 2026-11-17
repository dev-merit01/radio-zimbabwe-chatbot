"""Locally revoke a licence. A new vendor-signed token is needed to reinstate."""
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from apps.licensing.models import StationLicence
from django.conf import settings


class Command(BaseCommand):
    help = "Revoke an AirVote station licence (does not delete station votes)."

    def add_arguments(self, parser):
        parser.add_argument("station")
        parser.add_argument("--reason", required=True)

    def handle(self, *args, **options):
        station = options["station"]
        # Revoking a bootstrap grant creates a DB override; otherwise the
        # configured signed token would take effect again on the next request.
        updated = StationLicence.objects.filter(station=station).update(
            revoked_at=timezone.now(), revocation_reason=options["reason"][:200]
        )
        if not updated:
            bootstrap = getattr(settings, "AIRVOTE_BOOTSTRAP_LICENCES", {})
            token = bootstrap.get(station) if isinstance(bootstrap, dict) else None
            if not token:
                raise CommandError("No licence is installed for that station.")
            StationLicence.objects.create(
                station=station, signed_token=token,
                revoked_at=timezone.now(), revocation_reason=options["reason"][:200],
            )
        self.stdout.write(self.style.SUCCESS(
            f"Revoked licence for {options['station']}."
        ))
