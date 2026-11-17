"""Revoke a company's licence without destroying votes or station records."""
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from apps.licensing.models import BroadcastCompany, CompanyLicence


class Command(BaseCommand):
    help = "Revoke a signed AirVote company licence."

    def add_arguments(self, parser):
        parser.add_argument("company_slug")
        parser.add_argument("--reason", required=True)

    def handle(self, *args, **options):
        slug = options["company_slug"]
        company = BroadcastCompany.objects.filter(slug=slug).first()
        if company is None:
            raise CommandError("Unknown broadcasting company.")
        reason = options["reason"][:200]
        updated = CompanyLicence.objects.filter(company=company).update(
            revoked_at=timezone.now(), revocation_reason=reason
        )
        if not updated:
            bootstrap = getattr(settings, "AIRVOTE_COMPANY_BOOTSTRAP_LICENCES", {})
            signed_token = bootstrap.get(slug) if isinstance(bootstrap, dict) else None
            if not signed_token:
                raise CommandError("No licence installed for this company.")
            CompanyLicence.objects.create(
                company=company, signed_token=signed_token,
                revoked_at=timezone.now(), revocation_reason=reason,
            )
        self.stdout.write(self.style.SUCCESS(f"Revoked company licence for {company.name}."))
