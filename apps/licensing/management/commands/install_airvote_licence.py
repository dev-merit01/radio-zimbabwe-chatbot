"""Install one Kuiper-issued AV2 company licence. Keep signing keys OFF Railway."""
from datetime import datetime
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from apps.licensing.company_verification import decode_company_licence
from apps.licensing.models import BroadcastCompany, CompanyLicence
from apps.licensing.verification import LicenceError


class Command(BaseCommand):
    help = "Install a signed company-wide AirVote commercial licence file."

    def add_arguments(self, parser):
        parser.add_argument("licence_file", type=Path)

    def handle(self, *args, **options):
        if not settings.AIRVOTE_LICENCE_PUBLIC_KEY or not settings.AIRVOTE_LICENCE_INSTALLATION_ID:
            raise CommandError("Configure the public signing key and installation ID.")
        try:
            token = options["licence_file"].read_text(encoding="utf-8").strip()
            claims = decode_company_licence(token, settings.AIRVOTE_LICENCE_PUBLIC_KEY)
        except (OSError, UnicodeError, LicenceError) as exc:
            raise CommandError("Company licence is unreadable or invalid.") from exc
        if claims["installation_id"] != settings.AIRVOTE_LICENCE_INSTALLATION_ID:
            raise CommandError("This company licence belongs to a different installation.")
        with transaction.atomic():
            company = BroadcastCompany.objects.select_for_update().filter(
                slug=claims["company"]
            ).first()
            if company is None:
                raise CommandError("Create the broadcasting company before installation.")
            current = CompanyLicence.objects.select_for_update().filter(company=company).first()
            if current and current.revoked_at is not None:
                if datetime.fromisoformat(claims["issued_at"]) <= current.revoked_at:
                    raise CommandError("Licence predates company revocation.")
            CompanyLicence.objects.update_or_create(
                company=company,
                defaults={"signed_token": token, "revoked_at": None, "revocation_reason": ""},
            )
        self.stdout.write(self.style.SUCCESS(
            f"Licence installed for {company.name}: {claims['plan']} through {claims['expires_on']}."
        ))
