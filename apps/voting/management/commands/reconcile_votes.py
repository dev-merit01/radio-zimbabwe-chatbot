"""Recount canonical totals after deployment without rewriting published charts."""

from django.core.management.base import BaseCommand
from apps.accounts.models import Station
from apps.voting.models import ReviewAudit
from apps.voting.pipeline import rebuild_tallies
from django.db import transaction


class Command(BaseCommand):
    help = "Rebuild one station’s live totals, excluding known duplicate listener/day/song votes."

    def add_arguments(self, parser):
        parser.add_argument("--station", required=True, choices=Station.values)

    def handle(self, *args, **options):
        station = options["station"]
        with transaction.atomic():
            rebuild_tallies(station)
            ReviewAudit.objects.create(
                station=station,
                action="reconcile_votes",
                details={"source": "management_command"},
            )
        self.stdout.write(
            self.style.SUCCESS(
                "Live totals reconciled; raw submissions and published archives preserved."
            )
        )
