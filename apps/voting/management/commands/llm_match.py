from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Retired legacy command; use the v2 worker and audited workspace."

    def handle(self, *args, **options):
        raise CommandError(
            "llm_match is retired. Use run_vote_worker, the review workspace, or a database backup/restore for maintenance."
        )
