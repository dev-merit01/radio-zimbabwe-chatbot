from django.core.management.base import BaseCommand, CommandError

class Command(BaseCommand):
    help = 'Polling is disabled in the centrally hosted v2 deployment.'
    def handle(self, *args, **options):
        raise CommandError('Configure the authenticated Telegram webhook and run_vote_worker. Do not run a second receiver for the same bot.')
