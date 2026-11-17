from django.core.management.base import BaseCommand
from apps.public_voting.puller import poll_once, is_configured


class Command(BaseCommand):
    help = "Run one safe pull from the isolated public-voting holding area."

    def handle(self, *args, **kwargs):
        if not is_configured():
            self.stdout.write("Public-voting gateway is disabled (not configured).")
            return
        accepted = poll_once()
        self.stdout.write(f"Processed {accepted} gateway receipts.")
