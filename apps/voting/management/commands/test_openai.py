from django.core.management.base import BaseCommand, CommandError
from apps.voting.ai import call_openai_api


class Command(BaseCommand):
    help = "Test configured AI access without vote or listener data."

    def handle(self, *args, **options):
        try:
            call_openai_api("Reply with OK.", "Connection test.")
        except Exception as exc:
            raise CommandError("AI connection failed: " + type(exc).__name__) from None
        self.stdout.write("AI connection succeeded.")
