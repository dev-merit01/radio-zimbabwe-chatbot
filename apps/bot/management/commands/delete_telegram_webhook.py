from django.core.management.base import BaseCommand

from apps.bot.telegram_client import get_client, TelegramClientError


class Command(BaseCommand):
    help = 'Remove the Telegram webhook (useful before switching to polling).'

    def handle(self, *args, **options):
        try:
            client = get_client()
            client.delete_webhook()
        except TelegramClientError as exc:
            self.stderr.write(f'Failed to delete webhook: {exc}')
            return
        self.stdout.write('Webhook deleted successfully')
