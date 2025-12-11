from django.core.management.base import BaseCommand

from apps.bot.telegram_client import get_client, TelegramClientError

class Command(BaseCommand):
    help = 'Set Telegram webhook URL'

    def add_arguments(self, parser):
        parser.add_argument('webhook_url', type=str, help='The webhook URL to set')

    def handle(self, *args, **options):
        try:
            client = get_client()
            client.set_webhook(options['webhook_url'])
        except TelegramClientError as exc:
            self.stderr.write(f'Failed to set webhook: {exc}')
            return
        self.stdout.write('Webhook set successfully')