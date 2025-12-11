from django.core.management.base import BaseCommand

from apps.bot.telegram_client import get_client, TelegramClientError

class Command(BaseCommand):
    help = 'Test Telegram bot token'

    def handle(self, *args, **options):
        try:
            client = get_client()
            data = client.get_me()
        except TelegramClientError as exc:
            self.stderr.write(f'Telegram test failed: {exc}')
            return

        result = data.get('result', {})
        self.stdout.write(
            f"Bot info: {result.get('first_name', 'Unknown')} (@{result.get('username')})"
        )