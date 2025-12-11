import asyncio
import logging

from asgiref.sync import sync_to_async
from django.conf import settings
from django.core.management.base import BaseCommand
from telegram.ext import ApplicationBuilder, MessageHandler, filters

from apps.voting.services import VotingService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run the Telegram bot using long polling (development helper).'

    def handle(self, *args, **options):
        token = getattr(settings, 'TELEGRAM_BOT_TOKEN', '')
        if not token:
            self.stderr.write('TELEGRAM_BOT_TOKEN is not configured.')
            return

        self.stdout.write('Starting Telegram polling bot. Press CTRL+C to stop.')
        asyncio.run(self._run_bot(token))

    async def _run_bot(self, token: str):
        application = ApplicationBuilder().token(token).build()

        async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
            message = update.effective_message
            chat = update.effective_chat
            if message is None or chat is None:
                return

            text = (message.text or message.caption or '').strip()
            chat_id = str(chat.id)
            logger.info('Polling update chat_id=%s text=%s', chat_id, text[:50] if text else '(empty)')
            try:
                response = await sync_to_async(self._route_message)(chat_id, text)
                logger.info('Response generated: %s', response[:50] if response else '(empty)')
                await context.bot.send_message(chat_id=chat.id, text=response, disable_web_page_preview=True)
                logger.info('Message sent successfully to chat_id=%s', chat_id)
            except Exception as e:
                logger.exception('Error handling message: %s', e)
                try:
                    await context.bot.send_message(chat_id=chat.id, text="Sorry, something went wrong. Please try again.")
                except Exception as send_error:
                    logger.exception('Failed to send error message: %s', send_error)

        application.add_handler(MessageHandler(filters.ALL, handle_message))

        async with application:
            await application.start()
            await application.updater.start_polling()
            self.stdout.write('Bot is running. Press CTRL+C to stop.')
            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass
            finally:
                await application.updater.stop()
                await application.stop()

    @staticmethod
    def _route_message(chat_id: str, text: str) -> str:
        service = VotingService(channel='telegram', user_ref=chat_id)
        return service.handle_incoming_text(text)
