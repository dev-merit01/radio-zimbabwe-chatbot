import logging
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Optional

import requests
from django.conf import settings

logger = logging.getLogger(__name__)


class TelegramClientError(RuntimeError):
    """Base Telegram client error."""


class TelegramConfigurationError(TelegramClientError):
    """Raised when Telegram is not configured."""


class TelegramClient:
    def __init__(self, token: str, session: Optional[requests.Session] = None):
        if not token:
            raise TelegramConfigurationError('Telegram bot token is not configured.')
        self.token = token
        self._session = session or requests.Session()
        self._base_url = f'https://api.telegram.org/bot{token}'

    def _post(self, method: str, payload: dict) -> dict:
        url = f'{self._base_url}/{method}'
        try:
            response = self._session.post(url, json=payload, timeout=10)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.exception('Telegram request failed: %s', exc)
            raise TelegramClientError(str(exc)) from exc

        try:
            data = response.json()
        except ValueError as exc:
            logger.exception('Telegram returned non-JSON response: %s', response.text)
            raise TelegramClientError('Telegram returned an invalid response') from exc

        if not data.get('ok'):
            description = data.get('description', 'Unknown Telegram error')
            logger.error('Telegram API error: %s', description)
            raise TelegramClientError(description)
        return data

    def send_text(self, chat_id: str, text: str, parse_mode: Optional[str] = None) -> dict:
        payload = {
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': True,
        }
        if parse_mode:
            payload['parse_mode'] = parse_mode
        logger.debug('Sending Telegram message to %s', chat_id)
        return self._post('sendMessage', payload)

    def set_webhook(self, url: str) -> dict:
        logger.info('Setting Telegram webhook to %s', url)
        return self._post('setWebhook', {'url': url})

    def delete_webhook(self) -> dict:
        logger.info('Removing Telegram webhook')
        return self._post('deleteWebhook', {})

    def get_me(self) -> dict:
        return self._post('getMe', {})


_client_lock: Lock = Lock()
_client_instance: Optional[TelegramClient] = None
_executor = ThreadPoolExecutor(max_workers=5)


def _resolve_token() -> str:
    token = getattr(settings, 'TELEGRAM_BOT_TOKEN', '')
    if not token:
        token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    if not token:
        raise TelegramConfigurationError('Telegram bot token is not configured.')
    return token


def get_client() -> TelegramClient:
    global _client_instance
    token = _resolve_token()
    with _client_lock:
        if _client_instance is None or _client_instance.token != token:
            _client_instance = TelegramClient(token)
    return _client_instance


def send_text(chat_id: str, text: str, parse_mode: Optional[str] = None) -> bool:
    try:
        client = get_client()
        client.send_text(chat_id, text, parse_mode=parse_mode)
        logger.info('Sent message to %s', chat_id)
        return True
    except TelegramClientError as exc:
        logger.error('Failed to send message to %s: %s', chat_id, exc)
        return False


def send_text_async(chat_id: str, text: str, parse_mode: Optional[str] = None) -> None:
    def _task():
        send_text(chat_id, text, parse_mode=parse_mode)

    _executor.submit(_task)
