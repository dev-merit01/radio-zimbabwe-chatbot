"""
OneMsg.io WhatsApp Client

This module provides a client for sending WhatsApp messages via the OneMsg.io API.
API Docs: https://onemsg.io/api-documents/
Webhook Docs: https://onemsg.io/webhook/
"""
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Optional

import requests
from django.conf import settings

logger = logging.getLogger(__name__)


class WhatsAppClientError(RuntimeError):
    """Base WhatsApp client error."""


class WhatsAppConfigurationError(WhatsAppClientError):
    """Raised when WhatsApp/OneMsg is not configured."""


class WhatsAppClient:
    """
    Client for OneMsg.io WhatsApp API.
    
    Required settings:
        ONEMSG_APP_KEY: Your OneMsg application key
        ONEMSG_AUTH_KEY: Your OneMsg authentication key
    """
    
    BASE_URL = 'https://app.onemsg.io/api'
    
    def __init__(self, app_key: str, auth_key: str, session: Optional[requests.Session] = None):
        if not app_key or not auth_key:
            raise WhatsAppConfigurationError('OneMsg app_key and auth_key are required.')
        self.app_key = app_key
        self.auth_key = auth_key
        self._session = session or requests.Session()

    def _post(self, endpoint: str, payload: dict) -> dict:
        """Make a POST request to the OneMsg API."""
        url = f'{self.BASE_URL}/{endpoint}'
        
        # Add authentication to payload
        payload['appkey'] = self.app_key
        payload['authkey'] = self.auth_key
        
        try:
            response = self._session.post(url, data=payload, timeout=15)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.exception('OneMsg request failed: %s', exc)
            raise WhatsAppClientError(str(exc)) from exc

        try:
            data = response.json()
        except ValueError as exc:
            logger.exception('OneMsg returned non-JSON response: %s', response.text)
            raise WhatsAppClientError('OneMsg returned an invalid response') from exc

        # Check for API errors
        if data.get('status') == 'error' or data.get('success') is False:
            error_msg = data.get('message', data.get('error', 'Unknown OneMsg error'))
            logger.error('OneMsg API error: %s', error_msg)
            raise WhatsAppClientError(error_msg)
            
        return data

    def send_text(self, phone_number: str, text: str) -> dict:
        """
        Send a text message to a WhatsApp number.
        
        Args:
            phone_number: The recipient's phone number with country code (e.g., "263771234567")
            text: The message text to send
            
        Returns:
            API response dict
        """
        payload = {
            'to': phone_number,
            'message': text,
            'sandbox': 'false',
        }
        logger.debug('Sending WhatsApp message to %s', phone_number)
        return self._post('create-message', payload)
    
    def send_image(self, phone_number: str, image_url: str, caption: str = '') -> dict:
        """
        Send an image message to a WhatsApp number.
        
        Args:
            phone_number: The recipient's phone number with country code
            image_url: URL of the image to send
            caption: Optional caption for the image
            
        Returns:
            API response dict
        """
        payload = {
            'to': phone_number,
            'image': image_url,
            'caption': caption,
            'sandbox': 'false',
        }
        logger.debug('Sending WhatsApp image to %s', phone_number)
        return self._post('create-message', payload)
    
    def check_number_exists(self, phone_number: str) -> dict:
        """
        Check if a phone number exists on WhatsApp.
        
        Args:
            phone_number: The phone number to check
            
        Returns:
            API response dict with exists status
        """
        payload = {'receptor': phone_number}
        return self._post('v2/numberExists', payload)


# Module-level singleton and helper functions
_client_lock: Lock = Lock()
_client_instance: Optional[WhatsAppClient] = None
_executor = ThreadPoolExecutor(max_workers=5)


def _resolve_credentials() -> tuple[str, str]:
    """Get OneMsg credentials from settings or environment."""
    app_key = getattr(settings, 'ONEMSG_APP_KEY', '') or os.environ.get('ONEMSG_APP_KEY', '')
    auth_key = getattr(settings, 'ONEMSG_AUTH_KEY', '') or os.environ.get('ONEMSG_AUTH_KEY', '')
    
    if not app_key or not auth_key:
        raise WhatsAppConfigurationError('OneMsg credentials are not configured. Set ONEMSG_APP_KEY and ONEMSG_AUTH_KEY.')
    
    return app_key, auth_key


def get_client() -> WhatsAppClient:
    """Get or create the WhatsApp client singleton."""
    global _client_instance
    app_key, auth_key = _resolve_credentials()
    
    with _client_lock:
        if _client_instance is None or _client_instance.app_key != app_key:
            _client_instance = WhatsAppClient(app_key, auth_key)
    
    return _client_instance


def send_text(phone_number: str, text: str) -> bool:
    """
    Send a text message (synchronous).
    
    Args:
        phone_number: Recipient phone with country code
        text: Message text
        
    Returns:
        True if successful, False otherwise
    """
    try:
        client = get_client()
        client.send_text(phone_number, text)
        logger.info('Sent WhatsApp message to %s', phone_number)
        return True
    except WhatsAppClientError as exc:
        logger.error('Failed to send WhatsApp message: %s', exc)
        return False


def send_text_async(phone_number: str, text: str) -> None:
    """
    Send a text message asynchronously (fire-and-forget).
    
    Args:
        phone_number: Recipient phone with country code
        text: Message text
    """
    def _send():
        try:
            client = get_client()
            client.send_text(phone_number, text)
            logger.info('Sent WhatsApp message to %s', phone_number)
        except Exception as exc:
            logger.error('Failed to send WhatsApp message to %s: %s', phone_number, exc)
    
    _executor.submit(_send)


def normalize_phone_number(phone: str) -> str:
    """
    Normalize a phone number for WhatsApp.
    
    - Removes spaces, dashes, and parentheses
    - Removes leading + if present
    - Ensures country code is present (defaults to Zimbabwe 263 if number starts with 0)
    
    Args:
        phone: Raw phone number string
        
    Returns:
        Normalized phone number (e.g., "263771234567")
    """
    # Remove common formatting characters
    phone = phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '').replace('+', '')
    
    # If starts with 0, assume Zimbabwe and add country code
    if phone.startswith('0'):
        phone = '263' + phone[1:]
    
    return phone
