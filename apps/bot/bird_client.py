"""
Bird.com WhatsApp Client

This module provides a client for sending WhatsApp messages via the Bird.com API.
API Docs: https://docs.bird.com/api/channels-api/api-reference/messaging
"""
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Optional

import requests
from django.conf import settings

logger = logging.getLogger(__name__)


class BirdClientError(RuntimeError):
    """Base Bird client error."""


class BirdConfigurationError(BirdClientError):
    """Raised when Bird is not configured."""


class BirdClient:
    """
    Client for Bird.com WhatsApp API.
    
    Required settings:
        BIRD_ACCESS_KEY: Your Bird access key
        BIRD_WORKSPACE_ID: Your Bird workspace UUID
        BIRD_CHANNEL_ID: Your WhatsApp channel UUID
    """
    
    BASE_URL = 'https://api.bird.com'
    
    def __init__(
        self, 
        access_key: str, 
        workspace_id: str, 
        channel_id: str,
        session: Optional[requests.Session] = None
    ):
        if not access_key:
            raise BirdConfigurationError('Bird access_key is required.')
        if not workspace_id:
            raise BirdConfigurationError('Bird workspace_id is required.')
        if not channel_id:
            raise BirdConfigurationError('Bird channel_id is required.')
            
        self.access_key = access_key
        self.workspace_id = workspace_id
        self.channel_id = channel_id
        self._session = session or requests.Session()
        self._session.headers.update({
            'Authorization': f'AccessKey {access_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })

    def _post(self, endpoint: str, payload: dict) -> dict:
        """Make a POST request to the Bird API."""
        url = f'{self.BASE_URL}{endpoint}'
        
        try:
            response = self._session.post(url, json=payload, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.exception('Bird request failed: %s', exc)
            raise BirdClientError(str(exc)) from exc

        try:
            data = response.json()
        except ValueError as exc:
            logger.exception('Bird returned non-JSON response: %s', response.text)
            raise BirdClientError('Bird returned an invalid response') from exc

        return data

    def send_text(self, phone_number: str, text: str) -> dict:
        """
        Send a text message to a WhatsApp number.
        
        Args:
            phone_number: The recipient's phone number with country code (e.g., "+263771234567")
            text: The message text to send
            
        Returns:
            API response dict
        """
        # Ensure phone number has + prefix for international format
        if not phone_number.startswith('+'):
            phone_number = f'+{phone_number}'
        
        endpoint = f'/workspaces/{self.workspace_id}/channels/{self.channel_id}/messages'
        
        payload = {
            'receiver': {
                'contacts': [
                    {'identifierValue': phone_number}
                ]
            },
            'body': {
                'type': 'text',
                'text': {
                    'text': text
                }
            }
        }
        
        logger.debug('Sending Bird WhatsApp message to %s', phone_number)
        return self._post(endpoint, payload)
    
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
        if not phone_number.startswith('+'):
            phone_number = f'+{phone_number}'
            
        endpoint = f'/workspaces/{self.workspace_id}/channels/{self.channel_id}/messages'
        
        payload = {
            'receiver': {
                'contacts': [
                    {'identifierValue': phone_number}
                ]
            },
            'body': {
                'type': 'image',
                'image': {
                    'mediaUrl': image_url,
                    'caption': caption
                }
            }
        }
        
        logger.debug('Sending Bird WhatsApp image to %s', phone_number)
        return self._post(endpoint, payload)


# Module-level singleton and helper functions
_client_lock: Lock = Lock()
_client_instance: Optional[BirdClient] = None
_executor = ThreadPoolExecutor(max_workers=5)


def _resolve_credentials() -> tuple[str, str, str]:
    """Get Bird credentials from settings or environment."""
    access_key = getattr(settings, 'BIRD_ACCESS_KEY', '') or os.environ.get('BIRD_ACCESS_KEY', '')
    workspace_id = getattr(settings, 'BIRD_WORKSPACE_ID', '') or os.environ.get('BIRD_WORKSPACE_ID', '')
    channel_id = getattr(settings, 'BIRD_CHANNEL_ID', '') or os.environ.get('BIRD_CHANNEL_ID', '')
    
    if not access_key:
        raise BirdConfigurationError('BIRD_ACCESS_KEY is not configured.')
    if not workspace_id:
        raise BirdConfigurationError('BIRD_WORKSPACE_ID is not configured.')
    if not channel_id:
        raise BirdConfigurationError('BIRD_CHANNEL_ID is not configured.')
    
    return access_key, workspace_id, channel_id


def get_client() -> BirdClient:
    """Get or create the Bird client singleton."""
    global _client_instance
    access_key, workspace_id, channel_id = _resolve_credentials()
    
    with _client_lock:
        if _client_instance is None or _client_instance.access_key != access_key:
            _client_instance = BirdClient(access_key, workspace_id, channel_id)
    
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
        logger.info('Sent Bird WhatsApp message to %s', phone_number)
        return True
    except BirdClientError as exc:
        logger.error('Failed to send Bird WhatsApp message: %s', exc)
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
            logger.info('Sent Bird WhatsApp message to %s', phone_number)
        except Exception as exc:
            logger.error('Failed to send Bird WhatsApp message to %s: %s', phone_number, exc)
    
    _executor.submit(_send)


def normalize_phone_number(phone: str) -> str:
    """
    Normalize a phone number for WhatsApp.
    
    - Removes spaces, dashes, and parentheses
    - Ensures + prefix for international format
    - Ensures country code is present (defaults to Zimbabwe 263 if number starts with 0)
    
    Args:
        phone: Raw phone number string
        
    Returns:
        Normalized phone number (e.g., "+263771234567")
    """
    # Remove common formatting characters
    phone = phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    
    # Remove leading + if present (we'll add it back)
    if phone.startswith('+'):
        phone = phone[1:]
    
    # If starts with 0, assume Zimbabwe and add country code
    if phone.startswith('0'):
        phone = '263' + phone[1:]
    
    return f'+{phone}'
