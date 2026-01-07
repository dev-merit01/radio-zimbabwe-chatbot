import json
import logging
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
from apps.voting.services import VotingService
from .telegram_client import (
    send_text_async as telegram_send_text_async,
    get_client as get_telegram_client,
    TelegramConfigurationError,
)
from .whatsapp_client import (
    send_text_async as whatsapp_send_text_async,
    get_client as get_whatsapp_client,
    normalize_phone_number,
    WhatsAppConfigurationError,
)
from .bird_client import (
    send_text_async as bird_send_text_async,
    get_client as get_bird_client,
    normalize_phone_number as bird_normalize_phone,
    BirdConfigurationError,
)

logger = logging.getLogger(__name__)


def _extract_telegram_message(payload: dict) -> tuple[str | None, str, str | None]:
    """
    Extract chat_id, text, and media type from Telegram webhook payload.
    
    Returns:
        (chat_id, text, media_type) - media_type is None for text messages
    """
    message = (
        payload.get('message')
        or payload.get('edited_message')
        or payload.get('channel_post')
        or payload.get('callback_query', {}).get('message')
    )
    if not message:
        return None, '', None
    
    chat = message.get('chat') or {}
    chat_id = chat.get('id')
    
    # Check for media types
    media_type = None
    if 'photo' in message:
        media_type = 'photo'
    elif 'video' in message:
        media_type = 'video'
    elif 'audio' in message:
        media_type = 'audio'
    elif 'voice' in message:
        media_type = 'voice'
    elif 'video_note' in message:
        media_type = 'video_note'
    elif 'document' in message:
        media_type = 'document'
    elif 'sticker' in message:
        media_type = 'sticker'
    elif 'location' in message:
        media_type = 'location'
    elif 'contact' in message:
        media_type = 'contact'
    
    text = message.get('text') or message.get('caption') or ''
    return chat_id, text, media_type


@csrf_exempt
def telegram_webhook(request):
    if request.method != 'POST':
        return HttpResponseBadRequest('Invalid method')
    try:
        payload = json.loads(request.body.decode('utf-8'))
    except Exception:
        return HttpResponseBadRequest('Invalid JSON')

    chat_id_raw, text, media_type = _extract_telegram_message(payload)
    if chat_id_raw is None:
        logger.warning("Webhook payload missing chat information: %s", payload)
        return HttpResponseBadRequest('No chat id')

    chat_id = str(chat_id_raw)
    logger.info("Webhook: chat_id=%s, text=%s, media=%s", chat_id, text[:50] if text else '', media_type)

    try:
        get_telegram_client()
    except TelegramConfigurationError as exc:
        logger.error('Telegram configuration error: %s', exc)
        return JsonResponse({'ok': False, 'error': 'telegram_not_configured'}, status=500)

    # Reject media messages with a helpful response
    if media_type:
        logger.info("Telegram webhook: Media message (%s) from %s, rejecting", media_type, chat_id)
        rejection_msg = (
            "❌ Sorry, I can only accept text votes.\n\n"
            "Please send your vote as:\n"
            "Artist - Song\n\n"
            "Example: Winky D - Ijipita"
        )
        telegram_send_text_async(chat_id, rejection_msg)
        return JsonResponse({'ok': True, 'message': 'Media rejected'})

    vs = VotingService(channel='telegram', user_ref=chat_id)
    response_text = vs.handle_incoming_text(text)

    logger.info(f"Response: {response_text}")

    # Send response back to user on Telegram (non-blocking)
    telegram_send_text_async(chat_id, response_text)

    return JsonResponse({'ok': True, 'message': response_text})


def _extract_whatsapp_message(payload: dict) -> tuple[str | None, str, str | None]:
    """
    Extract sender phone, message text, and media type from OneMsg webhook payload.
    
    OneMsg webhook format:
    {
        "sender": "263771234567",
        "receiver": "263779876543",
        "payload": {
            "conversation": "message text",  # Simple text message
            # OR
            "extendedTextMessage": {"text": "message text"},  # Advanced text
            # OR other message types...
        }
    }
    
    Returns:
        (sender, text, media_type) - media_type is None for text messages
    """
    sender = payload.get('sender')
    if not sender:
        return None, '', None
    
    # Clean sender - remove @s.whatsapp.net suffix if present
    if '@' in sender:
        sender = sender.split('@')[0]
    
    message_payload = payload.get('payload', {})
    
    # Try different message types in order of likelihood
    text = ''
    media_type = None
    
    # Simple text message
    if 'conversation' in message_payload:
        text = message_payload['conversation']
    
    # Extended text message
    elif 'extendedTextMessage' in message_payload:
        text = message_payload['extendedTextMessage'].get('text', '')
    
    # Media messages - reject these
    elif 'imageMessage' in message_payload:
        media_type = 'image'
        text = message_payload['imageMessage'].get('caption', '')
    
    elif 'videoMessage' in message_payload:
        media_type = 'video'
        text = message_payload['videoMessage'].get('caption', '')
    
    elif 'audioMessage' in message_payload:
        media_type = 'audio'
    
    elif 'documentMessage' in message_payload:
        media_type = 'document'
    
    elif 'stickerMessage' in message_payload:
        media_type = 'sticker'
    
    elif 'locationMessage' in message_payload:
        media_type = 'location'
    
    elif 'contactMessage' in message_payload:
        media_type = 'contact'
    
    elif 'ptvMessage' in message_payload:  # Voice/video note
        media_type = 'voice'
    
    return sender, text.strip(), media_type


@csrf_exempt
def whatsapp_webhook(request):
    """
    Webhook endpoint for OneMsg.io WhatsApp messages.
    
    Configure this URL in OneMsg dashboard under My Device > Webhook Address.
    Example: https://yourdomain.com/webhooks/whatsapp/
    """
    if request.method != 'POST':
        return HttpResponseBadRequest('Invalid method')
    
    try:
        payload = json.loads(request.body.decode('utf-8'))
    except Exception:
        logger.warning("WhatsApp webhook: Invalid JSON received")
        return HttpResponseBadRequest('Invalid JSON')
    
    logger.debug("WhatsApp webhook payload: %s", payload)
    
    sender, text, media_type = _extract_whatsapp_message(payload)
    
    if not sender:
        logger.warning("WhatsApp webhook: No sender in payload: %s", payload)
        return JsonResponse({'ok': True, 'message': 'No sender'})
    
    # Reject media messages with a helpful response
    if media_type:
        logger.info("WhatsApp webhook: Media message (%s) from %s, rejecting", media_type, sender)
        try:
            get_whatsapp_client()
            rejection_msg = (
                "❌ Sorry, I can only accept text votes.\n\n"
                "Please send your vote as:\n"
                "Artist - Song\n\n"
                "Example: Winky D - Ijipita"
            )
            whatsapp_send_text_async(sender, rejection_msg)
        except WhatsAppConfigurationError:
            pass
        return JsonResponse({'ok': True, 'message': 'Media rejected'})
    
    if not text:
        # Ignore empty messages (reactions, read receipts, etc.)
        logger.debug("WhatsApp webhook: Empty message from %s, ignoring", sender)
        return JsonResponse({'ok': True, 'message': 'Empty message ignored'})
    
    logger.info("WhatsApp webhook: sender=%s, text=%s", sender, text[:50] if len(text) > 50 else text)
    
    # Check WhatsApp client is configured
    try:
        get_whatsapp_client()
    except WhatsAppConfigurationError as exc:
        logger.error('WhatsApp configuration error: %s', exc)
        return JsonResponse({'ok': False, 'error': 'whatsapp_not_configured'}, status=500)
    
    # Process the vote
    vs = VotingService(channel='whatsapp', user_ref=sender)
    response_text = vs.handle_incoming_text(text)
    
    logger.info("WhatsApp response to %s: %s", sender, response_text)
    
    # Send response back to user on WhatsApp (non-blocking)
    whatsapp_send_text_async(sender, response_text)
    
    return JsonResponse({'ok': True, 'message': response_text})


# Media types that should be rejected
MEDIA_TYPES = {'image', 'video', 'audio', 'voice', 'sticker', 'document', 'location', 'contact'}


def _extract_bird_message(payload: dict) -> tuple[str | None, str, str | None]:
    """
    Extract sender phone, message text, and media type from Bird.com webhook payload.
    
    Returns:
        (sender, text, media_type) - media_type is None for text messages
    """
    # Extract sender phone number - try multiple possible paths
    sender = None
    sender_info = payload.get('sender', {})
    
    # Try sender.contact.identifierValue (actual Bird format)
    contact = sender_info.get('contact', {})
    sender = contact.get('identifierValue', '')
    
    # Fallback: sender.connector.identifierValue
    if not sender:
        connector = sender_info.get('connector', {})
        sender = connector.get('identifierValue', '')
    
    # Fallback: direct identifierValue
    if not sender:
        sender = sender_info.get('identifierValue', '')
    
    # Clean phone number - remove + prefix for consistency
    if sender and sender.startswith('+'):
        sender = sender[1:]
    
    if not sender:
        return None, '', None
    
    # Extract message text and detect media type
    text = ''
    media_type = None
    body = payload.get('body', {})
    body_type = body.get('type', '')
    
    if body_type == 'text':
        text_obj = body.get('text', {})
        text = text_obj.get('text', '')
    elif body_type in MEDIA_TYPES:
        media_type = body_type
        # Check for caption on media
        media_obj = body.get(body_type, {})
        text = media_obj.get('caption', '')
    
    # Fallback: check for content field
    if not text and not media_type:
        content = payload.get('content', {})
        if isinstance(content, dict):
            text = content.get('text', '')
        elif isinstance(content, str):
            text = content
    
    return sender, text.strip(), media_type


@csrf_exempt
def bird_webhook(request):
    """
    Webhook endpoint for Bird.com WhatsApp messages.
    
    Configure this URL in Bird dashboard:
    1. Go to Settings > Webhooks
    2. Create new webhook
    3. Set URL to: https://yourdomain.com/webhook/bird/
    4. Select service: Channels
    5. Select event: whatsapp.inbound (or message.inbound)
    """
    if request.method != 'POST':
        return HttpResponseBadRequest('Invalid method')
    
    try:
        payload = json.loads(request.body.decode('utf-8'))
    except Exception:
        logger.warning("Bird webhook: Invalid JSON received")
        return HttpResponseBadRequest('Invalid JSON')
    
    logger.info("Bird webhook payload: %s", json.dumps(payload, indent=2)[:2000])
    
    # Check if this is an incoming message
    # Bird uses event field like "whatsapp.inbound" for incoming messages
    event = payload.get('event', '')
    if not event.endswith('.inbound') and event != 'message.created':
        # Also check direction for other payload formats
        direction = payload.get('direction', '')
        if direction != 'incoming':
            logger.debug("Bird webhook: Ignoring non-incoming message (event=%s, direction=%s)", event, direction)
            return JsonResponse({'ok': True, 'message': 'Ignored non-incoming'})
    
    # Extract from nested payload if present (Bird wraps the actual message)
    message_payload = payload.get('payload', payload)
    
    sender, text, media_type = _extract_bird_message(message_payload)
    
    if not sender:
        logger.warning("Bird webhook: No sender in payload")
        return JsonResponse({'ok': True, 'message': 'No sender'})
    
    # Reject media messages with a helpful response
    if media_type:
        logger.info("Bird webhook: Media message (%s) from %s, rejecting", media_type, sender)
        try:
            get_bird_client()
            rejection_msg = (
                "❌ Sorry, I can only accept text votes.\n\n"
                "Please send your vote as:\n"
                "Artist - Song\n\n"
                "Example: Winky D - Ijipita"
            )
            bird_send_text_async(sender, rejection_msg)
        except BirdConfigurationError:
            pass
        return JsonResponse({'ok': True, 'message': 'Media rejected'})
    
    if not text:
        # Ignore empty messages
        logger.debug("Bird webhook: Empty message from %s, ignoring", sender)
        return JsonResponse({'ok': True, 'message': 'Empty message ignored'})
    
    logger.info("Bird webhook: sender=%s, text=%s", sender, text[:50] if len(text) > 50 else text)
    
    # Check Bird client is configured
    try:
        get_bird_client()
    except BirdConfigurationError as exc:
        logger.error('Bird configuration error: %s', exc)
        return JsonResponse({'ok': False, 'error': 'bird_not_configured'}, status=500)
    
    # Process the vote
    vs = VotingService(channel='whatsapp', user_ref=sender)
    response_text = vs.handle_incoming_text(text)
    
    logger.info("Bird response to %s: %s", sender, response_text)
    
    # Send response back to user on WhatsApp via Bird (non-blocking)
    bird_send_text_async(sender, response_text)
    
    return JsonResponse({'ok': True, 'message': response_text})
