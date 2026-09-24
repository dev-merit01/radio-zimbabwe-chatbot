MEDIA_TYPES = {
    "image",
    "video",
    "audio",
    "voice",
    "sticker",
    "document",
    "location",
    "contact",
}


def _extract_telegram_message(payload: dict) -> tuple[str | None, str, str | None]:
    """
    Extract chat_id, text, and media type from Telegram webhook payload.

    Returns:
        (chat_id, text, media_type) - media_type is None for text messages
    """
    message = (
        payload.get("message")
        or payload.get("edited_message")
        or payload.get("channel_post")
        or payload.get("callback_query", {}).get("message")
    )
    if not message:
        return None, "", None

    chat = message.get("chat") or {}
    chat_id = chat.get("id")

    # Check for media types
    media_type = None
    if "photo" in message:
        media_type = "photo"
    elif "video" in message:
        media_type = "video"
    elif "audio" in message:
        media_type = "audio"
    elif "voice" in message:
        media_type = "voice"
    elif "video_note" in message:
        media_type = "video_note"
    elif "document" in message:
        media_type = "document"
    elif "sticker" in message:
        media_type = "sticker"
    elif "location" in message:
        media_type = "location"
    elif "contact" in message:
        media_type = "contact"

    text = message.get("text") or message.get("caption") or ""
    return chat_id, text, media_type


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
    sender = payload.get("sender")
    if not sender:
        return None, "", None

    # Clean sender - remove @s.whatsapp.net suffix if present
    if "@" in sender:
        sender = sender.split("@")[0]

    message_payload = payload.get("payload", {})

    # Try different message types in order of likelihood
    text = ""
    media_type = None

    # Simple text message
    if "conversation" in message_payload:
        text = message_payload["conversation"]

    # Extended text message
    elif "extendedTextMessage" in message_payload:
        text = message_payload["extendedTextMessage"].get("text", "")

    # Media messages - reject these
    elif "imageMessage" in message_payload:
        media_type = "image"
        text = message_payload["imageMessage"].get("caption", "")

    elif "videoMessage" in message_payload:
        media_type = "video"
        text = message_payload["videoMessage"].get("caption", "")

    elif "audioMessage" in message_payload:
        media_type = "audio"

    elif "documentMessage" in message_payload:
        media_type = "document"

    elif "stickerMessage" in message_payload:
        media_type = "sticker"

    elif "locationMessage" in message_payload:
        media_type = "location"

    elif "contactMessage" in message_payload:
        media_type = "contact"

    elif "ptvMessage" in message_payload:  # Voice/video note
        media_type = "voice"

    return sender, text.strip(), media_type


def _extract_bird_message(payload: dict) -> tuple[str | None, str, str | None]:
    """
    Extract sender phone, message text, and media type from Bird.com webhook payload.

    Returns:
        (sender, text, media_type) - media_type is None for text messages
    """
    # Extract sender phone number - try multiple possible paths
    sender = None
    sender_info = payload.get("sender", {})

    # Try sender.contact.identifierValue (actual Bird format)
    contact = sender_info.get("contact", {})
    sender = contact.get("identifierValue", "")

    # Fallback: sender.connector.identifierValue
    if not sender:
        connector = sender_info.get("connector", {})
        sender = connector.get("identifierValue", "")

    # Fallback: direct identifierValue
    if not sender:
        sender = sender_info.get("identifierValue", "")

    # Clean phone number - remove + prefix for consistency
    if sender and sender.startswith("+"):
        sender = sender[1:]

    if not sender:
        return None, "", None

    # Extract message text and detect media type
    text = ""
    media_type = None
    body = payload.get("body", {})
    body_type = body.get("type", "")

    if body_type == "text":
        text_obj = body.get("text", {})
        text = text_obj.get("text", "")
    elif body_type in MEDIA_TYPES:
        media_type = body_type
        # Check for caption on media
        media_obj = body.get(body_type, {})
        text = media_obj.get("caption", "")

    # Fallback: check for content field
    if not text and not media_type:
        content = payload.get("content", {})
        if isinstance(content, dict):
            text = content.get("text", "")
        elif isinstance(content, str):
            text = content

    return sender, text.strip(), media_type
