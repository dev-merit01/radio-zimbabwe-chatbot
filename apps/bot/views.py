"""Authenticated, idempotent receipt. Processing is performed by run_vote_worker."""

import base64
import hashlib
import hmac
import json
import time
from django.conf import settings
from django.http import JsonResponse
from django.core.exceptions import RequestDataTooBig
from django.views.decorators.csrf import csrf_exempt
from apps.accounts.models import Station
from apps.voting.models import InboundEvent
from .parsing import (
    _extract_bird_message,
    _extract_telegram_message,
    _extract_whatsapp_message,
)


def authorised(request, provider):
    secret = getattr(settings, f"{provider.upper()}_WEBHOOK_SECRET", "")
    if not secret:
        return False
    if provider == "telegram":
        return hmac.compare_digest(
            request.headers.get("X-Telegram-Bot-Api-Secret-Token", ""), secret
        )
    if provider == "onemsg":
        # Configure a provider/gateway capable of adding this secret header.
        return hmac.compare_digest(request.headers.get("X-Webhook-Token", ""), secret)
    try:
        timestamp = request.headers["webhook-timestamp"]
        if abs(time.time() - int(timestamp)) > 300:
            return False
        key = base64.b64decode(secret.removeprefix("whsec_"), validate=True)
        signed = (
            request.headers["webhook-id"] + "." + timestamp + "."
        ).encode() + request.body
        expected = base64.b64encode(
            hmac.new(key, signed, hashlib.sha256).digest()
        ).decode()
        return any(
            hmac.compare_digest(s, "v1," + expected)
            for s in request.headers.get("webhook-signature", "").split()
        )
    except (KeyError, ValueError, TypeError, OverflowError):
        return False


def receive(request, provider):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)
    try:
        body = request.body
    except RequestDataTooBig:
        return JsonResponse({"error": "Payload too large"}, status=413)
    if len(body) > 262144:
        return JsonResponse({"error": "Payload too large"}, status=413)
    if not authorised(request, provider):
        return JsonResponse({"error": "Webhook authentication failed"}, status=403)
    try:
        payload = json.loads(request.body)
        if not isinstance(payload, dict):
            raise ValueError()
        station = getattr(settings, f"{provider.upper()}_STATION")
        if station not in Station.values:
            return JsonResponse({"error": "Invalid station configuration"}, status=503)
        if provider == "telegram":
            # Only new private messages. Edits, callbacks and channel posts are not votes.
            message = payload.get("message")
            if not message or message.get("chat", {}).get("type") != "private":
                return JsonResponse({"ok": True, "ignored": True})
            sender, text, media = _extract_telegram_message(payload)
            message_id = payload.get("update_id")
        elif provider == "bird":
            message = payload.get("payload", payload.get("data", payload))
            event_type = payload.get("event", payload.get("type", ""))
            if (
                not event_type.endswith(".inbound")
                and message.get("direction") != "incoming"
            ):
                return JsonResponse({"ok": True, "ignored": True})
            # Refuse outbound/self echoes even with a broad subscription.
            if message.get("direction") in {"outgoing", "outbound"}:
                return JsonResponse({"ok": True, "ignored": True})
            sender, text, media = _extract_bird_message(message)
            message_id = message.get("id") or request.headers.get("webhook-id")
        else:
            if payload.get("fromMe") or payload.get("key", {}).get("fromMe"):
                return JsonResponse({"ok": True, "ignored": True})
            sender, text, media = _extract_whatsapp_message(payload)
            message_id = payload.get("id") or payload.get("key", {}).get("id")
        if not sender or message_id is None or not isinstance(text, str):
            raise ValueError()
        sender = str(sender).strip().lstrip("+")
        message_id = str(message_id)
        if (
            not sender
            or not message_id
            or len(sender) > 64
            or len(message_id) > 200
            or len(text) > 2048
            or len(media or "") > 32
        ):
            raise ValueError()
        if not text and not media:
            return JsonResponse({"ok": True, "ignored": True})
        _, created = InboundEvent.objects.get_or_create(
            provider=provider,
            station=station,
            message_id=message_id,
            defaults={"sender": sender, "text": text, "media_type": media or ""},
        )
        return JsonResponse(
            {"ok": True, "duplicate": not created}, status=202 if created else 200
        )
    except (ValueError, TypeError, AttributeError):
        return JsonResponse(
            {"error": "Invalid message envelope or missing message ID"}, status=400
        )


@csrf_exempt
def telegram_webhook(request):
    return receive(request, "telegram")


@csrf_exempt
def whatsapp_webhook(request):
    return receive(request, "onemsg")


@csrf_exempt
def bird_webhook(request):
    return receive(request, "bird")
