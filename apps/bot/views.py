"""Authenticated, idempotent receipt. Processing is performed by run_vote_worker."""

import base64
import hashlib
import hmac
import json
import time
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from django.http import JsonResponse
from django.core.exceptions import RequestDataTooBig
from django.views.decorators.csrf import csrf_exempt
from apps.accounts.models import station_exists
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
            request.headers.get("X-Telegram-Bot-Api-Secret-Token", "").encode(),
            secret.encode(),
        )
    if provider == "onemsg":
        # Configure a provider/gateway capable of adding this secret header.
        return hmac.compare_digest(
            request.headers.get("X-Webhook-Token", "").encode(), secret.encode()
        )
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


def bird_station_for_message(message):
    """Route by the authenticated Bird recipient, never untrusted payload station.

    Database routes allow new stations without editing Railway env variables.
    The original seven environment routes remain a compatibility fallback.
    Unknown numbers are ignored if any routing is configured.
    """
    from apps.accounts.models import BroadcastStation

    routes = getattr(settings, "BIRD_NUMBER_STATION_MAP", {})
    if not isinstance(routes, dict):
        return None
    if not routes and not BroadcastStation.objects.filter(
        is_active=True, bird_number__isnull=False,
    ).exists():
        # Older single-station installations have no receiver routing at all.
        # Keep their previously supported default station behaviour.
        return getattr(settings, "BIRD_STATION", "")
    recipient = message.get("to")
    if not isinstance(recipient, dict):
        return None
    number = recipient.get("phone_number")
    if not isinstance(number, str):
        return None
    number = number.strip().removeprefix("+")
    if not number or not number.isascii() or not number.isdigit():
        return None
    station = BroadcastStation.objects.filter(
        bird_number=number, is_active=True,
    ).values_list("slug", flat=True).first()
    if station:
        return station
    if routes:
        return routes.get(number) or routes.get("+" + number)
    # With any database route present, an unknown number is never assigned to
    # the default legacy station, even when the environment map is empty.
    if BroadcastStation.objects.filter(
        is_active=True, bird_number__isnull=False,
    ).exists():
        return None
    return getattr(settings, "BIRD_STATION", "")


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
        if provider == "bird":
            message = payload.get("payload", payload.get("data", payload))
            station = bird_station_for_message(message)
            if station is None:
                # Unknown Bird recipients must never affect any station's votes.
                return JsonResponse({"ok": True, "ignored": True, "reason": "Unmapped Bird destination"})
        if not station_exists(station, active_only=True):
            from apps.accounts.models import BroadcastStation
            if BroadcastStation.objects.filter(slug=station, is_active=False).exists():
                return JsonResponse({
                    "ok": True, "ignored": True, "reason": "Station disabled",
                })
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
                event_type != "whatsapp.received"
                and not event_type.endswith(".inbound")
                and message.get("direction") not in {"incoming", "inbound"}
            ):
                return JsonResponse({"ok": True, "ignored": True})
            # Refuse outbound/self echoes even with a broad subscription.
            if message.get("direction") in {"outgoing", "outbound"}:
                return JsonResponse({"ok": True, "ignored": True})
            sender, text, media = _extract_bird_message(message)
            message_id = message.get("whatsapp_id") or message.get("id") or request.headers.get("webhook-id")
        else:
            if payload.get("fromMe") or payload.get("key", {}).get("fromMe"):
                return JsonResponse({"ok": True, "ignored": True})
            sender, text, media = _extract_whatsapp_message(payload)
            message_id = payload.get("id") or payload.get("key", {}).get("id")
        if (
            type(sender) not in (str, int)
            or type(message_id) not in (str, int)
            or not sender
            or not isinstance(text, str)
            or (media is not None and not isinstance(media, str))
        ):
            raise ValueError()
        if provider == "telegram" and (
            type(message_id) is not int
            or message_id < 0
            or type(sender) is not int
            or sender <= 0
        ):
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
        from apps.voting.services import validate_submission
        from apps.charts.routing import route_vote
        from apps.voting.models import UnassignedVote, SuspectedSpamVote
        from apps.voting.spam_detection import suspected_spam_reason
        from apps.voting.pipeline import lock_station
        if media:
            return JsonResponse({"ok": True, "ignored": True, "reason": "Text votes only."})
        # Freeze the original routing decision on provider retries, even when
        # an administrator has since changed a code or voting period.
        if InboundEvent.objects.filter(
            provider=provider, station=station, message_id=message_id
        ).exists():
            return JsonResponse({"ok": True, "duplicate": True})
        if UnassignedVote.objects.filter(
            provider=provider, station=station, message_id=message_id
        ).exists():
            return JsonResponse({"ok": True, "duplicate": True, "unassigned": True})
        if SuspectedSpamVote.objects.filter(
            provider=provider, station=station, message_id=message_id
        ).exists():
            return JsonResponse({"ok": True, "duplicate": True, "flagged": True})
        received_at = timezone.now()
        chart_show, vote_text, submitted_code, route_error = route_vote(
            station, text, received_at
        )
        if route_error:
            UnassignedVote.objects.get_or_create(
                provider=provider, station=station, message_id=message_id,
                defaults={
                    "sender": sender, "text": text, "reason": route_error,
                    "received_at": received_at,
                },
            )
            # Safe acknowledgement to Bird itself; never reply to the listener.
            return JsonResponse({"ok": True, "unassigned": True}, status=202)
        valid, reason = validate_submission(vote_text)
        if not valid:
            return JsonResponse({"ok": True, "ignored": True, "reason": reason})
        with transaction.atomic():
            # Serialise intake decisions per station. Otherwise simultaneous
            # messages from one sender can both evade the burst threshold.
            lock_station(station)
            if InboundEvent.objects.filter(
                provider=provider, station=station, message_id=message_id
            ).exists():
                return JsonResponse({"ok": True, "duplicate": True})
            if UnassignedVote.objects.filter(
                provider=provider, station=station, message_id=message_id
            ).exists():
                return JsonResponse({"ok": True, "duplicate": True, "unassigned": True})
            if SuspectedSpamVote.objects.filter(
                provider=provider, station=station, message_id=message_id
            ).exists():
                return JsonResponse({"ok": True, "duplicate": True, "flagged": True})
            spam_reason = suspected_spam_reason(
                station, provider, sender, vote_text, received_at
            )
            if spam_reason:
                SuspectedSpamVote.objects.create(
                    provider=provider, station=station, message_id=message_id,
                    sender=sender, text=text, vote_text=vote_text,
                    submitted_code=submitted_code, chart_show=chart_show,
                    received_at=received_at, reason=spam_reason,
                )
                # Held before vote creation and before AI API use. No listener
                # message is sent; Bird receives only a normal webhook ack.
                return JsonResponse({"ok": True, "flagged": True}, status=202)
            InboundEvent.objects.create(
                provider=provider, station=station, message_id=message_id,
                sender=sender, text=text, vote_text=vote_text,
                submitted_code=submitted_code, chart_show=chart_show,
                received_at=received_at, media_type="",
            )
        return JsonResponse({"ok": True, "duplicate": False}, status=202)
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
