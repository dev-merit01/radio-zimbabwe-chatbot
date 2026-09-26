"""Database-backed worker: restart-safe intake, matching and outgoing replies."""

import uuid
from datetime import timedelta
import requests
from django.db import transaction, connection
from django.db.models import Q
from django.utils import timezone
from .models import InboundEvent, VoteJob, OutboundMessage
from .services import VotingService
from .pipeline import process_vote


LEASE_SECONDS = 180
MAX_ATTEMPTS = 6


def claim(model):
    now = timezone.now()
    with transaction.atomic():
        # An interrupted send has an unknown outcome. Do not automatically duplicate it.
        if model is OutboundMessage:
            model.objects.filter(state="running", lease_until__lt=now).update(
                state="uncertain",
                error="Delivery outcome unknown; check provider before retrying.",
            )
        model.objects.filter(
            state="running", lease_until__lt=now, attempts__gte=MAX_ATTEMPTS
        ).update(state="failed", error="Worker lease expired too many times.")
        due = Q(state="queued", available_at__lte=now)
        if model is not OutboundMessage:
            due |= Q(state="running", lease_until__lt=now)
        qs = model.objects.filter(due).order_by("available_at", "id")
        if connection.features.has_select_for_update_skip_locked:
            qs = qs.select_for_update(skip_locked=True)
        else:
            qs = qs.select_for_update()
        item = qs.first()
        if not item:
            return None
        item.state = "running"
        item.attempts += 1
        item.lease_token = str(uuid.uuid4())
        item.lease_until = now + timedelta(seconds=LEASE_SECONDS)
        item.save()
        return item


def finish(item, **updates):
    type(item).objects.filter(
        pk=item.pk, lease_token=item.lease_token, state="running"
    ).update(lease_until=None, error="", updated_at=timezone.now(), **updates)


def ingest(item):
    with transaction.atomic():
        event = InboundEvent.objects.select_for_update().get(pk=item.pk)
        if event.state != "running" or event.lease_token != item.lease_token:
            return
        if not event.reply:
            if event.media_type:
                event.reply = "Please send a text vote: Artist - Song. Media messages cannot be counted."
            else:
                service = VotingService(
                    "telegram" if event.provider == "telegram" else "whatsapp",
                    event.sender,
                    event.station,
                )
                event.reply = service.handle_incoming_text(
                    event.text, vote_date=timezone.localdate(event.received_at)
                )
            event.save(update_fields=["reply"])
        OutboundMessage.objects.get_or_create(
            event=event, defaults={"text": event.reply}
        )
        finish(item, state="done")


def send(item):
    from apps.bot.telegram_client import get_client as telegram
    from apps.bot.bird_client import get_client as bird
    from apps.bot.whatsapp_client import get_client as onemsg

    client = {"telegram": telegram, "bird": bird, "onemsg": onemsg}[
        item.event.provider
    ]()
    response = client.send_text(item.event.sender, item.text)
    # The request may already have committed remotely. Treat malformed success
    # envelopes as uncertain instead of retrying after AttributeError/TypeError.
    if not isinstance(response, dict):
        raise ValueError("Invalid delivery response")
    result = response.get("result", {})
    if not isinstance(result, dict):
        raise ValueError("Invalid delivery result")
    message_id = response.get("id") or result.get("message_id")
    if item.event.provider in {"telegram", "bird"} and not message_id:
        raise ValueError("Missing delivery message ID")
    if message_id is not None and (
        type(message_id) not in (str, int) or not str(message_id).strip()
    ):
        raise ValueError("Invalid delivery message ID")
    message_id = message_id if message_id is not None else ""
    finish(item, state="done", provider_message_id=str(message_id)[:200])


def run_one(model):
    item = claim(model)
    if item is None:
        return False
    try:
        if model is InboundEvent:
            ingest(item)
        elif model is VoteJob:
            process_vote(item.vote)
            finish(item, state="done")
        else:
            send(item)
    except Exception as exc:
        # Log only exception type, never tokens, phone numbers or raw provider URLs.
        cause = exc.__cause__ or exc
        status = getattr(getattr(cause, "response", None), "status_code", None)
        uncertain = model is OutboundMessage and isinstance(
            cause, (requests.Timeout, requests.ConnectionError, ValueError)
        )
        # HTTP 5xx on a send can also follow a provider-side commit.
        uncertain = uncertain or (
            model is OutboundMessage and status is not None and status >= 500
        )
        state = (
            "uncertain"
            if uncertain
            else ("failed" if item.attempts >= MAX_ATTEMPTS else "queued")
        )
        if status in {400, 401, 403, 404}:
            state = "failed"
        updates = {
            "state": state,
            "error": type(exc).__name__,
            "lease_until": None,
            "available_at": timezone.now()
            + timedelta(seconds=min(900, 2**item.attempts * 5)),
            "updated_at": timezone.now(),
        }
        model.objects.filter(
            pk=item.pk, lease_token=item.lease_token, state="running"
        ).update(**updates)
    return True
