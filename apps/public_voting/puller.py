"""Outbound-only retrieval from Cloudflare holding store, never from a browser.

The private AirVote worker signs each claim/ack request with a separate secret.
No Cloudflare credential can open PostgreSQL. Failed acknowledgements are
retried through lease expiry; PostgreSQL receipts make admission idempotent.
"""
import hashlib
import hmac
import json
import logging
import time
from urllib.parse import urlsplit

import requests
from django.conf import settings

from .admission import admit_web_vote
from .bridge import signing_message

logger = logging.getLogger(__name__)


def is_configured():
    return bool(
        settings.AIRVOTE_WEB_HOLDING_URL.startswith("https://")
        and len(settings.AIRVOTE_WEB_PULL_SECRET) >= 32
    )


def signed_post(route, payload):
    url = settings.AIRVOTE_WEB_HOLDING_URL.rstrip("/") + route
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
        raise ValueError("Invalid public-voting holding endpoint.")
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    timestamp = str(int(time.time()))
    digest = hmac.new(
        settings.AIRVOTE_WEB_PULL_SECRET.encode("utf-8"),
        signing_message("POST", parts.path + ("?" + parts.query if parts.query else ""), timestamp, body),
        hashlib.sha256,
    ).hexdigest()
    response = requests.post(
        url, data=body, headers={
            "Content-Type": "application/json",
            "X-AirVote-Timestamp": timestamp,
            "X-AirVote-Signature": digest,
        }, timeout=(3, 8),
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError("Invalid gateway response.")
    return data


def poll_once():
    if not is_configured():
        return 0
    claimed = signed_post("/internal/claim", {"limit": 20})
    votes = claimed.get("votes", [])
    if not isinstance(votes, list) or len(votes) > 20:
        raise ValueError("Invalid gateway claim response.")
    acknowledgements = []
    for item in votes:
        try:
            if not isinstance(item, dict) or not isinstance(item.get("lease_token"), str):
                raise ValueError("Invalid claimed receipt.")
            outcome = admit_web_vote(item)
            acknowledgements.append({
                "id": outcome["id"], "lease_token": item["lease_token"],
                "state": "accepted" if outcome["state"] == "admitted" else "rejected",
                "reason": outcome["reason"],
            })
        except Exception:
            # Do not ack/transiently lose a receipt when PostgreSQL is unavailable.
            logger.exception("Web vote admission failed; holding lease will expire.")
    if acknowledgements:
        signed_post("/internal/ack", {"items": acknowledgements})
    return len(acknowledgements)
