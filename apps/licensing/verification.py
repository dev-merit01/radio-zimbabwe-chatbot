"""Offline-verifiable Ed25519 licences. The signing private key never reaches AirVote."""

import base64
import binascii
from datetime import date, timedelta
import json
import uuid

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from django.conf import settings
from django.utils import timezone

ISSUER = "kuiperdatalabs.co.zw"
AUDIENCE = "airvote"
PLANS = frozenset({"trial", "annual", "enterprise", "perpetual"})


class LicenceError(ValueError):
    pass


def _unb64(value):
    if not value or len(value) > 8192:
        raise LicenceError("Invalid licence encoding.")
    try:
        return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except (ValueError, binascii.Error) as exc:
        raise LicenceError("Invalid licence encoding.") from exc


def decode_and_verify(token, public_key):
    """Return only cryptographically authenticated licence claims."""
    if not isinstance(token, str) or len(token) > 8192:
        raise LicenceError("Invalid licence format.")
    parts = token.strip().split(".")
    if len(parts) != 3 or parts[0] != "AV1":
        raise LicenceError("Invalid licence format.")
    raw, signature = _unb64(parts[1]), _unb64(parts[2])
    if len(raw) > 4096 or len(signature) != 64:
        raise LicenceError("Invalid licence payload.")
    try:
        key_bytes = base64.b64decode(public_key.strip(), validate=True)
        if len(key_bytes) != 32:
            raise LicenceError("Invalid public signing key.")
        Ed25519PublicKey.from_public_bytes(key_bytes).verify(signature, raw)
    except (InvalidSignature, ValueError, binascii.Error, TypeError) as exc:
        raise LicenceError("Licence signature could not be verified.") from exc
    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("Not an object")
        station = payload["station"]
        customer = payload["customer"]
        plan = payload["plan"]
        if (payload["issuer"] != ISSUER or payload["audience"] != AUDIENCE
                or type(station) is not str or not 1 <= len(station) <= 32
                or type(customer) is not str or not 1 <= len(customer) <= 160
                or plan not in PLANS):
            raise ValueError("Unsupported claims")
        if type(payload["grace_days"]) is not int or not 0 <= payload["grace_days"] <= 7:
            raise ValueError("Invalid grace")
        uuid.UUID(payload["licence_id"])
        start = date.fromisoformat(payload["starts_on"])
        expiry = date.fromisoformat(payload["expires_on"])
        if expiry < start:
            raise ValueError("End before start")
    except (ValueError, KeyError, TypeError) as exc:
        raise LicenceError("Invalid licence claims.") from exc
    return payload


def licence_status(station, *, today=None):
    """Fail closed on unknown, tampered, revoked and expired station licences."""
    from .models import StationLicence

    today = today or timezone.now().date()
    result = {"station": station, "state": "unlicensed", "permitted": False,
              "customer": None, "plan": None, "expires_on": None,
              "grace_until": None}
    record = StationLicence.objects.filter(station=station).first()
    if record is None:
        return result
    key = getattr(settings, "AIRVOTE_LICENCE_PUBLIC_KEY", "")
    if not key:
        return {**result, "state": "misconfigured"}
    try:
        claims = decode_and_verify(record.signed_token, key)
    except LicenceError:
        return {**result, "state": "invalid"}
    if claims["station"] != station:
        return {**result, "state": "invalid"}
    result.update(customer=claims["customer"], plan=claims["plan"],
                  expires_on=claims["expires_on"])
    if record.revoked_at is not None:
        return {**result, "state": "revoked"}
    begins = date.fromisoformat(claims["starts_on"])
    expires = date.fromisoformat(claims["expires_on"])
    grace_until = expires + timedelta(days=claims["grace_days"])
    result["grace_until"] = grace_until.isoformat()
    if today < begins:
        return {**result, "state": "not_started"}
    if today <= expires:
        return {**result, "state": "active", "permitted": True}
    if today <= grace_until:
        return {**result, "state": "grace", "permitted": True}
    return {**result, "state": "expired"}
