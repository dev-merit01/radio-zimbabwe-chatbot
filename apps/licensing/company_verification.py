"""AV2 Ed25519 company licensing; a single verified grant covers every owned station."""
import base64
import binascii
from datetime import date, datetime, timedelta
import json
import re
import uuid

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from django.conf import settings
from django.utils import timezone

from .verification import LicenceError

ISSUER = "kuiperdatalabs.co.zw"
AUDIENCE = "airvote-company"
COMPANY = re.compile(r"^[a-z][a-z0-9_-]{2,47}$")
PLANS = frozenset({"trial", "annual", "enterprise"})


def _decode64(value):
    if not isinstance(value, str) or not value or len(value) > 8192:
        raise LicenceError("Invalid licence encoding.")
    try:
        return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except (ValueError, binascii.Error) as exc:
        raise LicenceError("Invalid licence encoding.") from exc


def decode_company_licence(token, public_key):
    """Verify signature BEFORE inspecting commercial claims."""
    if not isinstance(token, str) or len(token) > 8192:
        raise LicenceError("Invalid company licence.")
    chunks = token.strip().split(".")
    if len(chunks) != 3 or chunks[0] != "AV2":
        raise LicenceError("Expected a Kuiper Data Labs company licence (AV2).")
    raw, signature = _decode64(chunks[1]), _decode64(chunks[2])
    if len(raw) > 4096 or len(signature) != 64:
        raise LicenceError("Invalid company licence.")
    try:
        public_bytes = base64.b64decode(public_key.strip(), validate=True)
        if len(public_bytes) != 32:
            raise LicenceError("Invalid verification key.")
        Ed25519PublicKey.from_public_bytes(public_bytes).verify(signature, raw)
    except (InvalidSignature, ValueError, TypeError, binascii.Error) as exc:
        raise LicenceError("Company licence signature could not be verified.") from exc
    try:
        claims = json.loads(raw)
        if not isinstance(claims, dict):
            raise ValueError("Claims must be an object.")
        if claims["issuer"] != ISSUER or claims["audience"] != AUDIENCE:
            raise ValueError("Unsupported issuer/audience.")
        company = claims["company"]
        customer = claims["customer"]
        plan = claims["plan"]
        if not isinstance(company, str) or not COMPANY.fullmatch(company):
            raise ValueError("Invalid company ID.")
        if not isinstance(customer, str) or not 1 <= len(customer.strip()) <= 160:
            raise ValueError("Invalid licence holder.")
        if not isinstance(plan, str) or plan not in PLANS:
            raise ValueError("Unsupported plan.")
        for key in ("licence_id", "installation_id"):
            uuid.UUID(claims[key])
        if type(claims["grace_days"]) is not int or not 0 <= claims["grace_days"] <= 7:
            raise ValueError("Invalid grace days.")
        when = datetime.fromisoformat(claims["issued_at"])
        if when.tzinfo is None or when.utcoffset() is None:
            raise ValueError("Issue date needs a time zone.")
        starts = date.fromisoformat(claims["starts_on"])
        ends = date.fromisoformat(claims["expires_on"])
        if ends < starts:
            raise ValueError("Expiry precedes start.")
    except (KeyError, ValueError, TypeError) as exc:
        raise LicenceError("Invalid company licence claims.") from exc
    return claims


def company_licence_status(company_slug, *, today=None):
    from .models import BroadcastCompany, CompanyLicence

    today = today or timezone.now().date()
    output = {
        "scope": "company", "company": company_slug, "company_name": None,
        "state": "unlicensed", "permitted": False, "customer": None,
        "plan": None, "expires_on": None, "grace_until": None,
    }
    company = BroadcastCompany.objects.filter(slug=company_slug).first()
    if company is None:
        return {**output, "state": "unknown_company"}
    output["company_name"] = company.name
    if not company.is_active:
        return {**output, "state": "company_suspended"}
    record = CompanyLicence.objects.filter(company=company).first()
    bootstrap = getattr(settings, "AIRVOTE_COMPANY_BOOTSTRAP_LICENCES", {})
    token = (record.signed_token if record else
             bootstrap.get(company_slug) if isinstance(bootstrap, dict) else None)
    if not token:
        return output
    key = getattr(settings, "AIRVOTE_LICENCE_PUBLIC_KEY", "")
    installation = getattr(settings, "AIRVOTE_LICENCE_INSTALLATION_ID", "")
    if not key or not installation:
        return {**output, "state": "misconfigured"}
    try:
        claims = decode_company_licence(token, key)
    except LicenceError:
        return {**output, "state": "invalid"}
    if claims["company"] != company_slug or claims["installation_id"] != installation:
        return {**output, "state": "invalid"}
    output.update(customer=claims["customer"], plan=claims["plan"],
                  expires_on=claims["expires_on"])
    if record and record.revoked_at is not None:
        return {**output, "state": "revoked"}
    starts, ends = date.fromisoformat(claims["starts_on"]), date.fromisoformat(claims["expires_on"])
    grace = ends + timedelta(days=claims["grace_days"])
    output["grace_until"] = grace.isoformat()
    if today < starts:
        return {**output, "state": "not_started"}
    if today <= ends:
        return {**output, "state": "active", "permitted": True}
    if today <= grace:
        return {**output, "state": "grace", "permitted": True}
    return {**output, "state": "expired"}


def company_status_for_station(station, *, today=None):
    from .models import CompanyStation

    record = CompanyStation.objects.select_related("company").filter(station_slug=station).first()
    if record is None:
        return {"scope": "company", "station": station, "company": None,
                "company_name": None, "state": "unassigned_company", "permitted": False,
                "customer": None, "plan": None, "expires_on": None, "grace_until": None}
    return {**company_licence_status(record.company_id, today=today), "station": station}
