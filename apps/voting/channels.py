"""Stable voting channel labels and privacy-preserving voter identities."""
import hashlib
import hmac
import re

from django.conf import settings


SOURCE_LABELS = {
    "web": "Online portal",
    "bird": "WhatsApp",
    "onemsg": "WhatsApp",
    "whatsapp": "WhatsApp",
    "sms": "SMS",
    "telegram": "Telegram",
    "manual": "Manual entry",
}


def vote_source(provider):
    if provider in {"bird", "onemsg", "whatsapp"}:
        return "whatsapp"
    return provider if provider in {"web", "sms", "telegram", "manual"} else "manual"


def source_label(source):
    return SOURCE_LABELS.get(source, SOURCE_LABELS["manual"])


def normalized_phone(raw):
    """Accept E.164 numbers, plus common Zimbabwe national-number notation."""
    if not isinstance(raw, str):
        return ""
    candidate = re.sub(r"[\s()\-]", "", raw.strip())
    if re.fullmatch(r"0[0-9]{9}", candidate):
        candidate = "+263" + candidate[1:]
    if re.fullmatch(r"[1-9][0-9]{7,14}", candidate):
        candidate = "+" + candidate
    return candidate if re.fullmatch(r"\+[1-9][0-9]{7,14}", candidate) else ""


def phone_fingerprint(phone):
    """Never store a visitor's raw telephone number with a public vote."""
    normalized = normalized_phone(phone)
    if not normalized:
        return ""
    return hmac.new(
        settings.SECRET_KEY.encode("utf-8"),
        ("airvote-voter-phone-v1:" + normalized).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
