"""Mutually authenticated read-only bridge between Cloudflare and AirVote.

Public clients never see this secret. Sign method, exact path/query,
timestamp and SHA-256 body digest. No cookie or Django staff session is used.
"""
import hashlib
import hmac
import time
from functools import wraps

from django.conf import settings
from django.http import JsonResponse


def signing_message(method, path, timestamp, body=b""):
    return "\n".join([str(timestamp), method.upper(), path, hashlib.sha256(body).hexdigest()]).encode("utf-8")


def valid_request(request):
    secret = getattr(settings, "AIRVOTE_WEB_BRIDGE_SECRET", "")
    if not isinstance(secret, str) or len(secret) < 32:
        return False
    timestamp = request.headers.get("X-AirVote-Timestamp", "")
    signature = request.headers.get("X-AirVote-Signature", "")
    if not timestamp.isdecimal() or len(timestamp) > 11 or len(signature) != 64:
        return False
    if abs(int(time.time()) - int(timestamp)) > 60:
        return False
    expected = hmac.new(
        secret.encode("utf-8"),
        signing_message(request.method, request.get_full_path(), timestamp, request.body),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def bridge_only(view):
    @wraps(view)
    def wrapper(request, *args, **kwargs):
        if not valid_request(request):
            response = JsonResponse({"error": "Not authorised."}, status=401)
            response["Cache-Control"] = "no-store"
            return response
        result = view(request, *args, **kwargs)
        result["Cache-Control"] = "private, no-store"
        return result
    return wrapper
