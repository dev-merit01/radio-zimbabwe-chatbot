"""Dependency readiness only; no credentials, counts, or exception details."""
from uuid import uuid4
from django.core.cache import cache
from django.db import connection
from django.http import JsonResponse
from django.views.decorators.http import require_GET


@require_GET
def readiness(request):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        key = "readiness:" + uuid4().hex
        cache.set(key, "ok", timeout=10)
        ready = cache.get(key) == "ok"
        cache.delete(key)
        if not ready:
            raise RuntimeError("Cache unavailable")
    except Exception:
        return JsonResponse({"status": "unavailable"}, status=503)
    return JsonResponse({"status": "ok"})
