"""Fail closed when production services are missing required configuration."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "radio_zimbabwe.settings")
from django.conf import settings  # noqa: E402


def validate():
    errors = []
    if settings.DEBUG:
        errors.append("DJANGO_DEBUG must be False")
    if settings.DATABASES["default"]["ENGINE"] != "django.db.backends.postgresql":
        errors.append("DATABASE_URL must point to PostgreSQL")
    if not os.environ.get("REDIS_URL", "").startswith(("redis://", "rediss://")):
        errors.append("REDIS_URL must be configured")
    if len(settings.SECRET_KEY) < 50 or settings.SECRET_KEY.startswith("build-only"):
        errors.append("DJANGO_SECRET_KEY must be a unique random value of at least 50 characters")
    if not settings.ALLOWED_HOSTS or "*" in settings.ALLOWED_HOSTS:
        errors.append("DJANGO_ALLOWED_HOSTS must list explicit hostnames")
    if settings.LOCAL_WEBHOOK_HOSTS:
        errors.append("LOCAL_WEBHOOK_HOSTS must be empty on the hosted server")
    if not settings.SECURE_SSL_REDIRECT or not getattr(settings, "SECURE_PROXY_SSL_HEADER", None):
        errors.append("Enable SECURE_SSL_REDIRECT and TRUST_PROXY behind Railway HTTPS")
    if errors:
        raise SystemExit("Production configuration incomplete: " + "; ".join(errors))


if __name__ == "__main__":
    validate()
