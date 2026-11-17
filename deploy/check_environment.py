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
    if settings.AI_ALL_VOTES:
        if not settings.OPENAI_API_KEY:
            errors.append("AI_ALL_VOTES requires OPENAI_API_KEY in this service")
        if not settings.OPENAI_MODEL:
            errors.append("AI_ALL_VOTES requires an OPENAI_MODEL")

    # Verify all configured transitional grants BEFORE enabling access enforcement.
    # This catches corrupt activation values during a non-disruptive deployment.
    bootstrap = getattr(settings, "AIRVOTE_BOOTSTRAP_LICENCES", {})
    if not isinstance(bootstrap, dict):
        errors.append("AIRVOTE_BOOTSTRAP_LICENCES must be a JSON object")
    elif bootstrap:
        from apps.licensing.verification import decode_and_verify, LicenceError
        from datetime import date

        public_key = settings.AIRVOTE_LICENCE_PUBLIC_KEY
        installation_id = settings.AIRVOTE_LICENCE_INSTALLATION_ID
        if not public_key or not installation_id:
            errors.append("Licence bootstrap requires the public key and installation ID")
        else:
            for station, token in bootstrap.items():
                if not isinstance(station, str) or not station:
                    errors.append("Invalid licence bootstrap station key")
                    continue
                try:
                    claims = decode_and_verify(token, public_key)
                    if (claims["station"] != station or
                            claims["installation_id"] != installation_id):
                        errors.append(f"Licence for {station} does not match this installation")
                    elif date.fromisoformat(claims["expires_on"]) < date.today():
                        errors.append(f"Transitional licence for {station} is expired")
                except (LicenceError, ValueError, TypeError):
                    errors.append(f"Invalid signed bootstrap licence for {station}")
    if settings.AIRVOTE_LICENSING_ENFORCED and (
        not settings.AIRVOTE_LICENCE_PUBLIC_KEY or
        not settings.AIRVOTE_LICENCE_INSTALLATION_ID
    ):
        errors.append("Enforced licensing requires signing public key and installation UUID")

    # Validate signed AV2 company transition licences without a DB connection.
    # A malformed or expired company token must never silently lock staff out.
    company_bootstrap = getattr(settings, "AIRVOTE_COMPANY_BOOTSTRAP_LICENCES", {})
    if not isinstance(company_bootstrap, dict):
        errors.append("AIRVOTE_COMPANY_BOOTSTRAP_LICENCES must be a JSON object")
    elif company_bootstrap:
        from apps.licensing.company_verification import decode_company_licence
        from apps.licensing.verification import LicenceError
        from datetime import date, timedelta

        key = settings.AIRVOTE_LICENCE_PUBLIC_KEY
        installation = settings.AIRVOTE_LICENCE_INSTALLATION_ID
        if not key or not installation:
            errors.append("Company licence grants require public key and installation ID")
        else:
            for company, token in company_bootstrap.items():
                try:
                    claims = decode_company_licence(token, key)
                    if (claims["company"] != company or
                            claims["installation_id"] != installation):
                        errors.append(f"Company licence for {company} mismatches this deployment")
                    elif (date.fromisoformat(claims["expires_on"]) +
                          timedelta(days=claims["grace_days"])) < date.today():
                        errors.append(f"Company licence for {company} has expired")
                except (LicenceError, ValueError, TypeError):
                    errors.append(f"Invalid signed company licence for {company}")
    if settings.AIRVOTE_COMPANY_LICENSING_ENFORCED and (
        not settings.AIRVOTE_LICENCE_PUBLIC_KEY or
        not settings.AIRVOTE_LICENCE_INSTALLATION_ID
    ):
        errors.append("Company licensing requires public verification key and installation ID")

    if not settings.SECURE_SSL_REDIRECT or not getattr(settings, "SECURE_PROXY_SSL_HEADER", None):
        errors.append("Enable SECURE_SSL_REDIRECT and TRUST_PROXY behind Railway HTTPS")
    if errors:
        raise SystemExit("Production configuration incomplete: " + "; ".join(errors))


if __name__ == "__main__":
    validate()
