from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from apps.accounts.models import station_exists
from .models import StationLicence
from .verification import LicenceError, decode_and_verify
from django.http import JsonResponse
from django.views.decorators.http import require_GET

from apps.accounts.context_processors import get_active_station
from .verification import licence_status


@require_GET
@login_required
def status(request):
    response = licence_status(get_active_station(request))
    response["enforced"] = settings.AIRVOTE_LICENSING_ENFORCED
    return JsonResponse(response)


@login_required
@require_http_methods(["GET", "POST"])
def activate(request):
    """A staff superuser can INSTALL, but never ISSUE, a vendor-signed licence."""
    if not request.user.is_superuser:
        from django.http import HttpResponseForbidden
        return HttpResponseForbidden("Only the AirVote administrator may activate licences.")
    if request.method == "GET":
        return render(request, "licensing/activate.html")
    token = request.POST.get("licence_token", "").strip()
    try:
        if not settings.AIRVOTE_LICENCE_PUBLIC_KEY:
            raise LicenceError("The public verification key is not configured.")
        claims = decode_and_verify(token, settings.AIRVOTE_LICENCE_PUBLIC_KEY)
        if not station_exists(claims["station"]):
            raise LicenceError("This station must be created first.")
        with transaction.atomic():
            current = StationLicence.objects.select_for_update().filter(
                station=claims["station"]
            ).first()
            if current and current.revoked_at and current.signed_token == token:
                raise LicenceError("This licence has been revoked. Obtain a new one.")
            StationLicence.objects.update_or_create(
                station=claims["station"],
                defaults={"signed_token": token, "revoked_at": None, "revocation_reason": ""},
            )
    except LicenceError as exc:
        return render(request, "licensing/activate.html", {"error": str(exc)}, status=400)
    return render(request, "licensing/activate.html", {
        "success": f"Verified licence installed for {claims['station']}. "
                   f"Expires {claims['expires_on']} (UTC)."
    })
