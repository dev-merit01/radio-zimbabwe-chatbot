"""Company-wide licence status and authenticated activation."""
from datetime import datetime

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import HttpResponseForbidden, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_http_methods

from apps.accounts.context_processors import get_active_station
from .company_verification import company_licence_status, decode_company_licence
from .models import BroadcastCompany, CompanyLicence, StationLicence
from apps.accounts.models import station_exists
from .verification import decode_and_verify
from .verification import LicenceError, licence_status


@require_GET
@login_required
def status(request):
    result = licence_status(get_active_station(request))
    result["enforced"] = bool(
        settings.AIRVOTE_COMPANY_LICENSING_ENFORCED or settings.AIRVOTE_LICENSING_ENFORCED
    )
    result["scope"] = "company" if settings.AIRVOTE_COMPANY_LICENSING_ENFORCED else "legacy_station"
    return JsonResponse(result)


@login_required
@require_http_methods(["GET", "POST"])
def activate(request):
    """Install a vendor-signed company token. Staff may NOT mint licences."""
    if not request.user.is_superuser:
        return HttpResponseForbidden("Only the AirVote administrator may activate licences.")
    context = {
        "installation_id": settings.AIRVOTE_LICENCE_INSTALLATION_ID,
        "companies": BroadcastCompany.objects.order_by("name"),
    }
    if request.method == "GET":
        return render(request, "licensing/activate.html", context)
    token = request.POST.get("licence_token", "").strip()
    try:
        if not settings.AIRVOTE_LICENCE_PUBLIC_KEY:
            raise LicenceError("The company's public verification key is not configured.")
        # Retain legacy AV1 activation only until the final company cutover.
        if token.startswith("AV1.") and not settings.AIRVOTE_COMPANY_LICENSING_ENFORCED:
            return _activate_legacy_station(request, token, context)
        claims = decode_company_licence(token, settings.AIRVOTE_LICENCE_PUBLIC_KEY)
        if claims["installation_id"] != settings.AIRVOTE_LICENCE_INSTALLATION_ID:
            raise LicenceError("This licence was issued for a different AirVote installation.")
        with transaction.atomic():
            company = BroadcastCompany.objects.select_for_update().filter(
                slug=claims["company"]
            ).first()
            if company is None:
                raise LicenceError("Create this broadcasting company before activation.")
            current = CompanyLicence.objects.select_for_update().filter(company=company).first()
            if current and current.revoked_at is not None:
                if datetime.fromisoformat(claims["issued_at"]) <= current.revoked_at:
                    raise LicenceError("Licence revoked. Obtain a newly issued company licence.")
            CompanyLicence.objects.update_or_create(
                company=company,
                defaults={"signed_token": token, "revoked_at": None,
                          "revocation_reason": ""},
            )
        status_data = company_licence_status(claims["company"])
        context["success"] = (
            f"Company licence installed for {company.name}: "
            f"{status_data['state']}, expiry {claims['expires_on']}."
        )
    except LicenceError as exc:
        context["error"] = str(exc)
        return render(request, "licensing/activate.html", context, status=400)
    return render(request, "licensing/activate.html", context)


def _activate_legacy_station(request, token, context):
    """Rollback-safe AV1 importer. Disabled completely under company enforcement."""
    try:
        claims = decode_and_verify(token, settings.AIRVOTE_LICENCE_PUBLIC_KEY)
        if claims["installation_id"] != settings.AIRVOTE_LICENCE_INSTALLATION_ID:
            raise LicenceError("This licence belongs to a different AirVote installation.")
        station = claims["station"]
        if not station_exists(station):
            raise LicenceError("Create the licensed station before activation.")
        with transaction.atomic():
            current = StationLicence.objects.select_for_update().filter(station=station).first()
            if current and current.revoked_at is not None:
                if datetime.fromisoformat(claims["issued_at"]) <= current.revoked_at:
                    raise LicenceError("This legacy licence has been revoked.")
            StationLicence.objects.update_or_create(
                station=station,
                defaults={"signed_token": token, "revoked_at": None, "revocation_reason": ""},
            )
    except LicenceError as exc:
        context["error"] = str(exc)
        return render(request, "licensing/activate.html", context, status=400)
    context["success"] = (
        f"Legacy station licence installed for {station} during migration. "
        "Convert to a company licence before final cutover."
    )
    return render(request, "licensing/activate.html", context)
