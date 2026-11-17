"""Superuser-controlled broadcasting company registry."""
import json
import re

from django.db import IntegrityError, transaction
from django.http import JsonResponse
from django.utils.text import slugify
from django.views.decorators.http import require_POST

from apps.licensing.models import BroadcastCompany, CompanyStation
from apps.accounts.models import BroadcastStation
from .workspace import api_access

_VALID_SLUG = re.compile(r"^[a-z][a-z0-9_-]{2,47}$")


@require_POST
@api_access("admin")
def create_company(request):
    payload = json.loads(request.body)
    raw = payload.get("name")
    if not isinstance(raw, str):
        raise ValueError("Enter the broadcasting company's legal name.")
    name = " ".join(raw.split())
    if not 3 <= len(name) <= 160:
        raise ValueError("Company name must be between 3 and 160 characters.")
    requested = payload.get("slug")
    slug = slugify(name)[:48] if requested in ("", None) else requested
    if not isinstance(slug, str) or not _VALID_SLUG.fullmatch(slug):
        raise ValueError("Use a company ID of 3–48 lowercase letters, digits, hyphens or underscores.")
    with transaction.atomic():
        if BroadcastCompany.objects.filter(name__iexact=name).exists():
            raise ValueError("A broadcasting company with that name already exists.")
        try:
            company = BroadcastCompany.objects.create(slug=slug, name=name)
        except IntegrityError as exc:
            raise ValueError("This company ID is already registered.") from exc
    return JsonResponse({"ok": True, "company": {
        "slug": company.slug, "name": company.name, "is_active": True,
        "licence_state": "unlicensed",
    }}, status=201)


@require_POST
@api_access("admin")
def update_company(request):
    payload = json.loads(request.body)
    slug = payload.get("slug")
    name = payload.get("name")
    active = payload.get("is_active")
    if not isinstance(slug, str) or not isinstance(name, str):
        raise ValueError("Choose a broadcasting company and provide its name.")
    name = " ".join(name.split())
    if len(name) < 3 or len(name) > 160 or type(active) is not bool:
        raise ValueError("Invalid broadcasting company settings.")
    with transaction.atomic():
        company = BroadcastCompany.objects.select_for_update().filter(slug=slug).first()
        if company is None:
            return JsonResponse({"error": "Company not found."}, status=404)
        if BroadcastCompany.objects.filter(name__iexact=name).exclude(slug=slug).exists():
            raise ValueError("Another broadcasting company already has that name.")
        if not active and company.is_active:
            slugs = CompanyStation.objects.filter(company=company).values_list(
                "station_slug", flat=True
            )
            if BroadcastStation.objects.filter(slug__in=slugs, is_active=True).exists():
                raise ValueError("Deactivate this company's stations before suspending the company.")
        company.name = name
        company.is_active = active
        company.save(update_fields=["name", "is_active", "updated_at"])
    return JsonResponse({"ok": True, "company": {
        "slug": company.slug, "name": company.name, "is_active": company.is_active,
    }})
