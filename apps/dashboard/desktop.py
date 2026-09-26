"""Public compatibility handshake; never returns staff, votes or credentials."""

from django.http import JsonResponse
from django.views.decorators.http import require_GET


@require_GET
def status(request):
    return JsonResponse(
        {"application": "radio-zimbabwe-voting-studio", "desktop_api": 1}
    )
