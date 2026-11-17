from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_GET

from apps.accounts.context_processors import get_active_station
from .verification import licence_status


@require_GET
@login_required
def status(request):
    response = licence_status(get_active_station(request))
    response["enforced"] = bool(__import__("django.conf", fromlist=["settings"]).settings.AIRVOTE_LICENSING_ENFORCED)
    return JsonResponse(response)
