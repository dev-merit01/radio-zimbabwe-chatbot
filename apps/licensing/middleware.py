"""Enforce station licences at the central server, not the Windows UI.

No webhook/worker pathways are intercepted: live votes remain durable during
a licence dispute or renewal. Publication and staff APIs are gated.
"""
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render

from apps.accounts.context_processors import get_active_station
from .verification import licence_status


class StationLicenceMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        path = request.path
        guarded = path == "/" or (path.startswith("/api/") and
                                  path not in ("/api/desktop/status", "/api/licensing/status"))
        if (getattr(settings, "AIRVOTE_LICENSING_ENFORCED", False)
                and guarded and request.user.is_authenticated):
            station = get_active_station(request)
            state = licence_status(station)
            if not state["permitted"]:
                if path.startswith("/api/"):
                    return JsonResponse(
                        {"error": "A valid AirVote station licence is required.",
                         "code": "LICENCE_REQUIRED", "licence": state},
                        status=402,
                    )
                return render(request, "licensing/required.html",
                              {"licence": state}, status=402)
        return self.get_response(request)
