from django.http import JsonResponse, HttpResponseForbidden


class WorkspaceSecurityMiddleware:
    """Protect staff responses from caching and require a real station assignment."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.path == "/admin/login/" and not request.user.is_authenticated:
            from django.shortcuts import redirect

            return redirect("/accounts/login/?next=/admin/")
        private = not request.path.startswith(("/webhook/", "/static/"))
        if private and request.user.is_authenticated and not request.user.is_superuser:
            from apps.accounts.models import Station

            profile = getattr(request.user, "profile", None)
            if not profile or profile.station not in Station.values:
                if request.path.startswith("/api/"):
                    return JsonResponse(
                        {"error": "Ask an administrator to assign your station."},
                        status=403,
                    )
                return HttpResponseForbidden(
                    "Ask an administrator to assign your station."
                )
        response = self.get_response(request)
        if private:
            response["Cache-Control"] = "no-store, private"
            response["Referrer-Policy"] = "same-origin"
            response["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        if request.path == "/" or request.path.startswith("/accounts/"):
            response["Content-Security-Policy"] = (
                "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' https: data:; connect-src 'self'; object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'"
            )
        return response


class LocalWebhookHostMiddleware:
    """Keep the local dashboard and DEBUG pages off the configured public tunnel."""
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        from django.conf import settings
        hosts = settings.LOCAL_WEBHOOK_HOSTS
        if hosts:
            host = request.get_host().split(":", 1)[0].lower().rstrip(".")
            if host in hosts and request.path not in {
                "/webhook/bird/", "/webhook/telegram/", "/webhook/whatsapp/"
            }:
                return JsonResponse({"error": "Not found."}, status=404)
        response = self.get_response(request)
        if hosts and host in hosts:
            # Never send a local DEBUG traceback to an external webhook caller.
            if response.status_code >= 500:
                return JsonResponse({"error": "Webhook processing unavailable."}, status=503)
            if response.status_code >= 400 and response.get("Content-Type", "").startswith("text/html"):
                return JsonResponse({"error": "Webhook request rejected."}, status=response.status_code)
        return response
