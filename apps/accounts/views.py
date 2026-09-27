from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import redirect, render
from django.http import JsonResponse
from django.core.cache import cache
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.http import require_POST
import hashlib


def safe_next(request, value):
    return (
        value
        if value
        and url_has_allowed_host_and_scheme(
            value, {request.get_host()}, require_https=request.is_secure()
        )
        else "/"
    )


from .forms import LoginForm
from .models import Station


def login_view(request):
    if request.user.is_authenticated:
        return redirect("dashboard")

    next_url = safe_next(request, request.POST.get("next", request.GET.get("next", "")))

    if request.method == "POST":
        form = LoginForm(request.POST)
        if form.is_valid():
            username = form.cleaned_data["username"]
            password = form.cleaned_data["password"]
            throttle = (
                "login:" + hashlib.sha256(username.casefold().encode()).hexdigest()
            )
            try:
                if cache.add(throttle, 1, 900):
                    attempts = 1
                else:
                    attempts = cache.incr(throttle)
            except Exception:
                messages.error(
                    request,
                    "Sign-in is temporarily unavailable. Please try again shortly.",
                )
                return render(
                    request,
                    "accounts/login.html",
                    {"form": form, "next": next_url},
                    status=503,
                )
            if attempts > 10:
                messages.error(
                    request, "Too many sign-in attempts. Please wait 15 minutes."
                )
                return render(
                    request,
                    "accounts/login.html",
                    {"form": form, "next": next_url},
                    status=429,
                )
            user = authenticate(request, username=username, password=password)
            if user is not None:
                cache.delete(throttle)
                login(request, user)
                return redirect(next_url or "dashboard")
            messages.error(request, "Invalid username or password.")
    else:
        form = LoginForm()

    return render(
        request,
        "accounts/login.html",
        {
            "form": form,
            "next": next_url,
        },
    )


def register_view(request):
    return render(request, "accounts/register.html", status=403)


@login_required
@require_POST
def logout_view(request):
    logout(request)
    return redirect("accounts:login")


@login_required
def switch_station(request):
    from django.contrib.auth.hashers import check_password
    from .models import StationAccess
    from .context_processors import get_active_station
    from apps.voting.models import ReviewAudit

    if request.method == "GET":
        return JsonResponse({"current_station": get_active_station(request),
            "stations": [{"value": s, "label": label} for s, label in Station.choices]})
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)
    station = request.POST.get("station")
    next_url = safe_next(request, request.POST.get("next", "/"))
    if station not in Station.values:
        return JsonResponse({"error": "Invalid station selected."}, status=400)
    previous = get_active_station(request)
    if station == previous:
        return redirect(next_url)
    version = None
    if not request.user.is_superuser:
        throttle = f"station-switch:{request.user.pk}"
        try:
            attempts = 1 if cache.add(throttle, 1, 900) else cache.incr(throttle)
        except Exception:
            return JsonResponse({"error": "Station switching temporarily unavailable."}, status=503)
        if attempts > 10:
            messages.error(request, "Too many station password attempts. Wait 15 minutes.")
            return redirect(next_url)
        access = StationAccess.objects.filter(station=station).first()
        password = request.POST.get("password", "")
        if not access or not password or len(password) > 128 or not check_password(password, access.password_hash):
            messages.error(request, "Station password not accepted. Ask an administrator for access.")
            return redirect(next_url)
        cache.delete(throttle)
        version = str(access.version)
    request.session["switched_station"] = station
    request.session["station_access_version"] = version
    request.session.cycle_key()
    ReviewAudit.objects.create(station=station, actor=request.user,
        action="station_switch", details={"from": previous})
    messages.success(request, f"Switched to {Station(station).label}")
    return redirect(next_url)


@login_required
@user_passes_test(lambda u: u.is_superuser)
@require_POST
def clear_station_switch(request):
    """Clear the station switch and return to user's default station."""
    if "switched_station" in request.session:
        del request.session["switched_station"]
        messages.success(request, "✅ Returned to your default station.")

    next_url = (
        request.GET.get("next") or request.META.get("HTTP_REFERER") or "dashboard"
    )
    return redirect(safe_next(request, next_url))
