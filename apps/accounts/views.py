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
@user_passes_test(lambda u: u.is_superuser)
def switch_station(request):
    """
    Allow superusers to switch to a different station.
    Affects both dashboard AND admin views.
    """
    if request.method == "POST":
        station = request.POST.get("station")
        valid_stations = [s[0] for s in Station.choices]

        if station in valid_stations:
            request.session["switched_station"] = station
            # Get display name
            station_display = dict(Station.choices).get(station, station)
            messages.success(request, f"✅ Switched to {station_display}")
        else:
            messages.error(request, "Invalid station selected.")

        # Redirect back to where user came from, or dashboard
        next_url = (
            request.POST.get("next") or request.META.get("HTTP_REFERER") or "dashboard"
        )
        return redirect(safe_next(request, next_url))

    # GET request - return JSON list of stations
    return JsonResponse(
        {
            "current_station": request.session.get("switched_station", ""),
            "stations": [{"value": s[0], "label": s[1]} for s in Station.choices],
        }
    )


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
