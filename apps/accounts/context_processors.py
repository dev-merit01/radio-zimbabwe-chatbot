from django.templatetags.static import static

from .models import (
    Station, StationAccess, BroadcastStation, station_exists, station_choices, station_label,
)


_STATION_LOGOS = {
    Station.RADIO_ZIMBABWE: "images/radioZim.jpg",
    Station.NATIONAL_FM: "images/national_fm.webp",
    Station.POWER_FM: "images/power_fm.webp",
    Station.CLASSIC_263: "images/classic_263.svg",
    Station.CENTRAL_RADIO: "images/central_radio.webp",
    Station.KHULUMANI_FM: "images/khulumani_fm.svg",
    Station.LUZIBO_RADIO: "images/luzibo_radio.svg",
}

# Identifiable station artwork sourced from public station profiles. These are
# remote visual assets only; local image assets are preferred where available.
# National FM, Central Radio and Power FM use the supplied artwork hosted locally.
_STATION_LOGO_IMAGES = {
    Station.CLASSIC_263: "https://mediabuy.africa/948-large_default/classic-263.jpg",
    Station.KHULUMANI_FM: "https://mediabuy.africa/557-large_default/khulumani-fm.jpg",
}

_STATION_NAMES = {
    Station.RADIO_ZIMBABWE: "Radio Zimbabwe",
    Station.NATIONAL_FM: "National FM",
    Station.POWER_FM: "Power FM",
    Station.CLASSIC_263: "Classic 263",
    Station.CENTRAL_RADIO: "Central Radio",
    Station.KHULUMANI_FM: "Khulumani FM",
    Station.LUZIBO_RADIO: "Luzibo Radio",
}


def get_active_station(request) -> str:
    """
    Get the active station for the current request.

    For superusers: Check session for switched station, fallback to profile station
    For regular users: Validate the station password grant, otherwise use their profile
    Fallback: Radio Zimbabwe
    """
    try:
        user = getattr(request, "user", None)
        if not user or not getattr(user, "is_authenticated", False):
            return Station.RADIO_ZIMBABWE

        from .permissions import request_presenter_show
        assigned = request_presenter_show(request)
        if assigned:
            # Assigned presenters can never override their station with a
            # stale session or an independently supplied station password.
            return assigned.station

        session = getattr(request, "session", None)
        switched = session.get("switched_station") if session else None
        if station_exists(switched, active_only=not user.is_superuser):
            if user.is_superuser:
                return switched
            version = session.get("station_access_version")
            access = StationAccess.objects.filter(station=switched).first()
            if access and version == str(access.version):
                return switched
            session.pop("switched_station", None)
            session.pop("station_access_version", None)

        # Regular users use their profile station
        profile = getattr(user, "profile", None)
        if profile and station_exists(getattr(profile, "station", None), active_only=True):
            return profile.station

        return Station.RADIO_ZIMBABWE
    except Exception:
        # Authenticated users must fail closed rather than silently seeing
        # Radio Zimbabwe if assignment retrieval is unavailable.
        if getattr(getattr(request, "user", None), "is_authenticated", False):
            raise
        return Station.RADIO_ZIMBABWE


def get_active_station_display(request) -> str:
    """Get the display name for the active station."""
    station = get_active_station(request)
    return station_label(station)


def station_branding(request):
    """Context processor for station branding in templates."""
    try:
        station = get_active_station(request)
        station_row = BroadcastStation.objects.filter(slug=station).values(
            "name", "logo_url",
        ).first()
        station_name = station_row["name"] if station_row else _STATION_NAMES.get(
            station, station
        )
        station_logo = _STATION_LOGOS.get(
            station, "images/airvote/app-icon.png"
        )
        fallback_url = static(station_logo)
        display_url = (
            (station_row or {}).get("logo_url")
            or _STATION_LOGO_IMAGES.get(station, fallback_url)
        )

        # Every signed-in station user can request a password-protected switch
        user = getattr(request, "user", None)
        from .permissions import request_presenter_show
        assigned = request_presenter_show(request)
        can_switch_station = bool(
            user and getattr(user, "is_authenticated", False)
            and assigned is None
        )

        return {
            "branding_station_name": station_name,
            "branding_station_logo": station_logo,
            "branding_station_logo_url": display_url,
            "branding_station_logo_fallback_url": fallback_url,
            "active_station": station,
            "can_switch_station": can_switch_station,
            "presenter_chart_show_name": assigned.name if assigned else None,
            "station_choices": station_choices() if can_switch_station else [],
        }
    except Exception:
        # Absolute fallback: keep templates rendering even if Station/model/session/user is misbehaving
        return {
            "branding_station_name": "Radio Zimbabwe",
            "branding_station_logo": _STATION_LOGOS.get(
                Station.RADIO_ZIMBABWE, "images/radioZim.jpg"
            ),
            "branding_station_logo_url": static("images/radioZim.jpg"),
            "branding_station_logo_fallback_url": static("images/radioZim.jpg"),
            "active_station": Station.RADIO_ZIMBABWE,
            "can_switch_station": False,
            "station_choices": [],
        }
