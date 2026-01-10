from .models import Station


_STATION_LOGOS = {
    Station.RADIO_ZIMBABWE: 'images/radioZim.jpg',
    Station.NATIONAL_FM: 'images/national_fm.svg',
    Station.POWER_FM: 'images/power_fm.svg',
    Station.CLASSIC_263: 'images/classic_263.svg',
    Station.CENTRAL_RADIO: 'images/central_radio.svg',
    Station.KHULUMANI_FM: 'images/khulumani_fm.svg',
}

_STATION_NAMES = {
    Station.RADIO_ZIMBABWE: 'Radio Zimbabwe',
    Station.NATIONAL_FM: 'National FM',
    Station.POWER_FM: 'Power FM',
    Station.CLASSIC_263: 'Classic 263',
    Station.CENTRAL_RADIO: 'Central Radio',
    Station.KHULUMANI_FM: 'Khulumani FM',
}


def get_active_station(request) -> str:
    """
    Get the active station for the current request.
    
    For superusers: Check session for switched station, fallback to profile station
    For regular users: Use their profile station
    Fallback: Radio Zimbabwe
    """
    try:
        user = getattr(request, 'user', None)
        if not user or not getattr(user, 'is_authenticated', False):
            return Station.RADIO_ZIMBABWE

        # Superusers can switch stations via session
        if getattr(user, 'is_superuser', False):
            session = getattr(request, 'session', None)
            switched_station = session.get('switched_station') if session else None
            if switched_station and switched_station in [s[0] for s in Station.choices]:
                return switched_station

        # Regular users use their profile station
        profile = getattr(user, 'profile', None)
        if profile and getattr(profile, 'station', None):
            return profile.station

        return Station.RADIO_ZIMBABWE
    except Exception:
        # Never let a context processor-dependent helper break page rendering
        return Station.RADIO_ZIMBABWE


def get_active_station_display(request) -> str:
    """Get the display name for the active station."""
    station = get_active_station(request)
    return _STATION_NAMES.get(station, 'Radio Zimbabwe')


def station_branding(request):
    """Context processor for station branding in templates."""
    try:
        station = get_active_station(request)
        station_name = _STATION_NAMES.get(station, 'Radio Zimbabwe')
        station_logo = _STATION_LOGOS.get(station, _STATION_LOGOS[Station.RADIO_ZIMBABWE])

        # Check if superuser can switch stations
        user = getattr(request, 'user', None)
        can_switch_station = bool(user and getattr(user, 'is_authenticated', False) and getattr(user, 'is_superuser', False))

        return {
            'branding_station_name': station_name,
            'branding_station_logo': station_logo,
            'active_station': station,
            'can_switch_station': can_switch_station,
            'station_choices': Station.choices if can_switch_station else [],
        }
    except Exception:
        # Absolute fallback: keep templates rendering even if Station/model/session/user is misbehaving
        return {
            'branding_station_name': 'Radio Zimbabwe',
            'branding_station_logo': _STATION_LOGOS.get(Station.RADIO_ZIMBABWE, 'images/radioZim.jpg'),
            'active_station': Station.RADIO_ZIMBABWE,
            'can_switch_station': False,
            'station_choices': [],
        }
