"""Fail-closed routing of authenticated Bird inbound WhatsApp events by destination number.

Do not trust a station field supplied by the incoming contact. The destination
number comes from Bird's signed event payload.
"""
import re

from django.conf import settings

from apps.accounts.models import Station


_E164 = re.compile(r"\+?[1-9]\d{7,14}\Z")


def normalise_number(value):
    """Return a strict E.164-style number, or None for invalid input."""
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not _E164.fullmatch(value):
        return None
    return "+" + value.lstrip("+")


def bird_number_routes():
    """Parse the server-owned number:station map. Invalid config fails closed."""
    raw = getattr(settings, "BIRD_NUMBER_STATIONS", "").strip()
    if not raw:
        return {}

    routes = {}
    assigned = set()
    for entry in raw.split(","):
        number, separator, station = entry.partition(":")
        number = normalise_number(number)
        station = station.strip()
        if (not separator or number is None or station not in Station.values
                or number in routes or station in assigned):
            raise ValueError("Invalid or duplicate Bird number-to-station configuration.")
        routes[number] = station
        assigned.add(station)
    return routes


def bird_station_for_message(message):
    """Find the intended station from data.to.phone_number, not the sender."""
    routes = bird_number_routes()
    if not routes:
        # Backward compatibility for single-account installations only.
        return settings.BIRD_STATION
    destination = message.get("to")
    if not isinstance(destination, dict):
        return None
    return routes.get(normalise_number(destination.get("phone_number")))
