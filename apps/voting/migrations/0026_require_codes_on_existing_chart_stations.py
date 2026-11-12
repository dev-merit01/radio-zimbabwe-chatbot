"""Safely require a programme code at the existing broadcast stations.

Every station has several chart shows. Uncoded or unknown-code WhatsApp
messages must be HELD for review instead of entering the historical tally.
This migration does NOT open closed shows, count any votes, alter show codes,
or change already received/assigned votes. Newly created stations retain
their explicit opt-in onboarding policy.
"""
from django.db import migrations


def require_codes_for_existing_stations(apps, schema_editor):
    Config = apps.get_model("voting", "StationVotingConfig")
    ChartShow = apps.get_model("voting", "ChartShow")
    existing = (
        "radio_zimbabwe", "national_fm", "power_fm", "classic_263",
        "central_radio", "khulumani_fm", "luzibo_radio",
    )
    for station in existing:
        # Only change stations already provisioned with distinct programme
        # identities; no code-free single-chart installation is affected.
        if ChartShow.objects.filter(
            station=station, kind__in=("weekly", "year_end")
        ).exclude(code="").exists():
            Config.objects.update_or_create(
                station=station, defaults={"require_codes": True}
            )


class Migration(migrations.Migration):
    dependencies = [("voting", "0025_verified_vote_cache")]

    operations = [
        migrations.RunPython(
            require_codes_for_existing_stations, migrations.RunPython.noop
        ),
    ]
