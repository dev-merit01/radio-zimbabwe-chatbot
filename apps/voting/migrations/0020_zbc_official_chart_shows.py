"""Populate the official Nov 2026 ZBC programme list without touching votes.

Source: ZBC_Chart_Shows_Visual_Enhanced.docx (one page, six stations).
Broadcast times are NOT voting windows. All newly configured shows remain
disabled until admin explicitly sets and announces voting periods and codes.
"""
from datetime import datetime, time
from zoneinfo import ZoneInfo

from django.db import migrations, models


# station, programme name, preferred code, type, size, broadcast day (Mon=0),
# broadcast starts/ends, presenter as stated, legacy template to reuse,
# confirmed 1–31 December 2026 voting period (only where the source says so).
OFFICIAL = [
    ("radio_zimbabwe", "Gospel 20", "G20", "weekly", 20, 5,
     "08:00", "09:00", "Thulani Siziba", "GOS20", False),
    ("radio_zimbabwe", "Glow Petroleum Top 20", "GP20", "weekly", 20, 5,
     "12:00", "13:00", "", "SAT20", False),
    ("radio_zimbabwe", "Top 50", "Y50", "year_end", 50, 5,
     None, None, "", "EOY", True),

    ("national_fm", "National FM Top 20", "N20", "weekly", 20, 5,
     "11:00", "12:00", "Soneni", "SAT20", False),
    ("national_fm", "National FM Gospel 10", "NG10", "weekly", 10, 6,
     "10:00", "11:00", "Rachel", "GOS20", False),
    ("national_fm", "National FM Top 50", "NY50", "year_end", 50, 5,
     None, None, "Soneni", "EOY", True),

    ("classic_263", "Gospel 20", "CG20", "weekly", 20, 6,
     "19:00", "20:00", "Rumbidzai Matinanga", "GOS20", False),
    ("classic_263", "Traffic Weekend Chart Show Top 20", "CT20", "weekly", 20, 5,
     "12:00", "13:00", "Obert Machiridza", "SAT20", False),
    ("classic_263", "Gospel End-of-Year Top 50", "CG50", "year_end", 50, 5,
     None, None, "", "EOY", False),
    ("classic_263", "Traffic Weekend Chart Show Top 50", "CT50", "year_end", 50, 5,
     None, None, "", None, False),

    ("power_fm", "Power FM Top 40", "P40", "weekly", 40, 5,
     "06:00", "10:00", "Promethius", "SAT20", False),
    ("power_fm", "Gospel Top 20", "PG20", "weekly", 20, 6,
     "18:00", "21:00", "Rachel", "GOS20", False),
    ("power_fm", "Power FM Top 50", "P50", "year_end", 50, 5,
     None, None, "", "EOY", False),
    ("power_fm", "Power FM Gospel 50", "PG50", "year_end", 50, 5,
     None, None, "", None, False),

    ("khulumani_fm", "Khulumani FM Top 20", "K20", "weekly", 20, 6,
     "12:00", "13:00", "Mphumelelo Mandimande", "SAT20", False),
    ("khulumani_fm", "Magnificent 10 – Hip-hop", "KH10", "weekly", 10, 5,
     "08:00", "09:00", "Thorne Leraq", None, False),
    ("khulumani_fm", "Gospel 10", "KG10", "weekly", 10, 6,
     "19:00", None, "Sandisiwe", "GOS20", False),

    ("central_radio", "Central Radio Top 20 Chart Show", "C20", "weekly", 20, 5,
     "08:00", "12:00", "Madeleine Runyambo", "SAT20", False),
]

TEMPLATE_NAMES = {
    "SAT20": "Saturday Top 20",
    "GOS20": "Sunday Gospel Top 20",
    "EOY": "End-of-Year Top 50",
}


def _unused_template(show, models_to_check):
    """Only repurpose generic unconfigured seed templates, never live shows."""
    if (show.name != TEMPLATE_NAMES.get(show.code)
            or show.is_enabled or show.is_archived):
        return False
    for model, lookup in models_to_check:
        if model.objects.filter(**{lookup: show.pk}).exists():
            return False
    return True


def _available_code(ChartShow, station, preferred, exclude=None):
    """Generated codes stay short and unique *within* a station."""
    code = preferred
    suffix = 2
    while ChartShow.objects.filter(station=station, code=code).exclude(pk=exclude).exists():
        code = f"{preferred}{suffix}"
        suffix += 1
    return code


def seed_official_chart_shows(apps, schema_editor):
    ChartShow = apps.get_model("voting", "ChartShow")
    RawVote = apps.get_model("voting", "RawVote")
    Inbound = apps.get_model("voting", "InboundEvent")
    WeeklyChart = apps.get_model("voting", "WeeklyChart")
    Edition = apps.get_model("voting", "EligibleEdition")
    Presenter = apps.get_model("accounts", "PresenterAssignment")
    Unassigned = apps.get_model("voting", "UnassignedVote")
    Spam = apps.get_model("voting", "SuspectedSpamVote")
    references = [
        (RawVote, "chart_show_id"), (Inbound, "chart_show_id"),
        (WeeklyChart, "chart_show_id"), (Edition, "chart_show_id"),
        (Presenter, "chart_show_id"), (Unassigned, "resolved_show_id"),
        (Spam, "chart_show_id"),
    ]
    tz = ZoneInfo("Africa/Harare")
    december_open = datetime(2026, 12, 1, 0, 0, tzinfo=tz)
    # Exclusive end includes all of 31 December.
    december_close = datetime(2027, 1, 1, 0, 0, tzinfo=tz)

    for (station, name, code, kind, size, day, start_text, end_text,
         presenter, template, december) in OFFICIAL:
        show = ChartShow.objects.filter(
            station=station, name__iexact=name
        ).order_by("id").first()
        new_or_pristine = False
        if show is None and template:
            candidate = ChartShow.objects.filter(
                station=station, code=template, kind=kind,
            ).first()
            if candidate and _unused_template(candidate, references):
                show = candidate
                new_or_pristine = True
        if show is None:
            show = ChartShow(station=station)
            new_or_pristine = True

        # Presenter and broadcast time are metadata, not voting restrictions.
        show.presenter_name = presenter
        show.broadcast_start = time.fromisoformat(start_text) if start_text else None
        show.broadcast_end = time.fromisoformat(end_text) if end_text else None
        show.broadcast_weekday = day if new_or_pristine else show.broadcast_weekday

        if new_or_pristine:
            show.name = name
            show.code = _available_code(ChartShow, station, code, show.pk)
            show.kind = kind
            show.chart_size = size
            show.is_enabled = False
            show.manual_open = False
            show.schedule_mode = "campaign" if kind == "year_end" else "manual"
            # Source explicitly confirms these *two* voting date ranges.
            # Unconfirmed annual periods stay blank and cannot be enabled.
            show.campaign_start = december_open if december else None
            show.campaign_end = december_close if december else None
        show.save()

    # Pristine generic demos with no programme in the supplied document are
    # hidden as archived (never deleted); all historic data is still intact.
    # Luzibo Radio was not present in the document: do not modify its shows.
    for station in ("khulumani_fm", "central_radio"):
        for code in ("SAT20", "GOS20", "EOY"):
            show = ChartShow.objects.filter(station=station, code=code).first()
            if show and _unused_template(show, references):
                show.is_archived = True
                show.save(update_fields=["is_archived", "updated_at"])


class Migration(migrations.Migration):
    dependencies = [
        ("voting", "0019_eligible_editions"),
        ("accounts", "0004_presenter_assignment"),
    ]

    operations = [
        migrations.AddField(
            model_name="chartshow",
            name="presenter_name",
            field=models.CharField(max_length=120, blank=True),
        ),
        migrations.AddField(
            model_name="chartshow",
            name="broadcast_start",
            field=models.TimeField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="chartshow",
            name="broadcast_end",
            field=models.TimeField(null=True, blank=True),
        ),
        migrations.RunPython(seed_official_chart_shows, migrations.RunPython.noop),
    ]
