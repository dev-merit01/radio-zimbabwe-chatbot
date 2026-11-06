"""Add programme identities without reassigning any historical station votes."""
import datetime

import django.db.models.deletion
from django.db import migrations, models
from django.db.models import Q


STATIONS = [
    "radio_zimbabwe", "national_fm", "power_fm", "classic_263",
    "central_radio", "khulumani_fm", "luzibo_radio",
]


def preserve_legacy_data(apps, schema_editor):
    ChartShow = apps.get_model("voting", "ChartShow")
    Config = apps.get_model("voting", "StationVotingConfig")
    RawVote = apps.get_model("voting", "RawVote")
    InboundEvent = apps.get_model("voting", "InboundEvent")
    WeeklyChart = apps.get_model("voting", "WeeklyChart")
    for station in STATIONS:
        legacy, _ = ChartShow.objects.get_or_create(
            station=station, kind="legacy",
            defaults={
                "name": "Historical station chart",
                "code": "",
                "is_enabled": False,
                "chart_size": 20,
            },
        )
        Config.objects.get_or_create(station=station, defaults={"require_codes": False})
        RawVote.objects.filter(station=station, chart_show__isnull=True).update(chart_show=legacy)
        InboundEvent.objects.filter(station=station, chart_show__isnull=True).update(chart_show=legacy)
        WeeklyChart.objects.filter(station=station, chart_show__isnull=True).update(chart_show=legacy)


class Migration(migrations.Migration):
    dependencies = [("voting", "0015_luzibo_radio")]

    operations = [
        migrations.CreateModel(
            name="ChartShow",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("station", models.CharField(max_length=32, db_index=True, choices=[
                    ("radio_zimbabwe", "Radio Zimbabwe"), ("national_fm", "National FM"),
                    ("power_fm", "Power FM"), ("classic_263", "Classic 263"),
                    ("central_radio", "Central Radio"), ("khulumani_fm", "Khulumani FM"),
                    ("luzibo_radio", "Luzibo Radio"),
                ])),
                ("name", models.CharField(max_length=120)),
                ("code", models.CharField(max_length=16, blank=True)),
                ("kind", models.CharField(max_length=16, default="weekly", choices=[
                    ("legacy", "Legacy station chart"),
                    ("weekly", "Regular chart show"), ("year_end", "End-of-year campaign"),
                ])),
                ("schedule_mode", models.CharField(max_length=16, default="weekly", choices=[
                    ("weekly", "Weekly schedule"), ("manual", "Manual"), ("campaign", "Date range"),
                ])),
                ("is_enabled", models.BooleanField(default=False)),
                ("manual_open", models.BooleanField(default=False)),
                ("broadcast_weekday", models.PositiveSmallIntegerField(default=5)),
                ("opens_weekday", models.PositiveSmallIntegerField(default=0)),
                ("closes_weekday", models.PositiveSmallIntegerField(default=5)),
                ("opens_time", models.TimeField(default="08:00")),
                ("closes_time", models.TimeField(default="18:00")),
                ("campaign_start", models.DateTimeField(null=True, blank=True)),
                ("campaign_end", models.DateTimeField(null=True, blank=True)),
                ("chart_size", models.PositiveSmallIntegerField(default=20)),
                ("daily_limit", models.PositiveSmallIntegerField(default=5)),
                ("is_archived", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["station", "kind", "name", "id"],
            },
        ),
        migrations.CreateModel(
            name="StationVotingConfig",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("station", models.CharField(max_length=32, unique=True, choices=[
                    ("radio_zimbabwe", "Radio Zimbabwe"), ("national_fm", "National FM"),
                    ("power_fm", "Power FM"), ("classic_263", "Classic 263"),
                    ("central_radio", "Central Radio"), ("khulumani_fm", "Khulumani FM"),
                    ("luzibo_radio", "Luzibo Radio"),
                ])),
                ("require_codes", models.BooleanField(default=False)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.CreateModel(
            name="UnassignedVote",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("station", models.CharField(max_length=32, choices=[
                    ("radio_zimbabwe", "Radio Zimbabwe"), ("national_fm", "National FM"),
                    ("power_fm", "Power FM"), ("classic_263", "Classic 263"),
                    ("central_radio", "Central Radio"), ("khulumani_fm", "Khulumani FM"),
                    ("luzibo_radio", "Luzibo Radio"),
                ])),
                ("provider", models.CharField(max_length=16)),
                ("message_id", models.CharField(max_length=200)),
                ("sender", models.CharField(max_length=64)),
                ("text", models.CharField(max_length=2048)),
                ("reason", models.CharField(max_length=40)),
                ("state", models.CharField(max_length=16, default="pending")),
                ("received_at", models.DateTimeField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("resolved_show", models.ForeignKey(to="voting.chartshow", null=True, blank=True, on_delete=django.db.models.deletion.PROTECT, related_name="resolved_unassigned_votes")),
            ],
        ),
        migrations.AddField(model_name="rawvote", name="chart_show",
            field=models.ForeignKey(to="voting.chartshow", null=True, blank=True, on_delete=django.db.models.deletion.PROTECT, related_name="raw_votes", db_index=True)),
        migrations.AddField(model_name="rawvote", name="submitted_code",
            field=models.CharField(max_length=16, blank=True)),
        migrations.AddField(model_name="inboundevent", name="chart_show",
            field=models.ForeignKey(to="voting.chartshow", null=True, blank=True, on_delete=django.db.models.deletion.PROTECT, related_name="inbound_events")),
        migrations.AddField(model_name="inboundevent", name="vote_text",
            field=models.CharField(max_length=2048, blank=True)),
        migrations.AddField(model_name="inboundevent", name="submitted_code",
            field=models.CharField(max_length=16, blank=True)),
        migrations.AddField(model_name="weeklychart", name="chart_show",
            field=models.ForeignKey(to="voting.chartshow", null=True, blank=True, on_delete=django.db.models.deletion.PROTECT, related_name="published_charts")),
        migrations.AddConstraint(model_name="chartshow", constraint=models.UniqueConstraint(
            fields=("station", "code"), condition=~Q(code=""), name="unique_station_show_code")),
        migrations.AddConstraint(model_name="chartshow", constraint=models.UniqueConstraint(
            fields=("station",), condition=Q(kind="legacy"), name="unique_legacy_show_per_station")),
        migrations.AddConstraint(model_name="unassignedvote", constraint=models.UniqueConstraint(
            fields=("provider", "station", "message_id"), name="unique_unassigned_message")),
        migrations.AddIndex(model_name="chartshow", index=models.Index(
            fields=["station", "is_enabled"], name="av_show_enabled_idx")),
        migrations.AddIndex(model_name="unassignedvote", index=models.Index(
            fields=["station", "state", "-received_at"], name="av_unassigned_idx")),
        migrations.RunPython(preserve_legacy_data, migrations.RunPython.noop),
        migrations.AlterUniqueTogether(name="weeklychart", unique_together={
            ("chart_show", "year", "week_number", "is_year_end"),
        }),
    ]
