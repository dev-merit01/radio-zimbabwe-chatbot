"""Curated, auditable station-scoped aliases for intelligent vote matching."""
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("voting", "0022_presenter_published_eligible_editions"),
    ]

    operations = [
        migrations.CreateModel(
            name="SongAlias",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("station", models.CharField(max_length=32, db_index=True, choices=[
                    ("radio_zimbabwe", "Radio Zimbabwe"), ("national_fm", "National FM"),
                    ("power_fm", "Power FM"), ("classic_263", "Classic 263"),
                    ("central_radio", "Central Radio"), ("khulumani_fm", "Khulumani FM"),
                    ("luzibo_radio", "Luzibo Radio"),
                ])),
                ("kind", models.CharField(max_length=12, choices=[
                    ("full", "Full listener expression"),
                    ("artist", "Artist/stage-name alias"),
                    ("title", "Song title/short title"),
                ])),
                ("value", models.CharField(max_length=512)),
                ("normalised", models.CharField(max_length=512, db_index=True)),
                ("is_approved", models.BooleanField(default=False, db_index=True)),
                ("source", models.CharField(max_length=24, default="reviewer")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("song", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="aliases", to="voting.cleanedsong")),
            ],
            options={
                "unique_together": {("station", "kind", "normalised", "song")},
                "indexes": [
                    models.Index(fields=["station", "kind", "normalised"], name="voting_songa_station_512778_idx"),
                    models.Index(fields=["station", "song", "is_approved"], name="voting_songa_station_76d1d8_idx"),
                ],
            },
        ),
    ]
