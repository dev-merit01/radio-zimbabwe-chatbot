"""Short-lived verified-spelling cache without storing any listener identity."""
import django.db.models.deletion
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("voting", "0024_semantic_and_indexed_matching"),
    ]

    operations = [
        migrations.CreateModel(
            name="VerifiedVoteCache",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("station", models.CharField(max_length=32, db_index=True, choices=[
                    ("radio_zimbabwe", "Radio Zimbabwe"), ("national_fm", "National FM"),
                    ("power_fm", "Power FM"), ("classic_263", "Classic 263"),
                    ("central_radio", "Central Radio"), ("khulumani_fm", "Khulumani FM"),
                    ("luzibo_radio", "Luzibo Radio"),
                ])),
                ("normalised_text", models.CharField(max_length=512)),
                ("artist_snapshot", models.CharField(max_length=256)),
                ("title_snapshot", models.CharField(max_length=256)),
                ("verified_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("expires_at", models.DateTimeField(db_index=True)),
                ("source", models.CharField(max_length=12, default="openai")),
                ("song", models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name="verified_vote_cache",
                    to="voting.cleanedsong",
                )),
            ],
            options={
                "indexes": [
                    models.Index(fields=["station", "expires_at"], name="vote_cache_station_exp_idx"),
                ],
                "constraints": [
                    models.UniqueConstraint(fields=["station", "normalised_text"], name="unique_verified_vote_spelling"),
                ],
            },
        ),
    ]
