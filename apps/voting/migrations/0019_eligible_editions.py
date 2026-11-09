"""Introduce draft candidate lists; existing voting remains unchanged until opt-in."""
import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("voting", "0018_suspected_spam_vote"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name="chartshow", name="eligibility_enforced",
            field=models.BooleanField(default=False),
        ),
        migrations.CreateModel(
            name="EligibleEdition",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("station", models.CharField(max_length=32, db_index=True, choices=[
                    ("radio_zimbabwe", "Radio Zimbabwe"), ("national_fm", "National FM"),
                    ("power_fm", "Power FM"), ("classic_263", "Classic 263"),
                    ("central_radio", "Central Radio"), ("khulumani_fm", "Khulumani FM"),
                    ("luzibo_radio", "Luzibo Radio"),
                ])),
                ("chart_date", models.DateField()),
                ("voting_start", models.DateField()),
                ("voting_end", models.DateField()),
                ("status", models.CharField(max_length=16, default="draft", choices=[
                    ("draft", "Draft"), ("submitted", "Awaiting approval"),
                    ("returned", "Returned for revision"), ("locked", "Approved and locked"),
                ])),
                ("note", models.CharField(max_length=500, blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("chart_show", models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name="eligible_editions", to="voting.chartshow",
                )),
                ("prepared_by", models.ForeignKey(
                    null=True, on_delete=django.db.models.deletion.SET_NULL,
                    related_name="prepared_eligible_editions", to=settings.AUTH_USER_MODEL,
                )),
                ("approved_by", models.ForeignKey(
                    null=True, blank=True, on_delete=django.db.models.deletion.SET_NULL,
                    related_name="approved_eligible_editions", to=settings.AUTH_USER_MODEL,
                )),
                ("approved_at", models.DateTimeField(null=True, blank=True)),
            ],
            options={"ordering": ["-chart_date", "-id"]},
        ),
        migrations.CreateModel(
            name="EditionCandidate",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("edition", models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name="candidates", to="voting.eligibleedition",
                )),
                ("song", models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name="candidate_editions", to="voting.cleanedsong",
                )),
            ],
        ),
        migrations.CreateModel(
            name="CandidateSongSuggestion",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("artist", models.CharField(max_length=256)),
                ("title", models.CharField(max_length=256)),
                ("notes", models.CharField(max_length=500, blank=True)),
                ("status", models.CharField(max_length=16, default="pending", choices=[
                    ("pending", "Pending review"), ("accepted", "Accepted"),
                    ("rejected", "Rejected"),
                ])),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("reviewed_at", models.DateTimeField(null=True, blank=True)),
                ("edition", models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name="suggestions", to="voting.eligibleedition",
                )),
                ("suggested_by", models.ForeignKey(
                    null=True, on_delete=django.db.models.deletion.SET_NULL,
                    related_name="candidate_suggestions", to=settings.AUTH_USER_MODEL,
                )),
                ("matched_song", models.ForeignKey(
                    null=True, blank=True, on_delete=django.db.models.deletion.PROTECT,
                    related_name="resolved_candidate_suggestions", to="voting.cleanedsong",
                )),
                ("reviewed_by", models.ForeignKey(
                    null=True, blank=True, on_delete=django.db.models.deletion.SET_NULL,
                    related_name="reviewed_candidate_suggestions", to=settings.AUTH_USER_MODEL,
                )),
            ],
        ),
        migrations.AddConstraint(
            model_name="eligibleedition",
            constraint=models.UniqueConstraint(fields=("chart_show", "chart_date"), name="unique_eligible_chart_edition"),
        ),
        migrations.AddConstraint(
            model_name="editioncandidate",
            constraint=models.UniqueConstraint(fields=("edition", "song"), name="unique_edition_song"),
        ),
    ]
