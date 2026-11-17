"""Add company ownership and company-wide licence records without touching votes."""
import django.db.models.deletion
from django.db import migrations, models
import django.core.validators


LEGACY_COMPANY_SLUG = "existing-airvote"


def group_existing_stations(apps, schema_editor):
    """Transition the existing installation into a single customer account."""
    BroadcastStation = apps.get_model("accounts", "BroadcastStation")
    Company = apps.get_model("licensing", "BroadcastCompany")
    CompanyStation = apps.get_model("licensing", "CompanyStation")
    if not BroadcastStation.objects.exists():
        return
    company, _ = Company.objects.get_or_create(
        slug=LEGACY_COMPANY_SLUG,
        defaults={"name": "Existing AirVote broadcaster (transitional)", "is_active": True},
    )
    for station_slug in BroadcastStation.objects.values_list("slug", flat=True):
        CompanyStation.objects.get_or_create(
            station_slug=station_slug, defaults={"company_id": company.pk}
        )


class Migration(migrations.Migration):
    dependencies = [
        ("licensing", "0001_initial"),
        ("accounts", "0005_broadcast_station"),
    ]
    operations = [
        migrations.CreateModel(
            name="BroadcastCompany",
            fields=[
                ("slug", models.CharField(max_length=48, primary_key=True, serialize=False,
                  validators=[django.core.validators.RegexValidator(
                      regex="^[a-z][a-z0-9_-]{2,47}$",
                      message="Use 3–48 lowercase letters, numbers, underscores or hyphens."
                  )])),
                ("name", models.CharField(max_length=160)),
                ("is_active", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"ordering": ["name", "slug"], "verbose_name_plural": "Broadcast companies"},
        ),
        migrations.CreateModel(
            name="CompanyStation",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("station_slug", models.CharField(max_length=32, unique=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("company", models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name="station_ownerships", to="licensing.broadcastcompany",
                )),
            ],
            options={"ordering": ["company_id", "station_slug"]},
        ),
        migrations.CreateModel(
            name="CompanyLicence",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("signed_token", models.TextField()),
                ("installed_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("revoked_at", models.DateTimeField(null=True, blank=True)),
                ("revocation_reason", models.CharField(blank=True, max_length=200)),
                ("company", models.OneToOneField(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name="commercial_licence", to="licensing.broadcastcompany",
                )),
            ],
            options={"verbose_name": "Company licence"},
        ),
        migrations.RunPython(group_existing_stations, migrations.RunPython.noop),
    ]
