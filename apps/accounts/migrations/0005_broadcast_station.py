"""Register the existing seven stations without changing any votes/accounts."""
from django.db import migrations, models


BUILT_IN = (
    ("radio_zimbabwe", "Radio Zimbabwe"),
    ("national_fm", "National FM"),
    ("power_fm", "Power FM"),
    ("classic_263", "Classic 263"),
    ("central_radio", "Central Radio"),
    ("khulumani_fm", "Khulumani FM"),
    ("luzibo_radio", "Luzibo Radio"),
)


def seed_stations(apps, schema_editor):
    Station = apps.get_model("accounts", "BroadcastStation")
    for slug, name in BUILT_IN:
        Station.objects.get_or_create(
            slug=slug, defaults={"name": name, "is_active": True},
        )


class Migration(migrations.Migration):
    dependencies = [("accounts", "0004_presenter_assignment")]

    operations = [
        migrations.CreateModel(
            name="BroadcastStation",
            fields=[
                ("slug", models.CharField(max_length=32, primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=100)),
                ("is_active", models.BooleanField(default=False)),
                ("bird_number", models.CharField(max_length=20, unique=True, null=True, blank=True, help_text="Inbound WhatsApp number, international digits without '+'.")),
                ("logo_url", models.URLField(max_length=500, blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"ordering": ["name", "slug"]},
        ),
        migrations.RunPython(seed_stations, migrations.RunPython.noop),
    ]
