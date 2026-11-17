from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True
    dependencies = []

    operations = [
        migrations.CreateModel(
            name="StationLicence",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("station", models.CharField(db_index=True, max_length=32, unique=True)),
                ("signed_token", models.TextField()),
                ("installed_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("revoked_at", models.DateTimeField(blank=True, null=True)),
                ("revocation_reason", models.CharField(blank=True, max_length=200)),
            ],
            options={"ordering": ["station"], "verbose_name": "Station licence"},
        )
    ]
