"""Public web intake disabled by default; station admin opts in explicitly."""
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("voting", "0030_station_voting_policy")]

    operations = [
        migrations.AddField(
            model_name="stationvotingconfig",
            name="public_web_enabled",
            field=models.BooleanField(default=False),
        ),
    ]
