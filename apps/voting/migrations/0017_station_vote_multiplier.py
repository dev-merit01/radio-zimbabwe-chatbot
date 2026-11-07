"""Keep every historical recorded vote and published score unchanged."""
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("voting", "0016_chart_shows")]

    operations = [
        migrations.AddField(
            model_name="stationvotingconfig", name="vote_multiplier",
            field=models.PositiveSmallIntegerField(default=1),
        ),
        migrations.AddField(
            model_name="weeklychart", name="vote_multiplier",
            field=models.PositiveSmallIntegerField(default=1),
        ),
    ]
