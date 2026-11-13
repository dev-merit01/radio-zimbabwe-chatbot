"""Optional station-wide daily cap and repeat-song rules, preserving prior defaults."""
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("voting", "0029_matching_controls_diagnostics")]

    operations = [
        migrations.AddField(
            model_name="stationvotingconfig",
            name="station_daily_limit",
            field=models.PositiveSmallIntegerField(blank=True, default=None, null=True),
        ),
        migrations.AddField(
            model_name="stationvotingconfig",
            name="repeat_song_policy",
            field=models.CharField(
                choices=[
                    ("inherit", "System default"),
                    ("block", "Block across station"),
                    ("allow", "Allow repeat songs"),
                ],
                default="inherit",
                max_length=8,
            ),
        ),
    ]
