"""Freeze the official comparison edition and each song's movement status.

Existing archives keep their recorded previous_rank, votes and positions.
A blank movement_status denotes a legacy archive for which the API can
derive the most defensible status from earlier published charts.
"""
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("voting", "0020_zbc_official_chart_shows"),
    ]

    operations = [
        migrations.AddField(
            model_name="weeklychart",
            name="comparison_chart",
            field=models.ForeignKey(
                to="voting.weeklychart", null=True, blank=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="compared_by",
            ),
        ),
        migrations.AddField(
            model_name="weeklychartentry",
            name="movement_status",
            field=models.CharField(
                max_length=16, blank=True, default="",
                choices=[
                    ("up", "Climbing"),
                    ("down", "Falling"),
                    ("same", "Unchanged"),
                    ("new", "New entry"),
                    ("re_entry", "Re-entry"),
                    ("no_previous", "No comparison available"),
                ],
                help_text="Frozen position movement when this chart was published.",
            ),
        ),
    ]
