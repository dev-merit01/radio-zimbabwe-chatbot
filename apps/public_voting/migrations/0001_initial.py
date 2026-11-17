# Generated for AirVote public-vote gateway; no changes to historical votes.
import uuid

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True
    dependencies = [("voting", "0030_station_voting_policy")]
    operations = [
        migrations.CreateModel(
            name="WebVoteReceipt",
            fields=[
                ("vote_id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("station", models.CharField(db_index=True, max_length=32)),
                ("state", models.CharField(choices=[("admitted", "Admitted to verification queue"), ("rejected", "Rejected")], max_length=12)),
                ("reason", models.CharField(blank=True, max_length=64)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("chart_show", models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to="voting.chartshow")),
                ("song", models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to="voting.cleanedsong")),
            ],
            options={"indexes": [models.Index(fields=["station", "-created_at"], name="public_voti_station_6aa2d4_idx")]},
        ),
    ]
