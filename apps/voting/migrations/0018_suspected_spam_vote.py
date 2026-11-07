"""Record suspicious inbound submissions without creating chart votes."""
import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("voting", "0017_station_vote_multiplier"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="SuspectedSpamVote",
            fields=[
                ("id", models.BigAutoField(
                    auto_created=True, primary_key=True, serialize=False,
                    verbose_name="ID",
                )),
                ("station", models.CharField(max_length=32, choices=[
                    ("radio_zimbabwe", "Radio Zimbabwe"),
                    ("national_fm", "National FM"),
                    ("power_fm", "Power FM"),
                    ("classic_263", "Classic 263"),
                    ("central_radio", "Central Radio"),
                    ("khulumani_fm", "Khulumani FM"),
                    ("luzibo_radio", "Luzibo Radio"),
                ])),
                ("provider", models.CharField(max_length=16)),
                ("message_id", models.CharField(max_length=200)),
                ("sender", models.CharField(max_length=64)),
                ("text", models.CharField(max_length=2048)),
                ("vote_text", models.CharField(max_length=2048)),
                ("submitted_code", models.CharField(max_length=16, blank=True)),
                ("reason", models.CharField(max_length=32)),
                ("state", models.CharField(
                    max_length=16, default="pending", choices=[
                        ("pending", "Awaiting review"),
                        ("released", "Released by administrator"),
                        ("dismissed", "Dismissed by administrator"),
                    ],
                )),
                ("received_at", models.DateTimeField()),
                ("resolved_at", models.DateTimeField(null=True, blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("chart_show", models.ForeignKey(
                    null=True, blank=True,
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name="spam_reviews", to="voting.chartshow",
                )),
                ("resolved_by", models.ForeignKey(
                    null=True, blank=True,
                    on_delete=django.db.models.deletion.SET_NULL,
                    to=settings.AUTH_USER_MODEL,
                )),
            ],
        ),
        migrations.AddConstraint(
            model_name="suspectedspamvote",
            constraint=models.UniqueConstraint(
                fields=("provider", "station", "message_id"),
                name="unique_flagged_inbound_message",
            ),
        ),
        migrations.AddIndex(
            model_name="suspectedspamvote",
            index=models.Index(
                fields=["station", "sender", "-received_at"],
                name="av_spam_sender_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="suspectedspamvote",
            index=models.Index(
                fields=["station", "state", "-received_at"],
                name="av_spam_queue_idx",
            ),
        ),
    ]
