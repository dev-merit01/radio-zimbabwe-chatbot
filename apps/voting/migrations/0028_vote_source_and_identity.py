"""Track the originating voting channel without changing historical totals."""
from django.db import migrations, models


def backfill_vote_sources(apps, schema_editor):
    RawVote = apps.get_model("voting", "RawVote")
    for channel, source in [
        ("whatsapp", "whatsapp"), ("telegram", "telegram"),
        ("web", "web"), ("sms", "sms"),
    ]:
        RawVote.objects.filter(user__channel=channel).update(source=source)


class Migration(migrations.Migration):
    dependencies = [("voting", "0027_record_intended_chart_on_held_votes")]

    operations = [
        migrations.AddField(
            model_name="rawvote", name="source",
            field=models.CharField(
                choices=[
                    ("web", "Online portal"), ("whatsapp", "WhatsApp"),
                    ("sms", "SMS"), ("telegram", "Telegram"),
                    ("manual", "Manual entry"),
                ],
                db_index=True, default="manual", max_length=16,
            ),
        ),
        migrations.AddField(
            model_name="rawvote", name="voter_fingerprint",
            field=models.CharField(
                blank=True, db_index=True, default="", max_length=64,
            ),
        ),
        migrations.AlterField(
            model_name="user", name="channel",
            field=models.CharField(
                choices=[
                    ("telegram", "Telegram"), ("whatsapp", "WhatsApp"),
                    ("web", "Online portal"), ("sms", "SMS"),
                    ("manual", "Manual entry"),
                ],
                max_length=16,
            ),
        ),
        migrations.RunPython(backfill_vote_sources, migrations.RunPython.noop),
    ]
