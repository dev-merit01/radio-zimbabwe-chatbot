"""Keep a record of the intended chart programme for held coded votes.

This is a metadata-only migration: no voting window is changed, no pending
record is assigned and no chart tally is incremented. Existing closed-show
submissions can now be displayed under the programme listeners requested.
"""
import re

from django.db import migrations, models
import django.db.models.deletion


CODE_VOTE = re.compile(r"^([A-Za-z0-9]{2,16})(?:\s*[:|]\s*|\s+)(.+)$", re.S)


def backfill_intended_shows(apps, schema_editor):
    UnassignedVote = apps.get_model("voting", "UnassignedVote")
    ChartShow = apps.get_model("voting", "ChartShow")
    for vote in UnassignedVote.objects.filter(
        state="pending", reason="show_closed",
    ).iterator(chunk_size=200):
        match = CODE_VOTE.match((vote.text or "").strip())
        if not match:
            continue
        code = match.group(1).upper()
        show = ChartShow.objects.filter(
            station=vote.station, code=code,
        ).first()
        if show is None or show.kind == "legacy":
            continue
        vote.intended_show_id = show.pk
        vote.submitted_code = code
        vote.vote_text = match.group(2).strip()[:2048]
        vote.save(update_fields=["intended_show", "submitted_code", "vote_text"])


class Migration(migrations.Migration):
    dependencies = [
        ("voting", "0026_require_codes_on_existing_chart_stations"),
    ]

    operations = [
        migrations.AddField(
            model_name="unassignedvote",
            name="intended_show",
            field=models.ForeignKey(
                blank=True, null=True, on_delete=django.db.models.deletion.PROTECT,
                related_name="held_routing_votes", to="voting.chartshow",
            ),
        ),
        migrations.AddField(
            model_name="unassignedvote",
            name="submitted_code",
            field=models.CharField(blank=True, max_length=16),
        ),
        migrations.AddField(
            model_name="unassignedvote",
            name="vote_text",
            field=models.CharField(blank=True, max_length=2048),
        ),
        migrations.RunPython(backfill_intended_shows, migrations.RunPython.noop),
    ]
