"""Retire administrator-review statuses without changing candidate selections."""
from django.db import migrations, models


def reopen_legacy_review_rows(apps, schema_editor):
    EligibleEdition = apps.get_model("voting", "EligibleEdition")
    EligibleEdition.objects.filter(
        status__in=("submitted", "returned")
    ).update(status="draft", note="")


class Migration(migrations.Migration):
    dependencies = [
        ("voting", "0021_chart_position_movement"),
    ]

    operations = [
        migrations.RunPython(reopen_legacy_review_rows, migrations.RunPython.noop),
        migrations.AlterField(
            model_name="eligibleedition",
            name="status",
            field=models.CharField(
                max_length=16, default="draft",
                choices=[("draft", "Draft"), ("locked", "Published")],
            ),
        ),
    ]
