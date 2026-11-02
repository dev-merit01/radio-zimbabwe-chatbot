from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("voting", "0013_alter_weeklychart_unique_together_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="rawvote",
            name="ai_status",
            field=models.CharField(
                max_length=16, default="legacy", db_index=True,
                choices=[
                    ("legacy", "Legacy verified workflow"),
                    ("awaiting", "Awaiting AI"),
                    ("approved", "AI/reviewer approved"),
                    ("review", "Needs human review"),
                ],
            ),
        ),
    ]
