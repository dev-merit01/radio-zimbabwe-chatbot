from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0003_luzibo_radio"),
        ("voting", "0018_suspected_spam_vote"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="PresenterAssignment",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("user", models.OneToOneField(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name="presenter_assignment",
                    to=settings.AUTH_USER_MODEL,
                )),
                ("chart_show", models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name="presenters", to="voting.chartshow",
                )),
            ],
        ),
    ]
