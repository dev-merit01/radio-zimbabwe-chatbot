from django.db import migrations, models


STATIONS = [
    ("radio_zimbabwe", "Radio Zimbabwe"),
    ("national_fm", "National FM"),
    ("power_fm", "Power FM"),
    ("classic_263", "Classic 263"),
    ("central_radio", "Central Radio"),
    ("khulumani_fm", "Khulumani FM"),
    ("luzibo_radio", "Luzibo Radio"),
]


class Migration(migrations.Migration):
    dependencies = [("accounts", "0002_stationaccess")]

    operations = [
        migrations.AlterField(
            model_name="accountprofile", name="station",
            field=models.CharField(
                choices=STATIONS, default="radio_zimbabwe", max_length=32,
            ),
        ),
        migrations.AlterField(
            model_name="stationaccess", name="station",
            field=models.CharField(choices=STATIONS, max_length=32, unique=True),
        ),
    ]
