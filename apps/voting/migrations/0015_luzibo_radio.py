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
    dependencies = [("voting", "0014_rawvote_ai_status")]

    operations = [
        migrations.AlterField(model_name="user", name="station", field=models.CharField(choices=STATIONS, default="radio_zimbabwe", max_length=32)),
        migrations.AlterField(model_name="rawvote", name="station", field=models.CharField(choices=STATIONS, default="radio_zimbabwe", max_length=32, db_index=True)),
        migrations.AlterField(model_name="rawsongtally", name="station", field=models.CharField(choices=STATIONS, default="radio_zimbabwe", max_length=32, db_index=True)),
        migrations.AlterField(model_name="songcatalog", name="added_by_station", field=models.CharField(choices=STATIONS, default="radio_zimbabwe", max_length=32, help_text="Station that first added this song to the catalog")),
        migrations.AlterField(model_name="stationsong", name="station", field=models.CharField(choices=STATIONS, max_length=32, db_index=True)),
        migrations.AlterField(model_name="cleanedsong", name="station", field=models.CharField(choices=STATIONS, default="radio_zimbabwe", max_length=32, db_index=True)),
        migrations.AlterField(model_name="matchkeymapping", name="station", field=models.CharField(choices=STATIONS, default="radio_zimbabwe", max_length=32, db_index=True)),
        migrations.AlterField(model_name="cleanedsongtally", name="station", field=models.CharField(choices=STATIONS, default="radio_zimbabwe", max_length=32, db_index=True)),
        migrations.AlterField(model_name="llmdecisionlog", name="station", field=models.CharField(choices=STATIONS, default="radio_zimbabwe", max_length=32, db_index=True)),
        migrations.AlterField(model_name="weeklychart", name="station", field=models.CharField(choices=STATIONS, default="radio_zimbabwe", max_length=32, db_index=True)),
        migrations.AlterField(model_name="inboundevent", name="station", field=models.CharField(choices=STATIONS, max_length=32)),
        migrations.AlterField(model_name="stationstate", name="station", field=models.CharField(choices=STATIONS, max_length=32, unique=True)),
        migrations.AlterField(model_name="reviewaudit", name="station", field=models.CharField(choices=STATIONS, max_length=32, db_index=True)),
    ]
