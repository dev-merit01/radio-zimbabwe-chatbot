from django.db import migrations


def enable_director_feed(apps, schema_editor):
    grants = apps.get_model("accounts", "StaffAccess").objects.using(schema_editor.connection.alias)
    for grant in grants.filter(role="director").iterator():
        if "incoming" not in grant.privileges:
            grant.privileges = [*grant.privileges, "incoming"]
            grant.save(update_fields=["privileges"])


class Migration(migrations.Migration):
    dependencies = [("accounts", "0007_configurable_programmes")]
    operations = [migrations.RunPython(enable_director_feed, migrations.RunPython.noop)]
