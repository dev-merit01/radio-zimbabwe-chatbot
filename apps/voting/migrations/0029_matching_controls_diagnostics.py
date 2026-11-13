"""Station-specific matching controls and structured vote diagnostics."""
import uuid
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [("voting", "0028_vote_source_and_identity")]

    operations = [
        migrations.CreateModel(
            name="MatchingControl",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("station", models.CharField(max_length=32, unique=True)),
                ("model_override", models.CharField(blank=True, default="", max_length=80)),
                ("shortlist_limit", models.PositiveSmallIntegerField(default=16)),
                ("retrieval_min_artist", models.FloatField(default=0.68)),
                ("retrieval_min_title", models.FloatField(default=0.66)),
                ("cache_mode", models.CharField(choices=[("inherit", "System default"), ("on", "Enabled"), ("off", "Disabled")], default="inherit", max_length=8)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.CreateModel(
            name="VoteMatchingDiagnostic",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("station", models.CharField(db_index=True, max_length=32)),
                ("trace_id", models.UUIDField(default=uuid.uuid4, editable=False, unique=True)),
                ("verification_method", models.CharField(default="unknown", max_length=20)),
                ("model_used", models.CharField(blank=True, max_length=80)),
                ("api_status", models.CharField(default="not_called", max_length=16)),
                ("api_latency_ms", models.PositiveIntegerField(blank=True, null=True)),
                ("queue_wait_ms", models.PositiveIntegerField(blank=True, null=True)),
                ("candidate_count", models.PositiveSmallIntegerField(default=0)),
                ("fuzzy_score", models.FloatField(blank=True, null=True)),
                ("artist_score", models.FloatField(blank=True, null=True)),
                ("title_score", models.FloatField(blank=True, null=True)),
                ("vector_score", models.FloatField(blank=True, null=True)),
                ("decision", models.CharField(default="retry", max_length=16)),
                ("decision_reason", models.CharField(blank=True, max_length=64)),
                ("attempts", models.PositiveSmallIntegerField(default=1)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("matched_song", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to="voting.cleanedsong")),
                ("vote", models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name="matching_diagnostic", to="voting.rawvote")),
            ],
            options={"indexes": [models.Index(fields=["station", "-created_at"], name="vote_match_diag_station_idx")]},
        ),
    ]
