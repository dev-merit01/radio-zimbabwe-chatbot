"""Durable idempotency ledger for votes admitted from the isolated public gateway.

No Google tokens, names, emails or raw phone numbers are stored here.
The main RawVote table retains the canonical, pseudonymous web vote.
"""
import uuid

from django.db import models


class WebVoteReceipt(models.Model):
    vote_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    station = models.CharField(max_length=32, db_index=True)
    chart_show = models.ForeignKey("voting.ChartShow", null=True, on_delete=models.PROTECT)
    song = models.ForeignKey("voting.CleanedSong", null=True, on_delete=models.PROTECT)
    state = models.CharField(max_length=12, choices=[
        ("admitted", "Admitted to verification queue"),
        ("rejected", "Rejected"),
    ])
    reason = models.CharField(max_length=64, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [models.Index(fields=["station", "-created_at"], name="public_voti_station_6aa2d4_idx")]
