from django.db import models


class StationLicence(models.Model):
    """Signed vendor-issued licence for one station. Tokens are never generated here."""

    station = models.CharField(max_length=32, unique=True, db_index=True)
    signed_token = models.TextField()
    installed_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    revoked_at = models.DateTimeField(null=True, blank=True)
    revocation_reason = models.CharField(max_length=200, blank=True)

    class Meta:
        ordering = ["station"]
        verbose_name = "Station licence"

    def __str__(self):
        return self.station
