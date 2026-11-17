"""Commercial AirVote licensing: one licence per broadcasting company."""
from django.core.validators import RegexValidator
from django.db import models


company_slug_validator = RegexValidator(
    regex=r"^[a-z][a-z0-9_-]{2,47}$",
    message="Use 3–48 lowercase letters, numbers, underscores or hyphens.",
)


class BroadcastCompany(models.Model):
    """One legal broadcasting customer, potentially owning many stations."""

    slug = models.CharField(max_length=48, primary_key=True, validators=[company_slug_validator])
    name = models.CharField(max_length=160)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name", "slug"]
        verbose_name_plural = "Broadcast companies"

    def __str__(self):
        return self.name


class CompanyStation(models.Model):
    """Explicit station ownership. A station belongs to exactly one company."""

    station_slug = models.CharField(max_length=32, unique=True)
    company = models.ForeignKey(
        BroadcastCompany, on_delete=models.PROTECT, related_name="station_ownerships"
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["company_id", "station_slug"]

    def __str__(self):
        return f"{self.station_slug} → {self.company_id}"

    def clean(self):
        from django.core.exceptions import ValidationError
        from apps.accounts.models import BroadcastStation

        if self.station_slug and not BroadcastStation.objects.filter(slug=self.station_slug).exists():
            raise ValidationError({"station_slug": "Create this station before assigning it."})


class CompanyLicence(models.Model):
    """One vendor-signed licence per company. Never holds a private signing key."""

    company = models.OneToOneField(
        BroadcastCompany, on_delete=models.PROTECT, related_name="commercial_licence"
    )
    signed_token = models.TextField()
    installed_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    revoked_at = models.DateTimeField(null=True, blank=True)
    revocation_reason = models.CharField(max_length=200, blank=True)

    class Meta:
        verbose_name = "Company licence"

    def __str__(self):
        return f"{self.company.name} company licence"


class StationLicence(models.Model):
    """Legacy station grant retained for migration and audit only.

    AV1 station grants are not used when company licensing is enabled. AirVote
    will not issue new station-specific grants after the AV2 migration.
    """

    station = models.CharField(max_length=32, unique=True, db_index=True)
    signed_token = models.TextField()
    installed_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    revoked_at = models.DateTimeField(null=True, blank=True)
    revocation_reason = models.CharField(max_length=200, blank=True)

    class Meta:
        ordering = ["station"]
        verbose_name = "Legacy station licence"

    def __str__(self):
        return self.station
