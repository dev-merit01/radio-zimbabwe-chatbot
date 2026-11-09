import uuid
from django.conf import settings
from django.db import models


class Station(models.TextChoices):
    RADIO_ZIMBABWE = 'radio_zimbabwe', 'Radio Zimbabwe'
    NATIONAL_FM = 'national_fm', 'National FM'
    POWER_FM = 'power_fm', 'Power FM'
    CLASSIC_263 = 'classic_263', 'Classic 263'
    CENTRAL_RADIO = 'central_radio', 'Central Radio'
    KHULUMANI_FM = 'khulumani_fm', 'Khulumani FM'
    LUZIBO_RADIO = 'luzibo_radio', 'Luzibo Radio'




class BroadcastStation(models.Model):
    """Authoritative station directory for AirVote.

    The legacy Station enum is retained only as the fixed identifiers for the
    original seven stations. New stations live in this table. Never rename
    slug identifiers: every vote, account, show and audit references them.
    """

    slug = models.CharField(max_length=32, primary_key=True)
    name = models.CharField(max_length=100)
    is_active = models.BooleanField(default=False)
    bird_number = models.CharField(
        max_length=20, unique=True, null=True, blank=True,
        help_text="Inbound WhatsApp number, international digits without '+'.",
    )
    logo_url = models.URLField(max_length=500, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name", "slug"]

    def __str__(self):
        return self.name




class AccountProfile(models.Model):
    user = models.OneToOneField(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='profile')
    station = models.CharField(max_length=32, choices=Station.choices, default=Station.RADIO_ZIMBABWE)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Account Profile'
        verbose_name_plural = 'Account Profiles'

    def __str__(self) -> str:
        return f"{self.user.username} ({station_label(self.station)})"


class StationAccess(models.Model):
    """Station-wide entry password; only hashes and a revocation version persist."""
    station = models.CharField(max_length=32, choices=Station.choices, unique=True)
    password_hash = models.CharField(max_length=128)
    version = models.UUIDField(default=uuid.uuid4, editable=False)
    updated_at = models.DateTimeField(auto_now=True)


class PresenterAssignment(models.Model):
    """A presenter can access exactly one station's one chart show at a time.

    Administrators are not assigned; station-team accounts remain unassigned
    until the central administrator gives them a programme.
    """

    user = models.OneToOneField(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE,
        related_name="presenter_assignment",
    )
    chart_show = models.ForeignKey(
        "voting.ChartShow", on_delete=models.PROTECT,
        related_name="presenters",
    )
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.user.username}: {self.chart_show}"


def station_choices(*, active_only=False):
    """Station choices from the database for registration and switching."""
    qs = BroadcastStation.objects.all()
    if active_only:
        qs = qs.filter(is_active=True)
    return [(row.slug, row.name) for row in qs.order_by("name", "slug")]


def station_exists(slug, *, active_only=False):
    if not isinstance(slug, str) or not slug or len(slug) > 32:
        return False
    active = BroadcastStation.objects.filter(slug=slug).values_list(
        "is_active", flat=True
    ).first()
    if active is None:
        # The historical seven station IDs remain valid if an old database
        # has not been seeded yet, or in isolated transactional test resets.
        # A deliberately deactivated database row is NOT overridden.
        return slug in Station.values
    return bool(active) if active_only else True


def station_label(slug):
    return BroadcastStation.objects.filter(slug=slug).values_list(
        "name", flat=True
    ).first() or dict(Station.choices).get(slug, slug)
