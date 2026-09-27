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


class AccountProfile(models.Model):
    user = models.OneToOneField(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='profile')
    station = models.CharField(max_length=32, choices=Station.choices, default=Station.RADIO_ZIMBABWE)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Account Profile'
        verbose_name_plural = 'Account Profiles'

    def __str__(self) -> str:
        return f"{self.user.username} ({self.get_station_display()})"


class StationAccess(models.Model):
    """Station-wide entry password; only hashes and a revocation version persist."""
    station = models.CharField(max_length=32, choices=Station.choices, unique=True)
    password_hash = models.CharField(max_length=128)
    version = models.UUIDField(default=uuid.uuid4, editable=False)
    updated_at = models.DateTimeField(auto_now=True)
