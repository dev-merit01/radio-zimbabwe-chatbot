from django.contrib.auth import get_user_model
from django.db.models.signals import post_save
from django.dispatch import receiver

from .models import AccountProfile, Station


User = get_user_model()


@receiver(post_save, sender=User)
def ensure_profile(sender, instance, created, **kwargs):
    if created:
        AccountProfile.objects.get_or_create(
            user=instance,
            defaults={'station': Station.RADIO_ZIMBABWE},
        )
