from django.urls import path
from .views import telegram_webhook, whatsapp_webhook, bird_webhook

urlpatterns = [
    path('telegram/', telegram_webhook, name='telegram_webhook'),
    path('whatsapp/', whatsapp_webhook, name='whatsapp_webhook'),
    path('bird/', bird_webhook, name='bird_webhook'),
]
