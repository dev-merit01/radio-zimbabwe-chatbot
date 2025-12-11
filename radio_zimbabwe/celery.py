import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'radio_zimbabwe.settings')

app = Celery('radio_zimbabwe')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
