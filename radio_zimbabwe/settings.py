import os
from pathlib import Path
import environ

env = environ.Env(
    DJANGO_DEBUG=(bool, True),
    DJANGO_SECRET_KEY=(str, 'changeme-secret'),
    DJANGO_ALLOWED_HOSTS=(str, 'localhost,127.0.0.1'),
    DJANGO_TIMEZONE=(str, 'Africa/Harare'),
    DATABASE_URL=(str, 'postgres://postgres:postgres@localhost:5432/radio_bot'),
    REDIS_URL=(str, 'redis://localhost:6379/0'),
    CHANNEL=(str, 'telegram'),
    TELEGRAM_BOT_TOKEN=(str, ''),
    WHATSAPP_BSP=(str, 'bird'),  # Options: 'bird', 'onemsg', 'twilio'
    SPOTIFY_CLIENT_ID=(str, ''),
    SPOTIFY_CLIENT_SECRET=(str, ''),
    GEMINI_API_KEY=(str, ''),
    COHERE_API_KEY=(str, ''),
    ANTHROPIC_API_KEY=(str, ''),
    # OneMsg.io WhatsApp API credentials (legacy)
    ONEMSG_APP_KEY=(str, ''),
    ONEMSG_AUTH_KEY=(str, ''),
    # Bird.com WhatsApp API credentials
    BIRD_ACCESS_KEY=(str, ''),
    BIRD_WORKSPACE_ID=(str, ''),
    BIRD_CHANNEL_ID=(str, ''),
)

BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env if present
ENV_FILE = BASE_DIR / '.env'
if ENV_FILE.exists():
    environ.Env.read_env(str(ENV_FILE))

SECRET_KEY = env('DJANGO_SECRET_KEY')
DEBUG = env('DJANGO_DEBUG')
ALLOWED_HOSTS = env('DJANGO_ALLOWED_HOSTS').split(',') + ['testserver','onrender.com','radio-zimbabwe-chatbot.onrender.com','testserver','127.0.0.1','localhost']

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'apps.bot',
    'apps.voting',
    'apps.charts',
    'apps.spotify',
    'apps.dashboard',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'radio_zimbabwe.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'radio_zimbabwe.wsgi.application'

DATABASES = {
    'default': env.db(),
}

AUTH_PASSWORD_VALIDATORS = []

LANGUAGE_CODE = 'en-us'
TIME_ZONE = env('DJANGO_TIMEZONE')
USE_I18N = True
USE_TZ = True

STATIC_URL = 'static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
STATICFILES_DIRS = [BASE_DIR / 'static']

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# Celery
CELERY_BROKER_URL = env('REDIS_URL')
CELERY_RESULT_BACKEND = env('REDIS_URL')

# Channel selection
CHANNEL = env('CHANNEL')
TELEGRAM_BOT_TOKEN = env('TELEGRAM_BOT_TOKEN')
WHATSAPP_BSP = env('WHATSAPP_BSP')
SPOTIFY_CLIENT_ID = env('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = env('SPOTIFY_CLIENT_SECRET')

LOG_LEVEL = 'DEBUG' if DEBUG else 'INFO'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '[{asctime}] {levelname} {name}: {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
    },
    'loggers': {
        'apps': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'django': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
    },
}

# Gemini API
GEMINI_API_KEY = env('GEMINI_API_KEY')

# Cohere API (for LLM vote matching - legacy)
COHERE_API_KEY = env('COHERE_API_KEY')

# Anthropic API (for LLM vote matching)
ANTHROPIC_API_KEY = env('ANTHROPIC_API_KEY')

# OneMsg.io WhatsApp API (legacy)
ONEMSG_APP_KEY = env('ONEMSG_APP_KEY')
ONEMSG_AUTH_KEY = env('ONEMSG_AUTH_KEY')

# Bird.com WhatsApp API
BIRD_ACCESS_KEY = env('BIRD_ACCESS_KEY')
BIRD_WORKSPACE_ID = env('BIRD_WORKSPACE_ID')
BIRD_CHANNEL_ID = env('BIRD_CHANNEL_ID')

# WhatsApp BSP (Business Solution Provider)
# Options: 'bird', 'onemsg', 'twilio'
WHATSAPP_BSP = env('WHATSAPP_BSP')

# Increase field limit for bulk delete operations in admin
DATA_UPLOAD_MAX_NUMBER_FIELDS = 100000
