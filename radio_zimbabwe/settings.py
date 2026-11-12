from pathlib import Path
import environ

env = environ.Env(
    DJANGO_DEBUG=(bool, False),
    DJANGO_SECRET_KEY=(str, ""),
    DJANGO_ALLOWED_HOSTS=(str, "localhost,127.0.0.1"),
    DJANGO_TIMEZONE=(str, "Africa/Harare"),
    DATABASE_URL=(str, "sqlite:///db.sqlite3"),
    REDIS_URL=(str, "redis://localhost:6379/0"),
    CHANNEL=(str, "telegram"),
    TELEGRAM_BOT_TOKEN=(str, ""),
    WHATSAPP_BSP=(str, "bird"),  # Options: 'bird', 'onemsg', 'twilio'
    SPOTIFY_CLIENT_ID=(str, ""),
    SPOTIFY_CLIENT_SECRET=(str, ""),
    GEMINI_API_KEY=(str, ""),
    COHERE_API_KEY=(str, ""),
    ANTHROPIC_API_KEY=(str, ""),
    OPENAI_API_KEY=(str, ""),
    # OneMsg.io WhatsApp API credentials (legacy)
    ONEMSG_APP_KEY=(str, ""),
    ONEMSG_AUTH_KEY=(str, ""),
    # Bird.com WhatsApp API credentials
    BIRD_ACCESS_KEY=(str, ""),
    BIRD_WORKSPACE_ID=(str, ""),
    BIRD_CHANNEL_ID=(str, ""),
)

BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env if present
ENV_FILE = BASE_DIR / ".env"
if ENV_FILE.exists():
    environ.Env.read_env(str(ENV_FILE))

SECRET_KEY = env("DJANGO_SECRET_KEY")
if not SECRET_KEY:
    from django.core.exceptions import ImproperlyConfigured

    raise ImproperlyConfigured("Set DJANGO_SECRET_KEY to a unique random value.")
DEBUG = env("DJANGO_DEBUG")
ALLOWED_HOSTS = [h.strip() for h in env("DJANGO_ALLOWED_HOSTS").split(",") if h.strip()]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.postgres",
    "rest_framework",
    "apps.bot",
    "apps.voting",
    "apps.charts",
    "apps.spotify",
    "apps.dashboard",
    "apps.accounts",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "radio_zimbabwe.middleware.LocalWebhookHostMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "radio_zimbabwe.middleware.WorkspaceSecurityMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "radio_zimbabwe.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "apps.accounts.context_processors.station_branding",
            ],
        },
    },
]

WSGI_APPLICATION = "radio_zimbabwe.wsgi.application"

DATABASES = {
    "default": env.db(),
}

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"
    },
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = env("DJANGO_TIMEZONE")
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_DIRS = [BASE_DIR / "static"]

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LOGIN_URL = "/accounts/login/"
LOGIN_REDIRECT_URL = "/"
LOGOUT_REDIRECT_URL = "/accounts/login/"

# Celery
CELERY_BROKER_URL = env("REDIS_URL")
CELERY_RESULT_BACKEND = env("REDIS_URL")

# Channel selection
CHANNEL = env("CHANNEL")
TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN")
WHATSAPP_BSP = env("WHATSAPP_BSP")
SPOTIFY_CLIENT_ID = env("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = env("SPOTIFY_CLIENT_SECRET")

LOG_LEVEL = env.str("DJANGO_LOG_LEVEL", default="DEBUG" if DEBUG else "INFO")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "[{asctime}] {levelname} {name}: {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },
    },
    "loggers": {
        "apps": {
            "handlers": ["console"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
        "django": {
            "handlers": ["console"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
    },
}

# Gemini API
GEMINI_API_KEY = env("GEMINI_API_KEY")

# Cohere API (for LLM vote matching - legacy)
COHERE_API_KEY = env("COHERE_API_KEY")

# Anthropic API (for LLM vote matching - legacy)
ANTHROPIC_API_KEY = env("ANTHROPIC_API_KEY")

# OpenAI API (for LLM vote matching)
OPENAI_API_KEY = env("OPENAI_API_KEY")

# OneMsg.io WhatsApp API (legacy)
ONEMSG_APP_KEY = env("ONEMSG_APP_KEY")
ONEMSG_AUTH_KEY = env("ONEMSG_AUTH_KEY")

# Bird.com WhatsApp API
BIRD_ACCESS_KEY = env("BIRD_ACCESS_KEY")
BIRD_WORKSPACE_ID = env("BIRD_WORKSPACE_ID")
BIRD_CHANNEL_ID = env("BIRD_CHANNEL_ID")

# WhatsApp BSP (Business Solution Provider)
# Options: 'bird', 'onemsg', 'twilio'
WHATSAPP_BSP = env("WHATSAPP_BSP")

# Increase field limit for bulk delete operations in admin
DATA_UPLOAD_MAX_NUMBER_FIELDS = 100000

# Version 2: public receipt endpoints, private staff operations.
APP_VERSION = "2.3.0"
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = not DEBUG
CSRF_COOKIE_SECURE = not DEBUG
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_EXPIRE_AT_BROWSER_CLOSE = True
SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_SSL_REDIRECT = env.bool("SECURE_SSL_REDIRECT", default=not DEBUG)
CSRF_TRUSTED_ORIGINS = env.list("CSRF_TRUSTED_ORIGINS", default=[])
# Set only behind a trusted proxy that strips client-supplied forwarding headers.
if env.bool("TRUST_PROXY", default=False):
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
DATA_UPLOAD_MAX_MEMORY_SIZE = 262144
ALLOW_REGISTRATION = True
VOTING_DAILY_LIMIT = env.int("VOTING_DAILY_LIMIT", default=5)
ALLOW_REPEAT_SONG = env.bool("ALLOW_REPEAT_SONG", default=False)
AUTO_AI_MATCH = env.bool("AUTO_AI_MATCH", default=False)
# Opt-in production mode: every new valid vote must be AI checked before counting.
AI_ALL_VOTES = env.bool("AI_ALL_VOTES", default=False)

# Public voting is deliberately off until verification and bot-protection
# credentials have been configured. Staff/API/WhatsApp channels are unaffected.
AIRVOTE_PUBLIC_VOTING_ENABLED = env.bool(
    "AIRVOTE_PUBLIC_VOTING_ENABLED", default=False,
)
AIRVOTE_TURNSTILE_SITE_KEY = env.str("AIRVOTE_TURNSTILE_SITE_KEY", default="")
AIRVOTE_TURNSTILE_SECRET_KEY = env.str("AIRVOTE_TURNSTILE_SECRET_KEY", default="")
AIRVOTE_VERIFY_API_KEY = env.str("AIRVOTE_VERIFY_API_KEY", default="")
AIRVOTE_BIRD_VERIFY_REGION = env.str("AIRVOTE_BIRD_VERIFY_REGION", default="us1")
# Never accept arbitrary provider base URLs from the browser or environment.
# Explicit opt-in: reuse a recently OpenAI-confirmed listener spelling only
# after station/canonical song/exhaustive-ambiguity checks. No listener IDs
# are cached and all actual votes still flow through anti-spam/limit rules.
AI_MATCH_CACHE_ENABLED = env.bool("AI_MATCH_CACHE_ENABLED", default=False)
AI_MATCH_CACHE_SECONDS = env.int("AI_MATCH_CACHE_SECONDS", default=600)
OPENAI_MODEL = env.str("OPENAI_MODEL", default="gpt-6-luna")
OPENAI_REASONING_EFFORT = env.str("OPENAI_REASONING_EFFORT", default="none")

# Layered retrieval: indexed lexical search is cheap and on by default.
# Semantic embedding API calls are explicitly opt-in because they incur cost.
AI_INDEXED_SEARCH_ENABLED = env.bool("AI_INDEXED_SEARCH_ENABLED", default=True)
AI_VECTOR_ENABLED = env.bool("AI_VECTOR_ENABLED", default=False)
AI_VECTOR_AUTO_REFRESH = env.bool("AI_VECTOR_AUTO_REFRESH", default=False)
AI_VECTOR_RETRIEVAL_MIN_SIMILARITY = env.float(
    "AI_VECTOR_RETRIEVAL_MIN_SIMILARITY", default=0.64
)
OPENAI_EMBEDDING_MODEL = env.str(
    "OPENAI_EMBEDDING_MODEL", default="text-embedding-3-small"
)
AI_CALIBRATED_MATCHING_ENABLED = env.bool(
    "AI_CALIBRATED_MATCHING_ENABLED", default=True
)
# Each provider account is bound to one station; never trust an inbound station value.
TELEGRAM_STATION = env.str("TELEGRAM_STATION", default="radio_zimbabwe")
BIRD_STATION = env.str("BIRD_STATION", default="radio_zimbabwe")
# Route signed Bird WhatsApp messages by receiving phone number when configured.
BIRD_NUMBER_STATION_MAP = env.json("BIRD_NUMBER_STATION_MAP", default={})
# When enabled on both Railway services, no Bird WhatsApp replies are sent.
BIRD_SILENT_VOTING = env.bool("BIRD_SILENT_VOTING", default=False)
ONEMSG_STATION = env.str("ONEMSG_STATION", default="radio_zimbabwe")
TELEGRAM_WEBHOOK_SECRET = env.str("TELEGRAM_WEBHOOK_SECRET", default="")
BIRD_WEBHOOK_SECRET = env.str("BIRD_WEBHOOK_SECRET", default="")
ONEMSG_WEBHOOK_SECRET = env.str("ONEMSG_WEBHOOK_SECRET", default="")

if not DEBUG:
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.redis.RedisCache",
            "LOCATION": env("REDIS_URL"),
        }
    }

SECURE_HSTS_SECONDS = env.int("SECURE_HSTS_SECONDS", default=3600 if not DEBUG else 0)
SECURE_REFERRER_POLICY = "same-origin"
SESSION_COOKIE_AGE = 28800

# Connected local mode exposes only receipt routes through these explicit tunnel hosts.
LOCAL_WEBHOOK_HOSTS = env.list("LOCAL_WEBHOOK_HOSTS", default=[])

# Railway probes over its internal HTTP network; this endpoint exposes no data.
SECURE_REDIRECT_EXEMPT = [r"^healthz/$"]
