"""Public AirVote voting portal.

Never expose staff APIs or accept caller-supplied station, artist or vote text.
Only published, verified song IDs in an OPEN programme are eligible. Public
submissions are queued into the existing AI verification worker (fail closed).
"""
import hashlib
import hmac
import logging
import re
import secrets
import smtplib
import time
import uuid

import requests
from django.conf import settings
from django.core.cache import cache
from django.core.mail import send_mail
from django.db import transaction
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.utils.crypto import salted_hmac
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST

from apps.accounts.models import BroadcastStation
from apps.voting.channels import normalized_email, email_fingerprint
from apps.voting.models import (
    ChartShow, CleanedSong, EditionCandidate, EligibleEdition, InboundEvent,
)

logger = logging.getLogger(__name__)

VERIFY_EMAIL_SESSION = "airvote_web_pending_email"
VERIFIED_ID_SESSION = "airvote_web_email_verified_voter"
VERIFIED_UNTIL_SESSION = "airvote_web_email_verified_until"
OTP_EXPIRES_SESSION = "airvote_web_email_pending_expires"
OTP_DIGEST_SESSION = "airvote_web_email_code_digest"
OTP_NONCE_SESSION = "airvote_web_email_code_nonce"
OTP_ATTEMPTS_SESSION = "airvote_web_email_code_attempts"


def _clear_pending(request):
    for key in (
        VERIFY_EMAIL_SESSION, OTP_EXPIRES_SESSION, OTP_DIGEST_SESSION,
        OTP_NONCE_SESSION, OTP_ATTEMPTS_SESSION,
    ):
        request.session.pop(key, None)


def _code_digest(email, code, nonce):
    return salted_hmac("airvote-portal-email-otp-v1",
                       f"{nonce}:{email}:{code}").hexdigest()


def portal_enabled():
    """Fail closed until a verified outbound email service is configured."""
    return (
        settings.AIRVOTE_PUBLIC_VOTING_ENABLED
        and settings.AI_ALL_VOTES
        and bool(settings.AIRVOTE_TURNSTILE_SITE_KEY)
        and bool(settings.AIRVOTE_TURNSTILE_SECRET_KEY)
        and bool(settings.EMAIL_HOST)
        and bool(settings.EMAIL_HOST_USER)
        and bool(settings.EMAIL_HOST_PASSWORD)
        and bool(settings.DEFAULT_FROM_EMAIL)
        and (settings.EMAIL_USE_TLS or settings.EMAIL_USE_SSL)
        and not (settings.EMAIL_USE_TLS and settings.EMAIL_USE_SSL)
    )


def _json_error(text, status=400):
    return JsonResponse({"ok": False, "error": text}, status=status)


def _rate(key, limit, seconds):
    """Bound public requests using a shared, atomic Redis/Django cache.

    Production requests fail closed when Redis is unavailable. Keys never
    contain raw phone numbers or IP addresses.
    """
    digest = hashlib.sha256(str(key).encode("utf-8")).hexdigest()
    cache_key = "airvote:portal:rate:" + digest
    try:
        if cache.add(cache_key, 1, timeout=seconds):
            return True
        return cache.incr(cache_key) <= limit
    except Exception:
        logger.warning("Online vote rate limiter unavailable")
        return False


def _browser_key(request):
    # Never trust arbitrary client-supplied X-Forwarded-For headers.
    return request.META.get("REMOTE_ADDR", "unknown")[:100]


def _turnstile(request, action):
    token = request.POST.get("cf-turnstile-response", "")
    if not isinstance(token, str) or not (16 <= len(token) <= 2048):
        return False
    try:
        answer = requests.post(
            "https://challenges.cloudflare.com/turnstile/v0/siteverify",
            data={
                "secret": settings.AIRVOTE_TURNSTILE_SECRET_KEY,
                "response": token,
            },
            timeout=6,
        )
        if answer.status_code != 200:
            return False
        result = answer.json()
        # Do not accept a token issued for another site/action.
        return (
            result.get("success") is True
            and result.get("action") == action
            and result.get("hostname", "").lower()
            == request.get_host().split(":", 1)[0].lower()
        )
    except (requests.RequestException, ValueError):
        logger.warning("Online vote bot verification unavailable")
        return False


@never_cache
@ensure_csrf_cookie
@require_GET
def home(request):
    return render(request, "vote/portal.html", {
        "voting_ready": portal_enabled(),
        "site_key": settings.AIRVOTE_TURNSTILE_SITE_KEY if portal_enabled() else "",
    })


@never_cache
@require_GET
def stations(request):
    entries = []
    shows_by_station = {}
    now = timezone.now()
    for show in ChartShow.objects.filter(
        kind__in=("weekly", "year_end"), is_archived=False,
    ).order_by("name", "id"):
        shows_by_station.setdefault(show.station, []).append({
            "id": show.pk,
            "name": show.name,
            "code": show.code,
            "open": show.accepts_at(now),
        })
    for station in BroadcastStation.objects.filter(is_active=True).order_by("name"):
        shows = shows_by_station.get(station.slug, [])
        if shows:
            entries.append({
                "slug": station.slug,
                "name": station.name,
                "logo": station.logo_url if station.logo_url.startswith("https://") else "",
                "shows": shows,
            })
    return JsonResponse({"stations": entries, "voting_enabled": portal_enabled()})


def _published_list(show, day):
    edition = EligibleEdition.objects.filter(
        station=show.station,
        chart_show=show,
        status="locked",
        voting_start__lte=day,
        voting_end__gte=day,
    ).order_by("-chart_date", "-pk").first()
    if not edition:
        return None, []
    songs = CleanedSong.objects.filter(
        station=show.station,
        status="verified",
        candidate_editions__edition=edition,
    ).order_by("title", "artist", "pk").distinct()
    return edition, songs


@never_cache
@require_GET
def show_detail(request, show_id):
    show = ChartShow.objects.filter(
        pk=show_id, kind__in=("weekly", "year_end"), is_archived=False,
        station__in=BroadcastStation.objects.filter(is_active=True).values("slug"),
    ).first()
    if show is None:
        return _json_error("Chart show not found.", 404)
    now = timezone.now()
    edition, songs = _published_list(show, timezone.localdate(now))
    voting_open = bool(edition and show.accepts_at(now) and portal_enabled())
    return JsonResponse({
        "id": show.pk,
        "name": show.name,
        "station": show.station,
        "code": show.code,
        "presenter": show.presenter_name,
        "voting_open": voting_open,
        "show_scheduled_open": show.accepts_at(now),
        "has_published_songs": bool(edition),
        "songs": [
            {"id": s.pk, "artist": s.artist, "title": s.title}
            for s in songs[:300]
        ],
    })


@never_cache
@require_POST
def start_verification(request):
    if not portal_enabled():
        return _json_error("Online voting is not enabled yet.", 503)
    email = normalized_email(request.POST.get("email", ""))
    if not email:
        return _json_error("Enter a valid email address.")
    show_id, song_id = request.POST.get("show_id", ""), request.POST.get("song_id", "")
    if not (show_id.isdecimal() and song_id.isdecimal()):
        return _json_error("Select an eligible song before requesting a code.")
    show = ChartShow.objects.filter(
        pk=int(show_id), kind__in=("weekly", "year_end"),
        is_archived=False,
        station__in=BroadcastStation.objects.filter(is_active=True).values("slug"),
    ).first()
    if show is None or not show.accepts_at(timezone.now()):
        return _json_error("Voting for this chart show is not open.", 409)
    edition, _ = _published_list(show, timezone.localdate())
    if not edition or not EditionCandidate.objects.filter(
        edition=edition, song_id=int(song_id), song__status="verified",
        song__station=show.station,
    ).exists():
        return _json_error("Choose a song from the published eligible list.", 409)
    if not _turnstile(request, "airvote-verify"):
        return _json_error("Complete the security check to request a code.", 403)
    fingerprint = email_fingerprint(email)
    if not _rate(("otp-ip", _browser_key(request)), 20, 3600):
        return _json_error("Too many verification requests. Try again later.", 429)
    if not _rate(("otp-email", fingerprint), 3, 3600):
        return _json_error("Too many requests for this email address.", 429)

    # Only a digest is persisted server-side, never the one-time code.
    code = f"{secrets.randbelow(1000000):06d}"
    nonce = secrets.token_urlsafe(20)
    digest = _code_digest(email, code, nonce)
    _clear_pending(request)
    request.session.pop(VERIFIED_ID_SESSION, None)
    request.session.pop(VERIFIED_UNTIL_SESSION, None)
    try:
        sent = send_mail(
            subject="Your AirVote voting code",
            message=(
                f"Your AirVote verification code is: {code}\n\n"
                "This code expires in 10 minutes.\n"
                "Do not share this code. If you did not request it, "
                "you can ignore this message."
            ),
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=[email],
            fail_silently=False,
        )
        if sent != 1:
            return _json_error("Verification email is unavailable. Try again later.", 503)
    except (smtplib.SMTPException, OSError, ValueError):
        logger.warning("Online voting verification email could not be sent")
        return _json_error("Verification email is unavailable. Try again later.", 503)
    request.session[VERIFY_EMAIL_SESSION] = email
    request.session[OTP_NONCE_SESSION] = nonce
    request.session[OTP_DIGEST_SESSION] = digest
    request.session[OTP_ATTEMPTS_SESSION] = 0
    request.session[OTP_EXPIRES_SESSION] = int(time.time()) + 600
    return JsonResponse({"ok": True, "message": "A verification code has been sent to your email."})


@never_cache
@require_POST
def check_verification(request):
    if not portal_enabled():
        return _json_error("Online voting is not enabled yet.", 503)
    email = request.session.get(VERIFY_EMAIL_SESSION, "")
    expiry = request.session.get(OTP_EXPIRES_SESSION, 0)
    digest = request.session.get(OTP_DIGEST_SESSION, "")
    nonce = request.session.get(OTP_NONCE_SESSION, "")
    if (not email or not digest or not nonce or
            type(expiry) is not int or time.time() >= expiry):
        _clear_pending(request)
        return _json_error("Request a new email verification code.")
    code = request.POST.get("code", "")
    if not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code):
        return _json_error("Enter the six-digit code sent to your email.")
    fingerprint = email_fingerprint(email)
    attempts = request.session.get(OTP_ATTEMPTS_SESSION, 0)
    if attempts >= 6 or not _rate(
        ("otp-check", fingerprint), 12, 600,
    ):
        _clear_pending(request)
        return _json_error("Too many attempts. Request a new code.", 429)
    request.session[OTP_ATTEMPTS_SESSION] = attempts + 1
    if not hmac.compare_digest(digest, _code_digest(email, code, nonce)):
        if attempts + 1 >= 6:
            _clear_pending(request)
        return _json_error("Incorrect or expired email verification code.")
    request.session[VERIFIED_ID_SESSION] = fingerprint
    request.session[VERIFIED_UNTIL_SESSION] = int(time.time()) + 1800
    _clear_pending(request)
    return JsonResponse({"ok": True, "message": "Email verified. You can now vote."})


@never_cache
@require_POST
def vote(request):
    if not portal_enabled():
        return _json_error("Online voting is not enabled yet.", 503)
    voter_key = request.session.get(VERIFIED_ID_SESSION, "")
    expires = request.session.get(VERIFIED_UNTIL_SESSION, 0)
    if (not isinstance(voter_key, str)
            or not re.fullmatch(r"[0-9a-f]{64}", voter_key)
            or type(expires) is not int or time.time() >= expires):
        return _json_error("Verify your email before voting.", 403)
    if not _rate(("vote-ip", _browser_key(request)), 90, 3600):
        return _json_error("Too many votes from this connection.", 429)
    if not _rate(("vote-email", voter_key), 12, 3600):
        return _json_error("Vote submission limit reached. Try again later.", 429)
    if not _turnstile(request, "airvote-vote"):
        return _json_error("Complete the security check before voting.", 403)
    show_id, song_id = request.POST.get("show_id", ""), request.POST.get("song_id", "")
    if not (show_id.isdecimal() and song_id.isdecimal()):
        return _json_error("Select a chart show and a song.")
    show = ChartShow.objects.filter(
        pk=int(show_id), kind__in=("weekly", "year_end"),
        is_archived=False, station__in=BroadcastStation.objects.filter(
            is_active=True).values("slug"),
    ).first()
    if show is None:
        return _json_error("Chart show not found.", 404)
    now = timezone.now()
    if not show.accepts_at(now):
        return _json_error("Voting for this chart show is closed.", 409)
    edition, _ = _published_list(show, timezone.localdate(now))
    if edition is None:
        return _json_error("The presenter has not published eligible songs yet.", 409)
    song = CleanedSong.objects.filter(
        pk=int(song_id), station=show.station, status="verified",
        candidate_editions__edition=edition,
    ).first()
    if song is None:
        return _json_error("This song is not on the current eligible list.", 409)
    # Count only server-derived song identity. No public endpoint accepts
    # arbitrary station ID, artist spelling, chart code or direct RawVote writes.
    text = f"{song.artist} - {song.title}"
    if len(text) > 100:
        return _json_error("This song's title needs review before online voting.", 409)
    with transaction.atomic():
        from apps.voting.pipeline import lock_station
        lock_station(show.station)
        # Queue quota: reject duplicates before they can consume worker/AI
        # resources. Historical votes still go through the normal worker checks.
        queued = InboundEvent.objects.filter(
            provider="web", station=show.station, chart_show=show,
            sender=voter_key,
            received_at__date=timezone.localdate(now),
        ).count()
        if queued >= show.daily_limit:
            return _json_error("You have reached this chart show's daily voting limit.", 429)
        if InboundEvent.objects.filter(
            provider="web", station=show.station, chart_show=show,
            sender=voter_key, vote_text=text,
            received_at__date=timezone.localdate(now),
        ).exists() and not settings.ALLOW_REPEAT_SONG:
            return _json_error("You have already voted for this song today.", 409)
        InboundEvent.objects.create(
            provider="web",
            station=show.station, chart_show=show,
            message_id=uuid.uuid4().hex,
            sender=voter_key, text=text, vote_text=text,
            submitted_code=show.code,
            received_at=now,
        )
    return JsonResponse({
        "ok": True,
        "message": "Vote received. It will count only after verification.",
    }, status=202)
