"""Security and data integrity for the public listener voting portal."""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from apps.accounts.models import BroadcastStation, AccountProfile, PresenterAssignment
from apps.voting.channels import normalized_email, email_fingerprint
from apps.voting.models import (
    ChartShow, CleanedSong, EditionCandidate, EligibleEdition,
    InboundEvent, RawVote, VoteJob,
)
from apps.voting.worker import run_one


SETTINGS = {
    "SECURE_SSL_REDIRECT": False,
    "AIRVOTE_PUBLIC_VOTING_ENABLED": True,
    "AI_ALL_VOTES": True,
    "AIRVOTE_TURNSTILE_SITE_KEY": "test-site-key",
    "AIRVOTE_TURNSTILE_SECRET_KEY": "test-secret-key",
    "EMAIL_HOST": "smtp.example.test",
    "EMAIL_HOST_USER": "airvote@example.test",
    "EMAIL_HOST_PASSWORD": "test-only",
    "DEFAULT_FROM_EMAIL": "AirVote <vote@example.test>",
    "EMAIL_USE_TLS": True,
    "EMAIL_USE_SSL": False,
    "CACHES": {"default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    }},
}


@override_settings(**SETTINGS)
class PublicVotePortalTests(TestCase):
    def setUp(self):
        cache.clear()
        BroadcastStation.objects.update_or_create(
            slug="radio_zimbabwe",
            defaults={"name": "Radio Zimbabwe", "is_active": True},
        )
        BroadcastStation.objects.update_or_create(
            slug="national_fm",
            defaults={"name": "National FM", "is_active": True},
        )
        self.show = ChartShow.objects.create(
            station="radio_zimbabwe", name="AirVote Test Programme",
            code="AVTST", kind="weekly", schedule_mode="manual",
            is_enabled=True, manual_open=True, eligibility_enforced=True,
        )
        self.foreign_show = ChartShow.objects.create(
            station="national_fm", name="Foreign Programme",
            code="AVFRGN", kind="weekly", schedule_mode="manual",
            is_enabled=True, manual_open=True, eligibility_enforced=True,
        )
        today = timezone.localdate()
        self.edition = EligibleEdition.objects.create(
            station="radio_zimbabwe", chart_show=self.show,
            chart_date=today, voting_start=today, voting_end=today,
            status="locked",
        )
        self.song = CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Jnr Spragga",
            title="Daka", canonical_name="Jnr Spragga - Daka",
            status="verified",
        )
        self.other_song = CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Other Artist",
            title="Unlisted", canonical_name="Other Artist - Unlisted",
            status="verified",
        )
        EditionCandidate.objects.create(edition=self.edition, song=self.song)
        self.client = Client()
        self.email = "listener@example.com"

    def _verify(self, client=None, email=None):
        client = client or self.client
        email = email or self.email
        with patch("apps.voting.portal._turnstile", return_value=True), patch(
            "apps.voting.portal.send_mail", return_value=1,
        ) as mail, patch("apps.voting.portal.secrets.randbelow", return_value=123456):
            response = client.post("/vote/api/verify/start/", {
                "email": email, "show_id": self.show.pk, "song_id": self.song.pk,
                "cf-turnstile-response": "token-placeholder",
            })
            self.assertEqual(response.status_code, 200, response.content)
            mail.assert_called_once()
            self.assertEqual(mail.call_args.kwargs["recipient_list"],
                             [normalized_email(email)])
            self.assertIn("123456", mail.call_args.kwargs["message"])
            response = client.post("/vote/api/verify/check/", {"code": "123456"})
            self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(client.session["airvote_web_email_verified_voter"],
                         email_fingerprint(email))
        self.assertNotIn("airvote_web_pending_email", client.session)
        return client

    def _cast(self, show=None, song=None, client=None):
        client = client or self.client
        with patch("apps.voting.portal._turnstile", return_value=True):
            return client.post("/vote/api/cast/", {
                "show_id": (show or self.show).pk,
                "song_id": (song or self.song).pk,
                "cf-turnstile-response": "token-placeholder",
            })

    def test_public_portal_is_readable_without_staff_login(self):
        self.assertEqual(self.client.get("/vote/").status_code, 200)
        stations = self.client.get("/vote/api/stations/").json()["stations"]
        self.assertTrue(any(s["slug"] == "radio_zimbabwe" for s in stations))
        show = self.client.get(f"/vote/api/shows/{self.show.pk}/").json()
        self.assertEqual(show["name"], self.show.name)
        self.assertTrue(show["voting_open"])
        self.assertEqual([s["id"] for s in show["songs"]], [self.song.pk])
        self.assertNotIn("raw_votes", show)
        self.assertEqual(self.client.get("/api/workspace/incoming").status_code, 401)

    def test_no_published_list_explains_reason_without_claiming_hours_closed(self):
        self.edition.status = "draft"
        self.edition.save(update_fields=["status"])
        payload = self.client.get(f"/vote/api/shows/{self.show.pk}/").json()
        self.assertTrue(payload["show_scheduled_open"])
        self.assertFalse(payload["voting_open"])
        self.assertFalse(payload["has_published_edition"])
        self.assertFalse(payload["has_published_songs"])
        self.assertEqual(payload["songs"], [])
        self.assertEqual(payload["voting_status"], "awaiting_song_list")
        self.assertEqual(payload["voting_status_label"], "Awaiting song list")
        self.assertEqual(
            [item["code"] for item in payload["voting_blockers"]],
            ["no_published_list"],
        )

    def test_zero_verified_songs_is_not_an_open_voting_show(self):
        EditionCandidate.objects.filter(edition=self.edition).delete()
        payload = self.client.get(f"/vote/api/shows/{self.show.pk}/").json()
        self.assertTrue(payload["has_published_edition"])
        self.assertFalse(payload["has_published_songs"])
        self.assertEqual(payload["eligible_song_count"], 0)
        self.assertFalse(payload["voting_open"])
        self.assertEqual(payload["voting_status"], "no_eligible_songs")
        self.assertEqual(
            [item["code"] for item in payload["voting_blockers"]],
            ["no_verified_songs"],
        )

    def test_portal_not_ready_is_not_mistaken_for_closed_voting_hours(self):
        with override_settings(AIRVOTE_PUBLIC_VOTING_ENABLED=False):
            payload = self.client.get(f"/vote/api/shows/{self.show.pk}/").json()
        self.assertTrue(payload["show_scheduled_open"])
        self.assertTrue(payload["has_published_songs"])
        self.assertFalse(payload["voting_open"])
        self.assertFalse(payload["online_voting_ready"])
        self.assertEqual(payload["voting_status"], "online_setup")
        self.assertEqual(payload["voting_status_label"], "Online voting not active")
        self.assertEqual(
            [item["code"] for item in payload["voting_blockers"]],
            ["online_setup"],
        )

    def test_schedule_closed_is_reported_separately_from_portal_setup(self):
        self.show.manual_open = False
        self.show.save(update_fields=["manual_open"])
        payload = self.client.get(f"/vote/api/shows/{self.show.pk}/").json()
        self.assertFalse(payload["show_scheduled_open"])
        self.assertTrue(payload["online_voting_ready"])
        self.assertEqual(payload["voting_status"], "voting_hours_closed")
        self.assertIn("voting hours", payload["voting_blockers"][0]["message"])
        self.assertFalse(payload["voting_open"])

    def test_missing_list_and_unconfigured_portal_are_both_explained(self):
        self.edition.status = "draft"
        self.edition.save(update_fields=["status"])
        with override_settings(AIRVOTE_PUBLIC_VOTING_ENABLED=False):
            payload = self.client.get(f"/vote/api/shows/{self.show.pk}/").json()
        self.assertEqual(payload["voting_status"], "awaiting_song_list")
        self.assertTrue(payload["show_scheduled_open"])
        self.assertEqual(
            {item["code"] for item in payload["voting_blockers"]},
            {"no_published_list", "online_setup"},
        )

    def test_missing_provider_credentials_fail_closed_without_changing_other_channels(self):
        with override_settings(EMAIL_HOST_PASSWORD=""):
            self.assertContains(self.client.get("/vote/"), "being prepared")
            self.assertEqual(self.client.post("/vote/api/verify/start/", {
                "email": self.email,
            }).status_code, 503)
            self.assertFalse(self.client.get(
                f"/vote/api/shows/{self.show.pk}/"
            ).json()["voting_open"])
        self.assertEqual(InboundEvent.objects.filter(provider="web").count(), 0)

    def test_csrf_is_required_for_public_submission(self):
        client = Client(enforce_csrf_checks=True)
        client.get("/vote/")
        self.assertEqual(client.post("/vote/api/verify/start/", {
            "email": self.email,
        }).status_code, 403)
        self.assertEqual(client.post("/vote/api/cast/", {
            "show_id": self.show.pk, "song_id": self.song.pk,
        }).status_code, 403)

    def test_email_verification_is_required_and_cannot_be_forged(self):
        self.assertEqual(self._cast().status_code, 403)
        session = self.client.session
        session["airvote_web_email_verified_voter"] = "not-a-real-fingerprint"
        session["airvote_web_email_verified_until"] = 99999999999
        session.save()
        self.assertEqual(self._cast().status_code, 403)
        self.assertEqual(InboundEvent.objects.filter(provider="web").count(), 0)

    def test_browser_vote_is_queued_for_existing_ai_pipeline_with_source(self):
        self._verify()
        response = self._cast()
        self.assertEqual(response.status_code, 202, response.content)
        event = InboundEvent.objects.get(provider="web")
        self.assertEqual(event.chart_show, self.show)
        self.assertEqual(event.station, "radio_zimbabwe")
        self.assertEqual(event.vote_text, "Jnr Spragga - Daka")
        self.assertEqual(event.submitted_code, "AVTST")
        self.assertEqual(event.sender, email_fingerprint(self.email))
        self.assertNotIn(self.email, event.text)
        self.assertEqual(RawVote.objects.count(), 0)
        self.assertTrue(run_one(InboundEvent))
        event.refresh_from_db()
        self.assertEqual(event.state, "done")
        vote = RawVote.objects.get()
        self.assertEqual(vote.source, "web")
        self.assertEqual(vote.user.channel, "web")
        self.assertEqual(vote.voter_fingerprint, event.sender)
        self.assertEqual(vote.ai_status, "awaiting")
        self.assertTrue(VoteJob.objects.filter(vote=vote).exists())

    def test_unlisted_song_other_station_or_closed_show_never_enters_queue(self):
        self._verify()
        self.assertEqual(self._cast(song=self.other_song).status_code, 409)
        self.assertEqual(self._cast(show=self.foreign_show).status_code, 409)
        self.show.manual_open = False
        self.show.save(update_fields=["manual_open"])
        self.assertEqual(self._cast().status_code, 409)
        self.assertFalse(InboundEvent.objects.filter(provider="web").exists())

    def test_duplicate_song_and_daily_limits_are_checked_before_queuing(self):
        self._verify()
        self.assertEqual(self._cast().status_code, 202)
        self.assertEqual(self._cast().status_code, 409)
        self.assertEqual(InboundEvent.objects.filter(provider="web").count(), 1)

    def test_reverification_on_another_browser_does_not_reset_vote_limit(self):
        self._verify()
        self.assertEqual(self._cast().status_code, 202)
        second = Client()
        self._verify(client=second)
        self.assertEqual(self._cast(client=second).status_code, 409)
        self.assertEqual(InboundEvent.objects.filter(provider="web").count(), 1)

    def test_gmail_aliases_share_one_identity(self):
        self._verify(email="Lis.tener+chart@gmail.com")
        self.assertEqual(self._cast().status_code, 202)
        second = Client()
        self._verify(client=second, email="listener@googlemail.com")
        self.assertEqual(self._cast(client=second).status_code, 409)

    def test_email_code_rejects_errors_and_expiration(self):
        with patch("apps.voting.portal._turnstile", return_value=True), patch(
            "apps.voting.portal.send_mail", return_value=1,
        ), patch("apps.voting.portal.secrets.randbelow", return_value=123456):
            response = self.client.post("/vote/api/verify/start/", {
                "email": self.email, "show_id": self.show.pk,
                "song_id": self.song.pk, "cf-turnstile-response": "token-placeholder",
            })
        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(self.client.post("/vote/api/verify/check/", {
            "code": "111111",
        }).status_code, 400)
        self.assertNotIn("airvote_web_email_verified_voter", self.client.session)
        session = self.client.session
        session["airvote_web_email_pending_expires"] = 1
        session.save()
        self.assertEqual(self.client.post("/vote/api/verify/check/", {
            "code": "123456",
        }).status_code, 400)

    def test_failed_email_delivery_leaves_no_valid_verification(self):
        with patch("apps.voting.portal._turnstile", return_value=True), patch(
            "apps.voting.portal.send_mail", side_effect=OSError("smtp down"),
        ):
            response = self.client.post("/vote/api/verify/start/", {
                "email": self.email, "show_id": self.show.pk,
                "song_id": self.song.pk, "cf-turnstile-response": "token-placeholder",
            })
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("airvote_web_pending_email", self.client.session)
        self.assertNotIn("airvote_web_email_verified_voter", self.client.session)

    def test_source_analytics_limited_to_presenters_own_programme(self):
        presenter = get_user_model().objects.create_user(
            "public-portal-presenter", password="test-password",
        )
        AccountProfile.objects.update_or_create(
            user=presenter, defaults={"station": "radio_zimbabwe"},
        )
        PresenterAssignment.objects.create(user=presenter, chart_show=self.show)
        self._verify()
        self.assertEqual(self._cast().status_code, 202)
        self.assertTrue(run_one(InboundEvent))
        self.client.force_login(presenter)
        result = self.client.get("/api/workspace/source-analytics").json()
        rows = {row["source"]: row for row in result["sources"]}
        self.assertEqual(rows["web"]["received"], 1)
        self.assertEqual(rows["web"]["awaiting"], 1)
        self.assertEqual(rows["whatsapp"]["received"], 0)
        incoming = self.client.get("/api/workspace/incoming").json()
        self.assertEqual(incoming["items"][0]["source"], "web")
        self.assertEqual(incoming["items"][0]["source_label"], "Online portal")
        self.assertEqual(incoming["submissions"][0]["source_label"], "Online portal")
        self.assertEqual(self.client.get("/api/workspace/source-analytics", {
            "show_id": self.foreign_show.pk,
        }).status_code, 400)

    def test_email_normalization_and_identity_are_private_and_stable(self):
        self.assertEqual(normalized_email(" Listener@Example.com "), self.email)
        self.assertEqual(normalized_email("invalid email"), "")
        self.assertEqual(len(email_fingerprint(self.email)), 64)
        self.assertEqual(
            email_fingerprint("Lis.tener+vote@gmail.com"),
            email_fingerprint("listener@googlemail.com"),
        )
        self.assertNotEqual(email_fingerprint(self.email),
                            email_fingerprint("someoneelse@example.com"))

    def test_turnstile_validation_checks_hostname_and_action(self):
        from apps.voting.portal import _turnstile
        request = self.client.request().wsgi_request
        request.method = "POST"
        request.POST = {"cf-turnstile-response": "valid-test-token-123456789"}
        with patch("apps.voting.portal.requests.post") as post:
            post.return_value.status_code = 200
            post.return_value.json.return_value = {
                "success": True, "action": "wrong-action",
                "hostname": "testserver",
            }
            self.assertFalse(_turnstile(request, "airvote-vote"))
            post.return_value.json.return_value = {
                "success": True, "action": "airvote-vote",
                "hostname": "attacker.example",
            }
            self.assertFalse(_turnstile(request, "airvote-vote"))
            post.return_value.json.return_value = {
                "success": True, "action": "airvote-vote",
                "hostname": "testserver",
            }
            self.assertTrue(_turnstile(request, "airvote-vote"))
