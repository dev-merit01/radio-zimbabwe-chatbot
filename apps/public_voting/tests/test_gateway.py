"""Private bridge, live AirVote configuration and retry-safe PostgreSQL admission."""
import hashlib
import hmac
import time
import uuid
from datetime import timedelta

from django.test import TestCase, override_settings
from django.utils import timezone

from apps.accounts.models import BroadcastStation
from apps.public_voting.admission import admit_web_vote
from apps.public_voting.bridge import signing_message
from apps.public_voting.models import WebVoteReceipt
from apps.public_voting.views import voting_link
from apps.voting.models import (
    ChartShow, CleanedSong, EditionCandidate, EligibleEdition,
    RawVote, StationVotingConfig,
)


@override_settings(
    SECURE_SSL_REDIRECT=False,
    AIRVOTE_WEB_BRIDGE_SECRET="this-is-a-test-only-bridge-secret-at-least-32",
    AIRVOTE_COMPANY_LICENSING_ENFORCED=False,
    AIRVOTE_PUBLIC_VOTING_URL="https://vote.example.org",
    AI_ALL_VOTES=False,
)
class PublicVotingGatewayTests(TestCase):
    def setUp(self):
        BroadcastStation.objects.update_or_create(
            slug="national_fm", defaults={"name": "National FM", "is_active": True},
        )
        BroadcastStation.objects.update_or_create(
            slug="power_fm", defaults={"name": "Power FM", "is_active": True},
        )
        self.show = ChartShow.objects.create(
            station="national_fm", name="Public Gateway Test Programme",
            kind="weekly", code="PV2026", schedule_mode="manual",
            manual_open=True, is_enabled=True, eligibility_enforced=True,
            daily_limit=3,
        )
        StationVotingConfig.objects.update_or_create(
            station="national_fm", defaults={"public_web_enabled": True}
        )
        self.song = CleanedSong.objects.create(
            station="national_fm", artist="Example Performer",
            title="The Example Song", canonical_name="Example Performer - The Example Song",
            status="verified",
        )
        self.outsider = CleanedSong.objects.create(
            station="national_fm", artist="Example Performer",
            title="Unapproved", canonical_name="Example Performer - Unapproved",
            status="verified",
        )
        now = timezone.localdate()
        self.edition = EligibleEdition.objects.create(
            station="national_fm", chart_show=self.show, chart_date=now,
            voting_start=now - timedelta(days=1),
            voting_end=now + timedelta(days=1), status="locked",
        )
        EditionCandidate.objects.create(edition=self.edition, song=self.song)

    def signed_get(self, path):
        moment = str(int(time.time()))
        signature = hmac.new(
            b"this-is-a-test-only-bridge-secret-at-least-32",
            signing_message("GET", path, moment),
            hashlib.sha256,
        ).hexdigest()
        return self.client.get(
            path, HTTP_X_AIRVOTE_TIMESTAMP=moment,
            HTTP_X_AIRVOTE_SIGNATURE=signature,
        )

    def payload(self, vote_id=None, station="national_fm", show_id=None, song_id=None):
        return {
            "id": str(vote_id or uuid.uuid4()),
            "station": station, "show_id": show_id or self.show.pk,
            "song_id": song_id or self.song.pk,
            "voter_key": "a" * 64,
            "received_at": timezone.now().isoformat(),
        }

    def test_bridge_cannot_be_called_without_valid_timed_signature(self):
        path = "/api/voting-bridge/stations"
        self.assertEqual(self.client.get(path).status_code, 401)
        self.assertEqual(self.signed_get(path).status_code, 200)
        response = self.client.get(
            path, HTTP_X_AIRVOTE_TIMESTAMP=str(int(time.time()) - 120),
            HTTP_X_AIRVOTE_SIGNATURE="f" * 64,
        )
        self.assertEqual(response.status_code, 401)
        other = self.signed_get("/api/voting-bridge/resolve/national_fm/PV2026")
        self.assertEqual(other.status_code, 200)

    def test_campaign_contains_only_verified_approved_songs_and_live_rules(self):
        path = "/api/voting-bridge/resolve/national_fm/PV2026"
        token = self.signed_get(path).json()["campaign"]
        config = self.signed_get("/api/voting-bridge/campaign/" + token)
        self.assertEqual(config.status_code, 200, config.content)
        data = config.json()
        self.assertTrue(data["open"])
        self.assertEqual([s["id"] for s in data["songs"]], [self.song.pk])
        self.assertNotIn("rank", str(data).lower())
        self.assertNotIn("votes", data)
        self.assertEqual(data["daily_limit"], 3)
        StationVotingConfig.objects.update_or_create(
            station="national_fm",
            defaults={"station_daily_limit": 1, "repeat_song_policy": "block"},
        )
        updated = self.signed_get("/api/voting-bridge/campaign/" + token).json()
        self.assertEqual(updated["station_daily_limit"], 1)
        self.assertEqual(updated["repeat_scope"], "station")
        self.show.manual_open = False
        self.show.save(update_fields=["manual_open"])
        self.assertFalse(self.signed_get("/api/voting-bridge/campaign/" + token).json()["open"])

    def test_programme_is_not_publicly_listed_and_links_are_signed(self):
        self.assertEqual(self.client.get("/vote/").status_code, 404)
        url = voting_link(self.show)
        self.assertTrue(url.startswith("https://vote.example.org/vote/"))
        token = url.split("/vote/", 1)[1]
        self.assertEqual(
            self.signed_get("/api/voting-bridge/campaign/" + token).status_code, 200,
        )
        self.assertEqual(
            self.signed_get("/api/voting-bridge/campaign/" + token + "BAD").status_code,
            404,
        )
        self.assertEqual(
            self.signed_get("/api/voting-bridge/resolve/power_fm/PV2026").status_code,
            404,
        )

    def test_first_valid_vote_is_enqueued_once_even_after_replay(self):
        raw = self.payload()
        first = admit_web_vote(raw)
        second = admit_web_vote(raw)
        self.assertEqual(first["state"], "admitted")
        self.assertEqual(second, first)
        self.assertEqual(RawVote.objects.filter(source="web", station="national_fm").count(), 1)
        self.assertTrue(WebVoteReceipt.objects.filter(vote_id=raw["id"]).exists())
        self.assertEqual(
            RawVote.objects.get(source="web").voter_fingerprint, "a" * 64,
        )

    def test_live_vote_limits_and_eligibility_apply_to_web_as_to_whatsapp(self):
        StationVotingConfig.objects.update_or_create(
            station="national_fm",
            defaults={"station_daily_limit": 1, "repeat_song_policy": "block"},
        )
        self.assertEqual(admit_web_vote(self.payload())["state"], "admitted")
        duplicate = admit_web_vote(self.payload())
        self.assertEqual(duplicate["state"], "rejected")
        self.assertIn(duplicate["reason"], ("vote_limit", "repeat_vote"))
        self.assertEqual(RawVote.objects.filter(source="web").count(), 1)
        other = admit_web_vote(self.payload(song_id=self.outsider.pk))
        self.assertEqual(other["reason"], "song_not_eligible")
        cross = admit_web_vote(self.payload(station="power_fm"))
        self.assertEqual(cross["reason"], "voting_closed")
        self.assertEqual(RawVote.objects.count(), 1)

    def test_station_web_voting_can_be_disabled_immediately_from_airvote(self):
        from apps.public_voting.views import permitted_station

        self.assertTrue(permitted_station("national_fm"))
        config = StationVotingConfig.objects.get(station="national_fm")
        config.public_web_enabled = False
        config.save(update_fields=["public_web_enabled"])
        self.assertFalse(permitted_station("national_fm"))
        self.assertEqual(
            self.signed_get("/api/voting-bridge/resolve/national_fm/PV2026").status_code,
            404,
        )
        blocked = admit_web_vote(self.payload())
        self.assertEqual(blocked["reason"], "station_unavailable")
        self.assertEqual(RawVote.objects.count(), 0)

    def test_stale_and_closed_votes_never_reach_the_main_vote_table(self):
        expired = self.payload()
        expired["received_at"] = (timezone.now() - timedelta(days=5)).isoformat()
        self.assertEqual(admit_web_vote(expired)["reason"], "receipt_expired")
        self.show.is_enabled = False
        self.show.save(update_fields=["is_enabled"])
        closed = self.payload()
        self.assertEqual(admit_web_vote(closed)["reason"], "voting_closed")
        self.assertEqual(RawVote.objects.count(), 0)

    def test_admission_rejects_malformed_ids_and_identity(self):
        bad = self.payload()
        bad["voter_key"] = "raw-email@example.org"
        with self.assertRaises(ValueError):
            admit_web_vote(bad)
        bad = self.payload()
        bad["show_id"] = True
        with self.assertRaises(ValueError):
            admit_web_vote(bad)
        self.assertEqual(RawVote.objects.count(), 0)
