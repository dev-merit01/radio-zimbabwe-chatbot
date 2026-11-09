"""Presenter permissions and immutable station-approved candidate lists."""
import json
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from apps.accounts.models import AccountProfile, PresenterAssignment
from apps.charts.show_rankings import counts_for_show, ranked_songs
from apps.voting.models import (
    ChartShow, CleanedSong, EditionCandidate, EligibleEdition, MatchKeyMapping,
    RawVote, ReviewAudit, User,
)


@override_settings(SECURE_SSL_REDIRECT=False)
class PresenterEligibleEditionsTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser(
            username="chart-administrator", password="Administrator-Test-Password!8"
        )
        self.client.force_login(self.admin)
        self.client.post("/accounts/switch-station/", {"station": "national_fm"})
        self.saturday = ChartShow.objects.create(
            station="national_fm", name="New Saturday Countdown",
            code="WEEK", kind="weekly", schedule_mode="manual",
            manual_open=True, is_enabled=True, broadcast_weekday=5,
        )
        self.gospel = ChartShow.objects.create(
            station="national_fm", name="Sunday Gospel List",
            code="GOSPEL", kind="weekly", schedule_mode="manual",
            manual_open=True, is_enabled=True, broadcast_weekday=6,
        )
        self.power = ChartShow.objects.create(
            station="power_fm", name="Power FM Saturday",
            code="WEEK", kind="weekly", schedule_mode="manual",
            manual_open=True, is_enabled=True, broadcast_weekday=5,
        )
        self.next_saturday = timezone.localdate() + timedelta(
            days=(5 - timezone.localdate().weekday()) % 7 + 14
        )
        self.start = self.next_saturday - timedelta(days=6)
        self.song = CleanedSong.objects.create(
            station="national_fm", artist="Known Artist", title="Known Song",
            canonical_name="Known Artist - Known Song", status="verified",
        )
        self.outsider = CleanedSong.objects.create(
            station="national_fm", artist="Unknown Artist", title="New Track",
            canonical_name="Unknown Artist - New Track", status="verified",
        )

    def post(self, url, data, client=None):
        return (client or self.client).post(
            "/api/workspace/" + url, json.dumps(data),
            content_type="application/json",
        )

    def create_presenter(self, show=None, username="presenter-new"):
        show = show or self.saturday
        response = self.post("presenters/create", {
            "username": username,
            "password": "Presenter-Long-Unique-Test!87",
            "chart_show_id": show.pk,
        })
        self.assertEqual(response.status_code, 200, response.content)
        return get_user_model().objects.get(username=username)

    def create_edition(self, client=None, show=None):
        response = self.post("eligibility/create", {
            "chart_show_id": (show or self.saturday).pk,
            "chart_date": str(self.next_saturday),
        }, client)
        self.assertEqual(response.status_code, 200, response.content)
        return response.json()["edition"]["id"]

    def add_candidate(self, eid, song_id=None, client=None):
        return self.post("eligibility/candidate", {
            "edition_id": eid, "song_id": song_id or self.song.pk,
            "action": "add",
        }, client)

    def approved_edition(self):
        eid = self.create_edition()
        self.assertEqual(self.add_candidate(eid).status_code, 200)
        self.assertEqual(self.post(
            "eligibility/submit", {"edition_id": eid}
        ).status_code, 200)
        response = self.post(
            "eligibility/review", {"edition_id": eid, "action": "approve"},
        )
        self.assertEqual(response.status_code, 200, response.content)
        self.saturday.refresh_from_db()
        return EligibleEdition.objects.get(pk=eid)

    def raw_vote(self, song, sender, show=None):
        show = show or self.saturday
        user = User.objects.create(
            channel="whatsapp", user_ref=sender, station=show.station
        )
        key = song.artist.casefold() + "::" + song.title.casefold()
        MatchKeyMapping.objects.get_or_create(
            station=show.station, match_key=key,
            defaults={
                "cleaned_song": song, "sample_display_name": song.canonical_name,
            },
        )
        RawVote.objects.create(
            user=user, station=show.station, chart_show=show,
            raw_input=song.canonical_name,
            artist_raw=song.artist, song_raw=song.title,
            artist_normalized=song.artist.casefold(),
            song_normalized=song.title.casefold(),
            match_key=key, display_name=song.canonical_name,
            vote_date=self.start + timedelta(days=1),
            ai_status="approved", submitted_code=show.code,
        )

    def test_presenter_login_is_locked_to_its_station_and_show(self):
        presenter = self.create_presenter()
        self.assertEqual(presenter.profile.station, "national_fm")
        self.assertEqual(
            PresenterAssignment.objects.get(user=presenter).chart_show_id,
            self.saturday.pk,
        )
        client = Client()
        self.assertTrue(client.login(
            username=presenter.username,
            password="Presenter-Long-Unique-Test!87",
        ))
        response = client.get("/api/workspace/shows")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["presenter_show_id"], self.saturday.pk)
        self.assertEqual(
            [show["id"] for show in response.json()["shows"]], [self.saturday.pk]
        )
        self.assertEqual(
            client.get("/api/workspace/overview").json()["chart_show_id"],
            self.saturday.pk,
        )
        self.assertEqual(
            client.get("/api/workspace/overview", {
                "show_id": self.gospel.pk
            }).status_code, 400,
        )
        self.assertEqual(
            client.get("/api/chart/today", {
                "show_id": self.gospel.pk
            }).status_code, 400,
        )
        self.assertEqual(
            client.get("/api/chart/today", {
                "show_id": self.power.pk
            }).status_code, 400,
        )
        self.assertEqual(
            client.post("/accounts/switch-station/", {
                "station": "power_fm"
            }).status_code, 403,
        )
        self.assertEqual(client.get("/api/workspace/presenters").status_code, 403)
        self.assertEqual(client.get("/api/workspace/administration").status_code, 403)
        self.assertEqual(client.get("/api/stats").status_code, 403)
        self.assertEqual(client.get("/api/workspace/health").status_code, 403)
        self.assertEqual(client.get("/api/workspace/audit").status_code, 403)

    def test_admin_can_switch_stations_without_presenter_assignment(self):
        self.assertFalse(
            PresenterAssignment.objects.filter(user=self.admin).exists()
        )
        self.assertGreaterEqual(
            len(self.client.get("/api/workspace/shows").json()["shows"]), 3
        )
        self.assertEqual(
            self.client.post("/accounts/switch-station/", {
                "station": "power_fm"
            }).status_code, 302,
        )
        self.assertEqual(
            self.client.get("/api/workspace/shows").json()["station"], "power_fm"
        )

    def test_presenter_can_prepare_but_only_admin_locks_shortlist(self):
        presenter = self.create_presenter()
        client = Client()
        client.force_login(presenter)
        eid = self.create_edition(client)
        self.assertEqual(self.add_candidate(eid, client=client).status_code, 200)
        self.assertEqual(self.post(
            "eligibility/candidate", {
                "edition_id": eid, "song_id": self.outsider.pk, "action": "add",
            }, client,
        ).status_code, 200)
        self.assertEqual(self.post(
            "eligibility/candidate", {
                "edition_id": eid, "song_id": self.outsider.pk, "action": "remove",
            }, client,
        ).status_code, 200)
        self.assertEqual(self.post(
            "eligibility/submit", {"edition_id": eid}, client
        ).status_code, 200)
        self.assertEqual(self.post(
            "eligibility/review", {"edition_id": eid, "action": "approve"}, client
        ).status_code, 403)
        self.assertEqual(self.post(
            "eligibility/candidate", {
                "edition_id": eid, "song_id": self.outsider.pk, "action": "add",
            }, client,
        ).status_code, 400)
        self.assertEqual(self.post(
            "eligibility/review", {"edition_id": eid, "action": "approve"}
        ).status_code, 200)
        edition = EligibleEdition.objects.get(pk=eid)
        self.assertEqual(edition.status, "locked")
        self.assertEqual(edition.approved_by, self.admin)
        self.saturday.refresh_from_db()
        self.assertTrue(self.saturday.eligibility_enforced)
        self.assertEqual(self.add_candidate(eid, song_id=self.outsider.pk).status_code, 400)
        self.assertEqual(
            self.post("eligibility/review", {
                "edition_id": eid, "action": "approve",
            }).status_code, 400,
        )
        self.assertTrue(
            ReviewAudit.objects.filter(
                station="national_fm", action="edition_approved",
            ).exists()
        )

    def test_unapproved_songs_receive_interest_but_no_official_rank(self):
        edition = self.approved_edition()
        self.raw_vote(self.song, "listener-1")
        for i in range(6):
            self.raw_vote(self.outsider, f"supporter-{i}")
        counts = counts_for_show(self.saturday, edition.voting_start, edition.voting_end)
        self.assertEqual(counts, {self.song.pk: 1})
        ranked = ranked_songs(self.saturday, edition.voting_start, edition.voting_end)
        self.assertEqual([(song.pk, count) for song, count in ranked],
                         [(self.song.pk, 1)])
        response = self.client.get(
            "/api/workspace/eligibility",
            {"show_id": self.saturday.pk, "edition_id": edition.pk},
        )
        self.assertEqual(response.status_code, 200, response.content)
        interest = response.json()["nomination_interest"]
        self.assertEqual(interest[0]["display_name"], self.outsider.canonical_name)
        self.assertEqual(interest[0]["n"], 6)
        self.assertEqual(RawVote.objects.count(), 7)
        # Another show's old ranking remains independent and unmodified.
        self.raw_vote(self.song, "other-listener", self.gospel)
        gospel_counts = counts_for_show(
            self.gospel, edition.voting_start, edition.voting_end,
        )
        self.assertEqual(gospel_counts, {self.song.pk: 1})

    def test_cross_show_and_cross_station_list_modification_denied(self):
        presenter = self.create_presenter()
        client = Client()
        client.force_login(presenter)
        eid = self.create_edition()
        self.assertEqual(
            self.post("eligibility/candidate", {
                "edition_id": eid, "song_id": self.song.pk, "action": "add",
            }, client).status_code, 200,
        )
        self.assertEqual(
            self.post("eligibility/create", {
                "chart_show_id": self.gospel.pk,
                "chart_date": str(self.next_saturday + timedelta(days=1)),
            }, client).status_code, 400,
        )
        self.assertEqual(
            self.post("eligibility/create", {
                "chart_show_id": self.power.pk,
                "chart_date": str(self.next_saturday),
            }, client).status_code, 400,
        )
        power_song = CleanedSong.objects.create(
            station="power_fm", artist="Another", title="Track",
            canonical_name="Another - Track", status="verified",
        )
        self.assertEqual(
            self.add_candidate(eid, power_song.pk, client).status_code, 400,
        )

    def test_pending_suggestion_requires_review_and_verified_song(self):
        presenter = self.create_presenter()
        client = Client()
        client.force_login(presenter)
        eid = self.create_edition(client)
        self.assertEqual(self.add_candidate(eid, client=client).status_code, 200)
        response = self.post("eligibility/suggest", {
            "edition_id": eid, "artist": "Fresh Artist",
            "title": "Upcoming Song", "notes": "Independent release",
        }, client)
        self.assertEqual(response.status_code, 200)
        suggestion_id = response.json()["suggestion_id"]
        self.assertEqual(self.post(
            "eligibility/submit", {"edition_id": eid}, client
        ).status_code, 200)
        self.assertEqual(self.post(
            "eligibility/review", {
                "edition_id": eid, "action": "approve",
            }).status_code, 400,
        )
        new_song = CleanedSong.objects.create(
            station="national_fm", artist="Fresh Artist",
            title="Upcoming Song", canonical_name="Fresh Artist - Upcoming Song",
            status="verified",
        )
        self.assertEqual(self.post(
            "eligibility/suggestion/review", {
                "suggestion_id": suggestion_id, "action": "accept",
                "song_id": new_song.pk,
            }
        ).status_code, 200)
        self.assertEqual(
            self.post("eligibility/review", {
                "edition_id": eid, "action": "approve",
            }).status_code, 200,
        )
        self.assertEqual(
            set(EditionCandidate.objects.filter(
                edition_id=eid
            ).values_list("song_id", flat=True)), {self.song.pk, new_song.pk}
        )

    def test_existing_staff_account_can_be_assigned_without_changing_password(self):
        existing = get_user_model().objects.create_user(
            username="existing-national", password="Old-Password-Testing!"
        )
        AccountProfile.objects.update_or_create(
            user=existing, defaults={"station": "national_fm"}
        )
        self.assertEqual(self.post(
            "presenters/assign", {
                "user_id": existing.pk, "chart_show_id": self.saturday.pk,
            }
        ).status_code, 200)
        self.assertTrue(existing.check_password("Old-Password-Testing!"))
        self.assertEqual(
            PresenterAssignment.objects.get(user=existing).chart_show_id,
            self.saturday.pk,
        )
        client = Client()
        client.force_login(existing)
        self.assertEqual(
            client.get("/api/workspace/overview").json()["chart_show_id"],
            self.saturday.pk,
        )

    def test_locked_edition_cannot_be_reapproved_after_votes_start(self):
        eid = self.create_edition()
        self.assertEqual(self.add_candidate(eid).status_code, 200)
        self.assertEqual(self.post(
            "eligibility/submit", {"edition_id": eid}
        ).status_code, 200)
        self.raw_vote(self.song, "already-voted")
        self.assertEqual(self.post(
            "eligibility/review", {"edition_id": eid, "action": "approve"}
        ).status_code, 400)
        self.saturday.refresh_from_db()
        self.assertFalse(self.saturday.eligibility_enforced)
