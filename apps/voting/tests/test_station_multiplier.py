"""Station score multiplier: transparent and non-destructive."""
import json
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from apps.accounts.models import AccountProfile, PresenterAssignment
from apps.charts.publishing import publish_edition
from apps.voting.models import ChartShow, RawVote, ReviewAudit, StationVotingConfig, VoteJob
from apps.voting.pipeline import review_song
from apps.voting.services import VotingService
from apps.voting.worker import run_one


@override_settings(SECURE_SSL_REDIRECT=False, AI_ALL_VOTES=False, AUTO_AI_MATCH=False)
class StationMultiplierTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser(
            username="weighted-admin", password="Testing-Password-473!"
        )
        self.client.force_login(self.admin)
        self.client.post("/accounts/switch-station/", {"station": "national_fm"})
        self.show = ChartShow.objects.create(
            station="national_fm", name="Test Chart", code="NTEST",
            kind="weekly", schedule_mode="manual", is_enabled=True, manual_open=True,
        )

    def save_multiplier(self, value):
        return self.client.post(
            "/api/workspace/shows/multiplier",
            json.dumps({"multiplier": value}), content_type="application/json",
        )

    def approve_vote(self, vote_date=None):
        VotingService("whatsapp", "one-listener", "national_fm").handle_incoming_text(
            "Sample Artist - Clear Song", vote_date=vote_date,
            chart_show=self.show, submitted_code="NTEST",
        )
        self.assertTrue(run_one(VoteJob))
        from apps.voting.models import CleanedSong
        song = CleanedSong.objects.get(
            station="national_fm", canonical_name="Sample Artist - Clear Song"
        )
        review_song("national_fm", self.admin, song.pk, "verified")
        return song

    def test_multiplier_default_and_live_score_separate_from_real_votes(self):
        data = self.client.get("/api/workspace/shows").json()
        self.assertEqual(data["vote_multiplier"], 1)
        self.assertEqual(self.save_multiplier(3).status_code, 200)
        song = self.approve_vote()
        result = self.client.get("/api/chart/today", {"show_id": self.show.id})
        self.assertEqual(result.status_code, 200)
        payload = result.json()
        self.assertEqual(payload["vote_multiplier"], 3)
        self.assertEqual(payload["total_votes"], 1)
        self.assertEqual(payload["total_score"], 3)
        self.assertEqual(payload["top100"][0]["count"], 1)
        self.assertEqual(payload["top100"][0]["score"], 3)
        self.assertEqual(payload["top100"][0]["title"], song.title)
        self.assertEqual(RawVote.objects.count(), 1)
        summary = self.client.get("/api/workspace/overview", {
            "show_id": self.show.id
        }).json()
        self.assertEqual(summary["verified_votes"], 1)
        self.assertEqual(summary["verified_score"], 3)
        csv = self.client.get("/api/workspace/export", {"show_id": self.show.id})
        self.assertEqual(csv.status_code, 200)
        self.assertIn("Weighted score (x3)", csv.content.decode("utf-8-sig"))

    def test_multiplier_restricted_to_superadmin_and_selected_station(self):
        reader = get_user_model().objects.create_user(
            username="weighted-reader", password="Testing-Reader-874!"
        )
        AccountProfile.objects.update_or_create(
            user=reader, defaults={"station": "national_fm"}
        )
        client = Client()
        client.force_login(reader)
        response = client.post(
            "/api/workspace/shows/multiplier",
            json.dumps({"multiplier": 10}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.save_multiplier(4).status_code, 200)
        self.assertEqual(
            StationVotingConfig.objects.get(station="national_fm").vote_multiplier, 4
        )
        self.assertEqual(
            StationVotingConfig.objects.get(station="power_fm").vote_multiplier, 1
        )
        self.client.post("/accounts/switch-station/", {"station": "power_fm"})
        self.assertEqual(self.save_multiplier(2).status_code, 200)
        self.assertEqual(
            StationVotingConfig.objects.get(station="national_fm").vote_multiplier, 4
        )
        self.assertEqual(
            StationVotingConfig.objects.get(station="power_fm").vote_multiplier, 2
        )
        self.assertEqual(
            ReviewAudit.objects.filter(
                station="national_fm", action="vote_multiplier"
            ).count(), 1
        )


    def test_presenter_sees_real_votes_and_movement_but_not_scoring_details(self):
        """Station scoring is admin-only in every endpoint and CSV export."""
        self.assertEqual(self.save_multiplier(4).status_code, 200)
        last_saturday = timezone.localdate() - timedelta(
            days=(timezone.localdate().weekday() - 5) % 7
        )
        self.approve_vote(vote_date=last_saturday)
        chart, created = publish_edition(
            "national_fm", last_saturday, self.admin,
            20, chart_show=self.show,
        )
        self.assertTrue(created)

        presenter = get_user_model().objects.create_user(
            username="movement-presenter", password="Test-Presenter-8173!"
        )
        AccountProfile.objects.update_or_create(
            user=presenter, defaults={"station": "national_fm"}
        )
        PresenterAssignment.objects.create(user=presenter, chart_show=self.show)
        client = Client()
        client.force_login(presenter)

        for endpoint in (
            "/api/workspace/shows",
            f"/api/workspace/overview?show_id={self.show.pk}",
        ):
            with self.subTest(endpoint=endpoint):
                response = client.get(endpoint)
                self.assertEqual(response.status_code, 200, response.content)
                data = response.json()
                self.assertNotIn("vote_multiplier", data)
                self.assertNotIn("verified_score", data)
                self.assertIn("station", data)
        live = client.get("/api/chart/today", {"show_id": self.show.pk})
        self.assertEqual(live.status_code, 200, live.content)
        live_data = live.json()
        self.assertNotIn("vote_multiplier", live_data)
        self.assertNotIn("total_score", live_data)
        self.assertTrue(all("score" not in row for row in live_data["top100"]))
        self.assertIn("comparison", live_data)

        listings = client.get("/api/chart/archives", {
            "show_id": self.show.pk, "year": chart.year,
        })
        self.assertEqual(listings.status_code, 200, listings.content)
        matches = [item for item in listings.json()["charts"]
                   if item["id"] == chart.pk]
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["total_votes"], 1)
        self.assertNotIn("total_score", matches[0])
        self.assertNotIn("vote_multiplier", matches[0])

        archived = client.get(f"/api/chart/{chart.pk}")
        self.assertEqual(archived.status_code, 200, archived.content)
        archive = archived.json()
        self.assertEqual(archive["chart"]["total_votes"], 1)
        self.assertNotIn("vote_multiplier", archive["chart"])
        self.assertNotIn("total_score", archive["chart"])
        self.assertEqual(archive["entries"][0]["count"], 1)
        self.assertNotIn("score", archive["entries"][0])
        self.assertIn("movement_status", archive["entries"][0])

        exported = client.get("/api/workspace/export", {"archive": chart.pk})
        self.assertEqual(exported.status_code, 200, exported.content)
        text = exported.content.decode("utf-8-sig")
        self.assertIn("Verified votes", text)
        self.assertIn("Previous position", text)
        self.assertNotIn("Weighted score", text)
        self.assertNotIn("x4", text)

        self.assertIn("vote_multiplier", self.client.get(
            "/api/workspace/shows"
        ).json())
        admin_archive = self.client.get(f"/api/chart/{chart.pk}").json()
        self.assertEqual(admin_archive["chart"]["vote_multiplier"], 4)
        self.assertEqual(admin_archive["entries"][0]["score"], 4)
        self.assertIn("Weighted score (x4)", self.client.get(
            "/api/workspace/export", {"archive": chart.pk}
        ).content.decode("utf-8-sig"))

    def test_reject_invalid_multiplier_without_changing_settings(self):
        for invalid in (0, 11, -1, 1.5, True, "5", None):
            with self.subTest(invalid=invalid):
                result = self.save_multiplier(invalid)
                self.assertEqual(result.status_code, 400)
                self.assertEqual(
                    StationVotingConfig.objects.get(
                        station="national_fm"
                    ).vote_multiplier, 1
                )

    def test_published_chart_uses_frozen_score_even_after_setting_changes(self):
        last_saturday = timezone.localdate() - timedelta(
            days=(timezone.localdate().weekday() - 5) % 7
        )
        self.assertEqual(self.save_multiplier(2).status_code, 200)
        self.approve_vote(vote_date=last_saturday)
        chart, created = publish_edition(
            "national_fm", last_saturday, self.admin,
            20, chart_show=self.show,
        )
        self.assertTrue(created)
        self.assertEqual(chart.vote_multiplier, 2)
        self.assertEqual(chart.total_votes, 1)
        archive = self.client.get(f"/api/chart/{chart.pk}").json()
        self.assertEqual(archive["chart"]["vote_multiplier"], 2)
        self.assertEqual(archive["chart"]["total_score"], 2)
        self.assertEqual(archive["entries"][0]["count"], 1)
        self.assertEqual(archive["entries"][0]["score"], 2)
        self.assertEqual(self.save_multiplier(5).status_code, 200)
        archive = self.client.get(f"/api/chart/{chart.pk}").json()
        self.assertEqual(archive["chart"]["vote_multiplier"], 2)
        self.assertEqual(archive["entries"][0]["score"], 2)
        self.assertEqual(RawVote.objects.count(), 1)
