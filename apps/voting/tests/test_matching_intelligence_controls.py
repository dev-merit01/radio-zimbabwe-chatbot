"""Admin matching controls must be secure and cannot weaken vote safety gates."""
import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from apps.voting.matching_controls import options_for_station
from apps.voting.models import MatchingControl, ReviewAudit


@override_settings(SECURE_SSL_REDIRECT=False, OPENAI_MODEL="gpt-6-luna")
class AdminMatchingIntelligenceTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser(
            username="matching-control-admin",
            password="Test-Only-AI-Controls!2468",
        )
        self.client.force_login(self.admin)

    def test_read_is_private_and_reports_live_defaults(self):
        response = self.client.get("/api/workspace/intelligence")
        self.assertEqual(response.status_code, 200, response.content)
        data = response.json()
        self.assertEqual(data["config"]["shortlist_limit"], 16)
        self.assertEqual(data["config"]["retrieval_min_artist"], 0.68)
        self.assertEqual(data["config"]["retrieval_min_title"], 0.66)
        self.assertEqual(data["metrics"]["total"], 0)
        self.client.logout()
        self.assertEqual(self.client.get("/api/workspace/intelligence").status_code, 401)

    def test_presenter_cannot_read_or_modify_matching_settings(self):
        presenter = get_user_model().objects.create_user(
            username="unauthorised-matching-view",
            password="Test-Only-Presenter!2468",
        )
        self.client.force_login(presenter)
        self.assertEqual(self.client.get("/api/workspace/intelligence").status_code, 403)
        self.assertEqual(self.client.post(
            "/api/workspace/intelligence/save",
            json.dumps({"shortlist_limit": 5}), content_type="application/json",
        ).status_code, 403)
        self.assertFalse(MatchingControl.objects.exists())

    def test_admin_tunes_station_retrieval_without_disabling_ai(self):
        response = self.client.post(
            "/api/workspace/intelligence/save",
            json.dumps({
                "model_override": "gpt-4o-mini", "shortlist_limit": 22,
                "retrieval_min_artist": 0.60, "retrieval_min_title": 0.58,
                "cache_mode": "off",
            }), content_type="application/json",
        )
        self.assertEqual(response.status_code, 200, response.content)
        station = response.json()["config"]
        self.assertEqual(station["model"], "gpt-4o-mini")
        self.assertEqual(station["shortlist_limit"], 22)
        self.assertFalse(station["cache_enabled"])
        self.assertEqual(station["retrieval_min_artist"], 0.60)
        self.assertEqual(station["retrieval_min_title"], 0.58)
        self.assertTrue(ReviewAudit.objects.filter(action="matching_settings").exists())

    def test_reject_unsafe_or_malformed_overrides(self):
        standard = {
            "model_override": "", "shortlist_limit": 16,
            "retrieval_min_artist": 0.68, "retrieval_min_title": 0.66,
            "cache_mode": "inherit",
        }
        for variation in (
            {"model_override": "unapproved-model"},
            {"shortlist_limit": 200},
            {"shortlist_limit": True},
            {"retrieval_min_artist": 0.40},
            {"retrieval_min_title": float("inf")},
            {"cache_mode": "bypass"},
        ):
            with self.subTest(variation=str(variation)):
                payload = {**standard, **variation}
                response = self.client.post(
                    "/api/workspace/intelligence/save", json.dumps(payload),
                    content_type="application/json",
                )
                self.assertEqual(response.status_code, 400, response.content)
        self.assertFalse(MatchingControl.objects.exists())

    @override_settings(OPENAI_API_KEY="fake-test-key")
    def test_non_reasoning_model_request_does_not_send_reasoning_effort(self):
        from apps.voting.ai import call_openai_api
        with patch("apps.voting.ai.requests.post") as sender:
            sender.return_value.json.return_value = {
                "choices": [{"message": {"content": '{"ok": true}'}}],
            }
            sender.return_value.raise_for_status.return_value = None
            self.assertEqual(
                call_openai_api("system", "user", model="gpt-4o-mini"),
                '{"ok": true}',
            )
            outgoing = sender.call_args.kwargs["json"]
            self.assertEqual(outgoing["model"], "gpt-4o-mini")
            self.assertNotIn("reasoning_effort", outgoing)

    def test_multiple_openai_models_are_available_in_admin(self):
        response = self.client.get("/api/workspace/intelligence")
        choices = {row["value"] for row in response.json()["model_options"]}
        self.assertGreaterEqual(len(choices), 8)
        self.assertTrue({
            "gpt-6-luna", "gpt-6.1-terra", "gpt-6.1-sol",
            "gpt-4.1-mini", "gpt-4o-mini",
        }.issubset(choices))
        data = {
            "model_override": "gpt-6.1-terra", "shortlist_limit": 16,
            "retrieval_min_artist": 0.68, "retrieval_min_title": 0.66,
            "cache_mode": "inherit",
        }
        saved = self.client.post(
            "/api/workspace/intelligence/save", json.dumps(data),
            content_type="application/json",
        )
        self.assertEqual(saved.status_code, 200, saved.content)
        self.assertEqual(saved.json()["config"]["model"], "gpt-6.1-terra")

    @override_settings(OPENAI_API_KEY="fake-test-key")
    def test_gpt_4_1_model_does_not_send_unsupported_reasoning_effort(self):
        from apps.voting.ai import call_openai_api
        with patch("apps.voting.ai.requests.post") as sender:
            sender.return_value.json.return_value = {
                "choices": [{"message": {"content": '{"ok": true}'}}],
            }
            sender.return_value.raise_for_status.return_value = None
            call_openai_api("system", "user", model="gpt-4.1-mini")
            self.assertNotIn("reasoning_effort", sender.call_args.kwargs["json"])

    def test_default_station_controls_do_not_impact_other_stations(self):
        MatchingControl.objects.create(
            station="radio_zimbabwe", model_override="gpt-4o-mini",
            shortlist_limit=24, cache_mode="off",
        )
        self.assertEqual(options_for_station("radio_zimbabwe")["shortlist_limit"], 24)
        self.assertEqual(options_for_station("power_fm")["shortlist_limit"], 16)
        self.assertEqual(options_for_station("power_fm")["model"], "gpt-6-luna")
