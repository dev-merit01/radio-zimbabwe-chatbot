"""Regression coverage for station-wide code safety and historical hold metadata."""
from importlib import import_module

from django.apps import apps
from django.test import TestCase
from django.utils import timezone

from apps.accounts.models import Station
from apps.charts.routing import route_vote, station_requires_codes
from apps.voting.models import ChartShow, StationVotingConfig, UnassignedVote


class StationCodeRoutingSafetyTests(TestCase):
    def test_every_configured_station_requires_codes_even_if_flag_is_off(self):
        for station, _label in Station.choices:
            show = ChartShow.objects.filter(
                station=station, kind__in=("weekly", "year_end"),
            ).exclude(code="").first()
            if show is None:
                continue
            with self.subTest(station=station):
                StationVotingConfig.objects.update_or_create(
                    station=station, defaults={"require_codes": False},
                )
                self.assertTrue(station_requires_codes(station))
                resolved, _, _, reason = route_vote(
                    station, "Test Singer - Song Title",
                )
                self.assertIsNone(resolved)
                self.assertEqual(reason, "missing_or_unknown_code")
                target, _, code, error = route_vote(
                    station, f"{show.code.lower()} Test Singer - Song Title",
                )
                self.assertIsNone(target)
                self.assertEqual(code, show.code)
                self.assertEqual(error, "show_closed")

    def test_one_legacy_only_installation_can_still_process_historical(self):
        station = "uncoded_test_station"
        StationVotingConfig.objects.create(station=station, require_codes=False)
        self.assertFalse(station_requires_codes(station))
        show, text, code, error = route_vote(
            station, "Test Singer - Song Title"
        )
        self.assertEqual(show.kind, "legacy")
        self.assertEqual(text, "Test Singer - Song Title")
        self.assertEqual(code, "")
        self.assertIsNone(error)

    def test_prior_closed_show_hold_gets_intended_programme_metadata_only(self):
        show = ChartShow.objects.get(station="radio_zimbabwe", code="GP20")
        held = UnassignedVote.objects.create(
            station="radio_zimbabwe", provider="bird", message_id="existing-gp20",
            sender="previous-listener", text="GP20 Sugar Sugar - Mudododo",
            reason="show_closed", received_at=timezone.now(),
        )
        original_state = held.state
        migration = import_module(
            "apps.voting.migrations.0027_record_intended_chart_on_held_votes"
        )
        migration.backfill_intended_shows(apps, None)
        held.refresh_from_db()
        self.assertEqual(held.intended_show_id, show.pk)
        self.assertEqual(held.submitted_code, "GP20")
        self.assertEqual(held.vote_text, "Sugar Sugar - Mudododo")
        self.assertEqual(held.state, original_state)
        self.assertIsNone(held.resolved_show_id)
        self.assertFalse(show.raw_votes.exists())
