"""Regression tests for long title-only radio votes such as Ndakainamatira.

No mocked high-confidence model response may bypass candidate uniqueness,
station isolation, duplicate protection or recording-version rules.
"""
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase, override_settings

from apps.voting.catalogue_matching import (
    automatic_candidate, distinctive_title_prefix, ranked_candidates,
)
from apps.voting.models import CleanedSong, CleanedSongTally, RawVote
from apps.voting.pipeline import process_vote
from apps.voting.services import VotingService


@override_settings(AI_ALL_VOTES=True, AUTO_AI_MATCH=True, ALLOW_REPEAT_SONG=True)
class DistinctiveTitleOnlyTests(TestCase):
    def setUp(self):
        self.song = CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Gracewave.Zw",
            title="Ndakainamatira Nyaya Iyoyi",
            canonical_name="Gracewave.Zw - Ndakainamatira Nyaya Iyoyi",
            status="verified",
        )

    def submit(self, text, number):
        response = VotingService("whatsapp", number).handle_incoming_text(text)
        self.assertIn("Submission received", response)
        return RawVote.objects.filter(user__user_ref=number).latest("pk")

    def run_ai(self, vote, matched=True):
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": matched,
                "matched_song_id": self.song.pk if matched else None,
                "confidence": "high" if matched else "none",
                "reasoning": "Unique catalogue title prefix" if matched else "Unresolved",
            },
        ) as mock:
            process_vote(vote)
        mock.assert_called_once()
        return mock.call_args.args[2]

    def test_unique_long_first_word_is_ai_confirmed_and_counted(self):
        vote = self.submit("Ndakainamatira", "long-title")
        self.assertEqual(vote.artist_normalized, "unknown artist")
        candidate_list = self.run_ai(vote)
        self.assertEqual([x["id"] for x in candidate_list], [self.song.pk])
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "approved")
        self.assertEqual(
            CleanedSongTally.objects.get(cleaned_song=self.song).count, 1
        )
        self.assertFalse(CleanedSong.objects.filter(status="pending").exists())
        process_vote(vote)
        self.assertEqual(
            CleanedSongTally.objects.get(cleaned_song=self.song).count, 1
        )

    def test_distinctive_prefix_is_retrievable_in_both_modes(self):
        for exhaustive in (True, False):
            with self.subTest(exhaustive=exhaustive):
                ranked = ranked_candidates(
                    "radio_zimbabwe", "Unknown Artist", "Ndakainamatira",
                    exhaustive=exhaustive,
                )
                self.assertEqual(ranked[0][3].pk, self.song.pk)
                self.assertEqual(
                    automatic_candidate(ranked, "Unknown Artist", "Ndakainamatira"),
                    self.song,
                )

    def test_shared_long_prefix_is_ambiguous_even_if_llm_is_confident(self):
        CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Another Artist",
            title="Ndakainamatira Nyaya Yangu",
            canonical_name="Another Artist - Ndakainamatira Nyaya Yangu",
            status="verified",
        )
        ranked = ranked_candidates("radio_zimbabwe", "Unknown Artist", "Ndakainamatira")
        self.assertEqual(len(ranked), 2)
        self.assertIsNone(
            automatic_candidate(ranked, "Unknown Artist", "Ndakainamatira")
        )
        vote = self.submit("Ndakainamatira", "shared-title")
        self.run_ai(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_short_common_title_prefix_cannot_auto_approve(self):
        CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Other Artist",
            title="Bhura Dhanzi", canonical_name="Other Artist - Bhura Dhanzi",
            status="verified",
        )
        ranked = ranked_candidates("radio_zimbabwe", "Unknown Artist", "Bhura")
        self.assertIsNone(
            automatic_candidate(ranked, "Unknown Artist", "Bhura")
        )
        self.assertFalse(distinctive_title_prefix("Bhura", "Bhura Dhanzi"))

    def test_wrong_version_or_station_does_not_qualify(self):
        self.assertFalse(
            ranked_candidates("radio_zimbabwe", "Unknown Artist", "Ndakainamatira Live")
        )
        self.assertFalse(
            ranked_candidates("national_fm", "Unknown Artist", "Ndakainamatira")
        )

    def test_pending_historical_vote_retries_and_maps_once(self):
        vote = self.submit("Ndakainamatira", "pending-title")
        self.run_ai(vote, matched=False)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertTrue(CleanedSong.objects.filter(status="pending").exists())
        with patch("apps.voting.management.commands.retry_catalogue_votes.timezone.now") as now:
            from django.utils import timezone
            now.return_value = timezone.now()
            call_command("retry_catalogue_votes", stdout=StringIO())
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "awaiting")
        self.run_ai(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "approved")
        self.assertEqual(
            CleanedSongTally.objects.get(cleaned_song=self.song).count, 1
        )
        self.assertFalse(CleanedSong.objects.filter(status="pending").exists())
        process_vote(vote)
        self.assertEqual(
            CleanedSongTally.objects.get(cleaned_song=self.song).count, 1
        )
