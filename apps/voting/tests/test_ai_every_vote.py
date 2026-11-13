"""AI-first processing: every valid vote must pass the model before chart tallying."""
from unittest.mock import patch

from django.test import TestCase, override_settings

from apps.voting.models import (
    CleanedSong, CleanedSongTally, MatchKeyMapping, RawVote, RawSongTally,
    LLMDecisionLog, VoteMatchingDiagnostic,
)
from apps.voting.pipeline import process_vote, review_song, rebuild_tallies
from apps.voting.services import VotingService


@override_settings(AI_ALL_VOTES=True, AUTO_AI_MATCH=True, ALLOW_REPEAT_SONG=True)
class EveryVoteAiTests(TestCase):
    def song(self, station="radio_zimbabwe", title="Morning Sunshine"):
        return CleanedSong.objects.create(
            station=station, artist="Example Artist", title=title,
            canonical_name=f"Example Artist - {title}", status="verified"
        )

    def vote(self, text="Example Artist - Morning Sunshine",
             listener="listener", station="radio_zimbabwe"):
        response = VotingService("whatsapp", listener, station).handle_incoming_text(text)
        self.assertIn("Submission received", response)
        return RawVote.objects.filter(user__user_ref=listener, station=station).latest("pk")

    @staticmethod
    def approval(song):
        return {"matched": True, "matched_song_id": song.pk,
                "confidence": "high", "reasoning": "Clear artist and song."}

    def test_exact_match_is_not_counted_until_ai_approves(self):
        song = self.song()
        vote = self.vote()
        self.assertEqual(vote.ai_status, "awaiting")
        self.assertFalse(CleanedSongTally.objects.exists())
        with patch("apps.voting.ai.match_vote_with_llm",
                   return_value=self.approval(song)) as ai:
            process_vote(vote)
            process_vote(vote)  # repeated job is idempotent
        self.assertEqual(ai.call_count, 1)
        self.assertEqual(RawVote.objects.get(pk=vote.pk).ai_status, "approved")
        self.assertEqual(CleanedSongTally.objects.get(cleaned_song=song).count, 1)
        self.assertTrue(LLMDecisionLog.objects.get().was_applied)

    def test_trace_records_actual_openai_request_without_raw_prompt(self):
        song = self.song()
        vote = self.vote()
        with patch(
            "apps.voting.ai.call_openai_api",
            return_value=(
                '{"matched":true,"matched_song_id":' + str(song.pk) +
                ',"confidence":"high","decision":"MATCH","reasoning":"Verified"}'
            ),
        ):
            process_vote(vote)
        diagnostic = VoteMatchingDiagnostic.objects.get(vote=vote)
        self.assertEqual(diagnostic.decision, "approved")
        self.assertEqual(diagnostic.verification_method, "openai")
        self.assertEqual(diagnostic.api_status, "success")
        self.assertGreaterEqual(diagnostic.api_latency_ms, 0)
        self.assertEqual(diagnostic.matched_song, song)

    def test_existing_mapping_does_not_bypass_ai_or_count_unapproved_vote(self):
        song = self.song()
        first = self.vote(listener="first")
        with patch("apps.voting.ai.match_vote_with_llm",
                   return_value=self.approval(song)) as ai:
            process_vote(first)
        second = self.vote(listener="second")
        self.assertEqual(RawSongTally.objects.get().count, 2)
        self.assertEqual(CleanedSongTally.objects.get().count, 1)
        with patch("apps.voting.ai.match_vote_with_llm",
                   return_value={"matched": False, "matched_song_id": None,
                                 "confidence": "none", "reasoning": "Ambiguous"}) as ai:
            process_vote(second)
            ai.assert_called_once()
        self.assertEqual(RawVote.objects.get(pk=second.pk).ai_status, "review")
        self.assertEqual(CleanedSongTally.objects.get().count, 1)
        rebuild_tallies("radio_zimbabwe")
        self.assertEqual(CleanedSongTally.objects.get().count, 1)

    def test_unknown_song_is_checked_and_not_auto_added_as_verified(self):
        vote = self.vote(text="New Artist - Unreleased Song")
        with patch("apps.voting.ai.match_vote_with_llm",
                   return_value={"matched": False, "matched_song_id": None,
                                 "confidence": "none", "reasoning": "No candidates"}) as ai:
            process_vote(vote)
        self.assertEqual(ai.call_count, 1)
        self.assertEqual(ai.call_args.args[2], [])
        self.assertEqual(RawVote.objects.get(pk=vote.pk).ai_status, "review")
        self.assertEqual(MatchKeyMapping.objects.get().cleaned_song.status, "pending")
        self.assertFalse(CleanedSongTally.objects.exists())
        pending = MatchKeyMapping.objects.get().cleaned_song
        review_song("radio_zimbabwe", None, pending.pk, "verified")
        self.assertEqual(RawVote.objects.get(pk=vote.pk).ai_status, "approved")
        self.assertEqual(CleanedSongTally.objects.get().count, 1)

    def test_api_outage_keeps_vote_unverified_for_retry(self):
        self.song()
        vote = self.vote()
        with patch("apps.voting.ai.match_vote_with_llm",
                   return_value={"reasoning": "LLM error: Timeout"}):
            with self.assertRaises(RuntimeError):
                process_vote(vote)
        self.assertEqual(RawVote.objects.get(pk=vote.pk).ai_status, "awaiting")
        self.assertFalse(MatchKeyMapping.objects.exists())
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_openai_string_id_and_confidence_are_normalised_before_approval(self):
        song = self.song()
        vote = self.vote()
        with patch(
            "apps.voting.ai.call_openai_api",
            return_value=(
                '{"matched":true,"matched_song_id":"' + str(song.pk) +
                '","confidence":"High","reasoning":"Exact catalogue identity"}'
            ),
        ) as openai:
            process_vote(vote)
        openai.assert_called_once()
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "approved")
        self.assertEqual(CleanedSongTally.objects.get(cleaned_song=song).count, 1)

    def test_foreign_station_song_id_never_counts(self):
        other = self.song(station="national_fm")
        vote = self.vote(text="Example Artist - Morning Sunshine")
        with patch("apps.voting.ai.match_vote_with_llm",
                   return_value=self.approval(other)) as ai:
            process_vote(vote)
        self.assertNotIn(other.pk, [r["id"] for r in ai.call_args.args[2]])
        self.assertEqual(RawVote.objects.get(pk=vote.pk).ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_invalid_message_is_rejected_without_ai_job(self):
        response = VotingService("whatsapp", "invalid").handle_incoming_text("123456")
        self.assertNotIn("Submission received", response)
        self.assertFalse(RawVote.objects.exists())
