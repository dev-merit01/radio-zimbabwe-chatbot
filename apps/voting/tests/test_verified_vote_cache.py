"""Verified spelling cache: no AI repetition, no shortcut around vote rules.

Cache entries are previous OPENAI approvals, never raw fuzzy scores. Every
new submission remains a separate immutable RawVote, idempotent and subject
to the station/chart/listener vote policies.
"""
from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.utils import timezone

from apps.voting.catalogue_matching import automatic_candidate, ranked_candidates
from apps.voting.models import (
    CleanedSong, CleanedSongTally, LLMDecisionLog, RawVote,
    VerifiedVoteCache,
)
from apps.voting.pipeline import process_vote
from apps.voting.services import VotingService


@override_settings(
    AI_ALL_VOTES=True,
    AUTO_AI_MATCH=True,
    AI_MATCH_CACHE_ENABLED=True,
    AI_MATCH_CACHE_SECONDS=600,
    ALLOW_REPEAT_SONG=True,
)
class VerifiedCacheTests(TestCase):
    def setUp(self):
        self.song = CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Atenda Chinx", title="My Man",
            canonical_name="Atenda Chinx - My Man", status="verified",
        )

    def submit(self, message, listener, station="radio_zimbabwe"):
        response = VotingService("whatsapp", listener, station).handle_incoming_text(message)
        self.assertIn("Submission received", response)
        return RawVote.objects.filter(
            station=station, user__user_ref=listener,
        ).latest("pk")

    @staticmethod
    def model_reply(song):
        return {
            "matched": True, "matched_song_id": song.pk,
            "confidence": "high", "reasoning": "Unique artist and song identity",
        }

    def test_unique_lead_artist_and_title_without_separator(self):
        ranked = ranked_candidates("radio_zimbabwe", "Unknown Artist", "Atenda My Man")
        self.assertEqual(ranked[0][3].pk, self.song.pk)
        self.assertEqual(
            automatic_candidate(ranked, "Unknown Artist", "Atenda My Man"),
            self.song,
        )

    def test_repeat_verified_typo_does_not_call_openai_again(self):
        first = self.submit("Atenda My Man", "listener-one")
        with patch("apps.voting.ai.match_vote_with_llm", return_value=self.model_reply(self.song)) as ai:
            process_vote(first)
        ai.assert_called_once()
        self.assertEqual(first.ai_status, "awaiting")  # stale instance; database has approved status
        self.assertEqual(
            RawVote.objects.get(pk=first.pk).ai_status, "approved"
        )
        cache = VerifiedVoteCache.objects.get(
            station="radio_zimbabwe", normalised_text="atenda my man",
        )
        self.assertEqual(cache.song_id, self.song.pk)

        second = self.submit("Atenda. My Man", "listener-two")
        with patch("apps.voting.ai.match_vote_with_llm") as ai:
            process_vote(second)
        ai.assert_not_called()
        second.refresh_from_db()
        self.assertEqual(second.ai_status, "approved")
        self.assertEqual(
            CleanedSongTally.objects.get(cleaned_song=self.song).count, 2
        )
        self.assertEqual(
            LLMDecisionLog.objects.filter(input_type="cached_verified_vote").count(), 1
        )

    def test_competing_artist_invalidates_cached_abbreviation(self):
        first = self.submit("Atenda My Man", "first")
        with patch("apps.voting.ai.match_vote_with_llm", return_value=self.model_reply(self.song)):
            process_vote(first)
        CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Atenda Soul", title="My Man",
            canonical_name="Atenda Soul - My Man", status="verified",
        )
        second = self.submit("Atenda My Man", "second")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={"matched": False, "confidence": "none", "reasoning": "Ambiguous"},
        ) as ai:
            process_vote(second)
        ai.assert_called_once()
        second.refresh_from_db()
        self.assertEqual(second.ai_status, "review")
        self.assertEqual(CleanedSongTally.objects.get(cleaned_song=self.song).count, 1)

    def test_renamed_song_invalidates_cached_match(self):
        first = self.submit("Atenda My Man", "one")
        with patch("apps.voting.ai.match_vote_with_llm", return_value=self.model_reply(self.song)):
            process_vote(first)
        self.song.title = "My Man New Version"
        self.song.save(update_fields=["title"])
        second = self.submit("Atenda My Man", "two")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={"matched": False, "confidence": "none", "reasoning": "Different version"},
        ) as ai:
            process_vote(second)
        ai.assert_called_once()
        self.assertEqual(RawVote.objects.get(pk=second.pk).ai_status, "review")

    def test_expired_cache_falls_back_to_openai(self):
        first = self.submit("Atenda My Man", "one")
        with patch("apps.voting.ai.match_vote_with_llm", return_value=self.model_reply(self.song)):
            process_vote(first)
        VerifiedVoteCache.objects.filter(
            station="radio_zimbabwe",
        ).update(expires_at=timezone.now() - timedelta(minutes=1))
        second = self.submit("Atenda My Man", "two")
        with patch("apps.voting.ai.match_vote_with_llm", return_value=self.model_reply(self.song)) as ai:
            process_vote(second)
        ai.assert_called_once()
        self.assertEqual(CleanedSongTally.objects.get(cleaned_song=self.song).count, 2)

    def test_cached_spelling_cannot_cross_station(self):
        first = self.submit("Atenda My Man", "one")
        with patch("apps.voting.ai.match_vote_with_llm", return_value=self.model_reply(self.song)):
            process_vote(first)
        other = CleanedSong.objects.create(
            station="national_fm", artist="Atenda Chinx", title="My Man",
            canonical_name="Atenda Chinx - My Man", status="verified",
        )
        second = self.submit("Atenda My Man", "two", "national_fm")
        with patch("apps.voting.ai.match_vote_with_llm", return_value=self.model_reply(other)) as ai:
            process_vote(second)
        ai.assert_called_once()
        self.assertEqual(RawVote.objects.get(pk=second.pk).ai_status, "approved")
        self.assertEqual(
            CleanedSongTally.objects.get(cleaned_song=other).count, 1
        )

    def test_unverified_model_decision_is_never_cached(self):
        vote = self.submit("Atenda My Man", "ambiguous")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={"matched": False, "confidence": "none", "reasoning": "No match"},
        ):
            process_vote(vote)
        self.assertFalse(VerifiedVoteCache.objects.exists())
        self.assertEqual(RawVote.objects.get(pk=vote.pk).ai_status, "review")

    @override_settings(ALLOW_REPEAT_SONG=False)
    def test_duplicate_listener_does_not_gain_extra_count_from_cache(self):
        vote = self.submit("Atenda My Man", "same")
        with patch("apps.voting.ai.match_vote_with_llm", return_value=self.model_reply(self.song)):
            process_vote(vote)
        response = VotingService("whatsapp", "same").handle_incoming_text("Atenda My Man")
        self.assertIn("already voted", response)
        self.assertEqual(RawVote.objects.count(), 1)
        self.assertEqual(
            CleanedSongTally.objects.get(cleaned_song=self.song).count, 1
        )

    @override_settings(AI_MATCH_CACHE_ENABLED=False)
    def test_feature_flag_preserves_existing_all_votes_through_ai(self):
        first = self.submit("Atenda My Man", "one")
        second = self.submit("Atenda My Man", "two")
        with patch("apps.voting.ai.match_vote_with_llm", return_value=self.model_reply(self.song)) as ai:
            process_vote(first)
            process_vote(second)
        self.assertEqual(ai.call_count, 2)
        self.assertFalse(VerifiedVoteCache.objects.exists())
