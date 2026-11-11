"""Zimbabwe listener regression: clipped stage name plus title transcription.

This verifies *general* string-pattern handling and deliberately conflicting
catalogue entries, not a hard-coded Ngende/Kwadabuka song mapping.
"""
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase, override_settings

from apps.voting.catalogue_matching import (
    artist_similarity, automatic_candidate, distinctive_single_word_typo,
    ranked_candidates, shortened_artist_suffix,
)
from apps.voting.models import (
    CleanedSong, CleanedSongTally, MatchKeyMapping, RawVote,
)
from apps.voting.pipeline import process_vote
from apps.voting.services import VotingService


@override_settings(
    AI_ALL_VOTES=True, AUTO_AI_MATCH=True, ALLOW_REPEAT_SONG=True,
    AI_MATCH_CACHE_ENABLED=False, AI_VECTOR_ENABLED=False,
)
class ClippedArtistAndTitleTypoTests(TestCase):
    def setUp(self):
        self.song = CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Hulengende",
            title="Kukwadabuka",
            canonical_name="Hulengende - Kukwadabuka", status="verified",
        )

    def vote(self, raw, listener="listener"):
        result = VotingService(
            "whatsapp", listener, "radio_zimbabwe"
        ).handle_incoming_text(raw)
        self.assertIn("Submission received", result)
        return RawVote.objects.filter(
            station="radio_zimbabwe", user__user_ref=listener,
        ).latest("pk")

    def ai_decide(self, vote, song=None):
        song = song or self.song
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": True,
                "matched_song_id": song.pk,
                "confidence": "high",
                "reasoning": "Distinctive artist and title pair",
            },
        ) as ai:
            process_vote(vote)
        ai.assert_called_once()
        return ai.call_args.args[2]

    def test_shortened_stage_suffix_is_evidence_not_an_identity_by_itself(self):
        self.assertTrue(shortened_artist_suffix("Ngende", "Hulengende"))
        self.assertFalse(shortened_artist_suffix("Gende", "Hulengende"))
        self.assertFalse(shortened_artist_suffix("Ngende", "Different Singer"))
        self.assertGreaterEqual(artist_similarity("Ngende", "Hulengende"), 0.90)
        self.assertTrue(distinctive_single_word_typo("Kwadabuka", "Kukwadabuka"))
        self.assertFalse(distinctive_single_word_typo("Bhura", "Bhura Dhanzi"))

    def test_informal_typo_combinations_are_ai_verified_and_counted(self):
        forms = (
            "Ngende Kwadabuka",
            "Kwadabuka Ngende",
            "Ngende - Kwadabuka",
            "vote for Ngende Kwadabuka",
            "Kwadabuka",  # long distinctive title-only typo
        )
        for n, phrase in enumerate(forms):
            with self.subTest(phrase=phrase):
                vote = self.vote(phrase, str(n))
                ranked = ranked_candidates(
                    "radio_zimbabwe",
                    vote.artist_normalized, vote.song_normalized,
                )
                self.assertIn(self.song.pk, [entry[3].pk for entry in ranked])
                shortlist = self.ai_decide(vote)
                self.assertIn(self.song.pk, [row["id"] for row in shortlist])
                vote.refresh_from_db()
                self.assertEqual(vote.ai_status, "approved")
        self.assertEqual(
            CleanedSongTally.objects.get(cleaned_song=self.song).count,
            len(forms),
        )
        self.assertFalse(CleanedSong.objects.filter(status="pending").exists())

    def test_clipped_artist_with_a_different_title_needs_review(self):
        vote = self.vote("Ngende Other Song")
        self.ai_decide(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_competing_clipped_stage_names_are_ambiguous(self):
        CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Zungende",
            title="Kukwadabuka",
            canonical_name="Zungende - Kukwadabuka", status="verified",
        )
        ranked = ranked_candidates(
            "radio_zimbabwe", "Unknown Artist", "Ngende Kwadabuka",
            exhaustive=True,
        )
        self.assertEqual(len(ranked), 2)
        self.assertIsNone(
            automatic_candidate(ranked, "Unknown Artist", "Ngende Kwadabuka")
        )
        vote = self.vote("Ngende Kwadabuka")
        self.ai_decide(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_competing_title_transcriptions_are_ambiguous(self):
        CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Hulengende",
            title="Kwadabuka",
            canonical_name="Hulengende - Kwadabuka", status="verified",
        )
        vote = self.vote("Ngende Kwadabuka")
        self.ai_decide(vote, song=self.song)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_artist_only_does_not_identify_a_song(self):
        vote = self.vote("Ngende")
        self.ai_decide(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")

    def test_cross_station_songs_never_compete_or_count(self):
        CleanedSong.objects.create(
            station="national_fm", artist="Hulengende",
            title="Kukwadabuka",
            canonical_name="Hulengende - Kukwadabuka", status="verified",
        )
        self.assertEqual(
            [row[3].pk for row in ranked_candidates(
                "radio_zimbabwe", "Unknown Artist", "Ngende Kwadabuka",
                exhaustive=True,
            )],
            [self.song.pk],
        )

    def test_historical_pending_vote_is_retried_once_and_reconciled(self):
        vote = self.vote("Ngende Kwadabuka")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": False, "matched_song_id": None,
                "confidence": "none", "reasoning": "Old candidate logic missed it",
            },
        ):
            process_vote(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        old = MatchKeyMapping.objects.get().cleaned_song
        self.assertEqual(old.status, "pending")

        call_command("retry_catalogue_votes", stdout=StringIO())
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "awaiting")
        self.ai_decide(vote)
        vote.refresh_from_db()
        old.refresh_from_db()
        self.assertEqual(vote.ai_status, "approved")
        self.assertEqual(old.status, "rejected")
        self.assertEqual(MatchKeyMapping.objects.get().cleaned_song_id, self.song.pk)
        self.assertEqual(CleanedSongTally.objects.get(cleaned_song=self.song).count, 1)
        process_vote(vote)
        self.assertEqual(CleanedSongTally.objects.get(cleaned_song=self.song).count, 1)
