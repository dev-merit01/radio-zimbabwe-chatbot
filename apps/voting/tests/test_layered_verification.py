"""Regression cases from Radio Zimbabwe's actual failed spelling patterns.

The OpenAI response is mocked here; the tests enforce that the intended
canonical song appears in the shortlist and that only station-verified,
unambiguous outcomes can change the count.
"""
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.core.management import call_command

from apps.voting.catalogue_matching import (
    artist_credits, ranked_candidates, normalise,
)
from apps.voting.models import CleanedSong, CleanedSongTally, RawVote, SongAlias
from apps.voting.pipeline import process_vote
from apps.voting.services import VotingService


@override_settings(AI_ALL_VOTES=True, AUTO_AI_MATCH=True, ALLOW_REPEAT_SONG=True)
class LayeredVoteVerificationTests(TestCase):
    def setUp(self):
        self.song = CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Hulengende Feat: Madedido",
            title="Bhura Dhanzi",
            canonical_name="Hulengende Feat: Madedido - Bhura Dhanzi",
            status="verified",
        )

    def vote(self, message, identity):
        response = VotingService("whatsapp", identity).handle_incoming_text(message)
        self.assertIn("Submission received", response)
        return RawVote.objects.filter(user__user_ref=identity).latest("pk")

    def decide(self, vote, song=None):
        song = song or self.song
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": True, "matched_song_id": song.pk,
                "confidence": "high", "reasoning": "Distinctive artist and title",
            },
        ) as ai:
            process_vote(vote)
        self.assertEqual(ai.call_count, 1)
        return ai.call_args.args[2]

    def test_lead_artist_separate_from_featured_credits(self):
        lead, featured = artist_credits("Hulengende Feat: Madedido")
        self.assertEqual(lead, "hulengende")
        self.assertEqual(featured, "madedido")

    def test_shortened_title_phonetic_artist_all_approve_without_human(self):
        variations = (
            "Hulingende Bhura",
            "Hulengende Bhura",
            "Bhura Hulingende",
            "Hulengende - Bhura",
            "vote for Hulingende Bhura",
        )
        for n, message in enumerate(variations):
            with self.subTest(message=message):
                vote = self.vote(message, f"short-{n}")
                candidates = self.decide(vote)
                self.assertIn(self.song.pk, [entry["id"] for entry in candidates])
                self.assertEqual(candidates[0]["primary_artist"], "hulengende")
                self.assertEqual(candidates[0]["featured_artists"], "madedido")
                vote.refresh_from_db()
                self.assertEqual(vote.ai_status, "approved")
        self.assertEqual(
            CleanedSongTally.objects.get(cleaned_song=self.song).count,
            len(variations),
        )
        self.assertFalse(CleanedSong.objects.filter(status="pending").exists())

    def test_two_similar_songs_keep_shortened_title_in_review(self):
        CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Hulengende feat. Madedido",
            title="Bhura Love",
            canonical_name="Hulengende feat. Madedido - Bhura Love",
            status="verified",
        )
        vote = self.vote("Hulingende Bhura", "ambiguous")
        candidates = self.decide(vote)
        self.assertEqual(len(candidates), 2)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_title_only_shortened_title_is_not_enough(self):
        vote = self.vote("Bhura", "title-only")
        self.decide(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_wrong_artist_and_different_recording_do_not_count(self):
        for n, message in enumerate(("Winky D Bhura", "Hulengende - Bhura Dhanzi Live")):
            vote = self.vote(message, f"invalid-{n}")
            self.decide(vote)
            vote.refresh_from_db()
            self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_approved_full_alias_can_recover_an_old_pending_vote(self):
        vote = self.vote("Hule Bhur", "historical")
        self.decide(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        SongAlias.objects.create(
            station="radio_zimbabwe", song=self.song, kind="full",
            value="Hule Bhur", is_approved=True,
        )
        call_command("retry_catalogue_votes", verbosity=0)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "awaiting")
        candidates = self.decide(vote)
        self.assertIn(self.song.pk, [entry["id"] for entry in candidates])
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "approved")
        self.assertEqual(CleanedSongTally.objects.get(cleaned_song=self.song).count, 1)
        process_vote(vote)
        self.assertEqual(CleanedSongTally.objects.get(cleaned_song=self.song).count, 1)

    def test_unapproved_alias_never_counts(self):
        SongAlias.objects.create(
            station="radio_zimbabwe", song=self.song, kind="full",
            value="Unrelated Name", is_approved=False,
        )
        vote = self.vote("Unrelated Name", "unapproved")
        self.decide(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")

    def test_alias_collisions_are_not_accepted(self):
        second = CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Other Artist",
            title="Other Song", canonical_name="Other Artist - Other Song",
            status="verified",
        )
        for song in (self.song, second):
            SongAlias.objects.create(
                station="radio_zimbabwe", song=song, kind="full",
                value="Shared Name", is_approved=True,
            )
        vote = self.vote("Shared Name", "collision")
        self.decide(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_cross_station_alias_is_not_available(self):
        SongAlias.objects.create(
            station="radio_zimbabwe", song=self.song, kind="full",
            value="Hule Bhur", is_approved=True,
        )
        self.assertFalse(ranked_candidates("national_fm", "Unknown Artist", "Hule Bhur"))
        self.assertEqual(normalise("Beyoncé"), "beyonce")

