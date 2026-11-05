from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase, override_settings

from apps.voting.models import (
    CleanedSong,
    CleanedSongTally,
    MatchKeyMapping,
    RawVote,
    ReviewAudit,
)
from apps.voting.pipeline import process_vote, rebuild_tallies, review_song
from apps.voting.services import VotingService


@override_settings(AI_ALL_VOTES=True, AUTO_AI_MATCH=True, ALLOW_REPEAT_SONG=True)
class FreeformCatalogueTests(TestCase):
    def setUp(self):
        self.song = CleanedSong.objects.create(
            station="radio_zimbabwe",
            artist="Jnr Spragga",
            title="Daka",
            canonical_name="Jnr Spragga - Daka",
            status="verified",
        )

    def vote(self, text, listener="listener"):
        VotingService("whatsapp", listener).handle_incoming_text(text)
        return RawVote.objects.latest("pk")

    def approve(self, vote, song=None):
        song = song or self.song
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": True,
                "matched_song_id": song.pk,
                "confidence": "high",
                "reasoning": "Clear matching song",
            },
        ) as ai:
            process_vote(vote)
        return ai.call_args.args[2] if ai.called else []

    def test_typo_missing_separator_reverse_and_short_title(self):
        for index, text in enumerate(
            ("jnr spraga daka", "daka jnr spraga", "jnr spraga - daka", "daka")
        ):
            with self.subTest(text=text):
                vote = self.vote(text, str(index))
                candidates = self.approve(vote)
                self.assertIn(self.song.pk, [row["id"] for row in candidates])
                vote.refresh_from_db()
                self.assertEqual(vote.ai_status, "approved")
        self.assertEqual(CleanedSongTally.objects.get(cleaned_song=self.song).count, 4)

    def test_ambiguous_title_remains_review_even_if_ai_selects(self):
        CleanedSong.objects.create(
            station="radio_zimbabwe",
            artist="Another Artist",
            title="Daka",
            canonical_name="Another Artist - Daka",
            status="verified",
        )
        vote = self.vote("daka")
        self.approve(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_near_identical_artists_remain_review(self):
        CleanedSong.objects.create(
            station="radio_zimbabwe",
            artist="Jnr Spraga",
            title="Daka",
            canonical_name="Jnr Spraga - Daka",
            status="verified",
        )
        vote = self.vote("jnr spraggaa daka")
        self.approve(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")

    def test_wrong_artist_version_and_extra_words_do_not_count(self):
        for index, text in enumerate(
            ("Winky D daka", "jnr spragga daka remix", "Jnr Spragga Daka Banana")
        ):
            vote = self.vote(text, str(index))
            self.approve(vote)
            vote.refresh_from_db()
            self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_classic_263_misspelled_full_title_is_offered_to_ai(self):
        song = CleanedSong.objects.create(
            station="classic_263",
            artist="Oriyano",
            title="Jolai Nengirozi",
            canonical_name="Oriyano - Jolai Nengirozi",
            status="verified",
        )
        response = VotingService(
            "whatsapp", "classic-listener", "classic_263"
        ).handle_incoming_text("Jola Nengirozi")
        self.assertIn("Submission received", response)
        vote = RawVote.objects.get(station="classic_263")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": True, "matched_song_id": song.pk,
                "confidence": "high", "reasoning": "One clear title typo",
            },
        ) as ai:
            process_vote(vote)
        self.assertIn(song.pk, [row["id"] for row in ai.call_args.args[2]])
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "approved")
        self.assertEqual(
            MatchKeyMapping.objects.get(station="classic_263").cleaned_song, song
        )
        self.assertEqual(
            CleanedSongTally.objects.get(cleaned_song=song).count, 1
        )
        self.assertFalse(
            CleanedSong.objects.filter(station="classic_263", status="pending").exists()
        )

    def test_classic_263_pending_title_typo_is_retried_and_reconciled(self):
        song = CleanedSong.objects.create(
            station="classic_263",
            artist="Oriyano",
            title="Jolai Nengirozi",
            canonical_name="Oriyano - Jolai Nengirozi",
            status="verified",
        )
        VotingService(
            "whatsapp", "classic-old-listener", "classic_263"
        ).handle_incoming_text("Jola Nengirozi")
        vote = RawVote.objects.get(station="classic_263")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={"matched": False, "confidence": "none",
                          "reasoning": "Old model decision"},
        ):
            process_vote(vote)
        placeholder = MatchKeyMapping.objects.get(
            station="classic_263"
        ).cleaned_song
        self.assertEqual(vote.ai_status, "awaiting")  # stale model instance
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertEqual(placeholder.status, "pending")
        self.assertFalse(CleanedSongTally.objects.exists())
        call_command("retry_catalogue_votes", stdout=StringIO())
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "awaiting")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": True, "matched_song_id": song.pk,
                "confidence": "high", "reasoning": "Distinctive verified title",
            },
        ):
            process_vote(vote)
        vote.refresh_from_db()
        placeholder.refresh_from_db()
        self.assertEqual(vote.ai_status, "approved")
        self.assertEqual(placeholder.status, "rejected")
        self.assertEqual(
            MatchKeyMapping.objects.get(station="classic_263").cleaned_song, song
        )
        self.assertEqual(CleanedSongTally.objects.get(cleaned_song=song).count, 1)
        self.assertEqual(RawVote.objects.filter(station="classic_263").count(), 1)

    def test_close_competing_full_titles_require_human_review(self):
        first = CleanedSong.objects.create(
            station="classic_263", artist="Oriyano", title="Jolai Nengirozi",
            canonical_name="Oriyano - Jolai Nengirozi", status="verified",
        )
        CleanedSong.objects.create(
            station="classic_263", artist="Second Artist",
            title="Jolai Nengirozi",
            canonical_name="Second Artist - Jolai Nengirozi", status="verified",
        )
        VotingService("whatsapp", "ambiguous", "classic_263").handle_incoming_text(
            "Jola Nengirozi"
        )
        vote = RawVote.objects.get(station="classic_263")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": True, "matched_song_id": first.pk,
                "confidence": "high", "reasoning": "Guess",
            },
        ):
            process_vote(vote)
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_extra_words_are_not_accepted_as_a_title_typo(self):
        song = CleanedSong.objects.create(
            station="classic_263", artist="Oriyano", title="Jolai Nengirozi",
            canonical_name="Oriyano - Jolai Nengirozi", status="verified",
        )
        VotingService("whatsapp", "extras", "classic_263").handle_incoming_text(
            "Jola Nengirozi Bonus"
        )
        vote = RawVote.objects.get(station="classic_263")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": True, "matched_song_id": song.pk,
                "confidence": "high", "reasoning": "Guess",
            },
        ) as ai:
            process_vote(vote)
        self.assertNotIn(song.pk, [row["id"] for row in ai.call_args.args[2]])
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())

    def pending(self):
        vote = self.vote("jnr spraga daka")
        with patch(
            "apps.voting.ai.match_vote_with_llm", return_value={"matched": False}
        ):
            process_vote(vote)
        return vote, MatchKeyMapping.objects.get().cleaned_song

    def test_recovery_replaces_placeholder_and_is_idempotent(self):
        vote, placeholder = self.pending()
        call_command("retry_catalogue_votes", stdout=StringIO())
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "awaiting")
        self.approve(vote)
        self.approve(vote)
        call_command("retry_catalogue_votes", stdout=StringIO())
        rebuild_tallies("radio_zimbabwe")
        vote.refresh_from_db()
        placeholder.refresh_from_db()
        self.assertEqual(vote.ai_status, "approved")
        self.assertEqual(placeholder.status, "rejected")
        self.assertEqual(MatchKeyMapping.objects.get().cleaned_song_id, self.song.pk)
        self.assertEqual(CleanedSongTally.objects.get().count, 1)
        self.assertEqual(RawVote.objects.count(), 1)

    @override_settings(ALLOW_REPEAT_SONG=False)
    def test_recovered_variation_does_not_double_count_listener(self):
        vote, _placeholder = self.pending()
        correct = self.vote("Jnr Spragga - Daka")
        self.approve(correct)
        call_command("retry_catalogue_votes", stdout=StringIO())
        self.approve(vote)
        self.assertEqual(CleanedSongTally.objects.get().count, 1)

    def test_manual_pending_or_rejected_decision_is_preserved(self):
        vote, placeholder = self.pending()
        review_song("radio_zimbabwe", None, placeholder.pk, "pending")
        call_command("retry_catalogue_votes", stdout=StringIO())
        vote.refresh_from_db()
        self.assertEqual(vote.ai_status, "review")
        self.assertFalse(CleanedSongTally.objects.exists())
        self.assertFalse(ReviewAudit.objects.filter(action="ai_remap").exists())

    def test_existing_pending_mapping_does_not_trap_future_votes(self):
        old, _placeholder = self.pending()
        new = self.vote("jnr spraga daka", "second")
        self.approve(new)
        old.refresh_from_db()
        new.refresh_from_db()
        self.assertEqual(old.ai_status, "review")
        self.assertEqual(new.ai_status, "approved")
        self.assertEqual(CleanedSongTally.objects.get().count, 1)
