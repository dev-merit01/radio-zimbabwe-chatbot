"""End-to-end regression cases for malformed intake and canonical vote identity."""

import json
from datetime import date, timedelta
from unittest.mock import patch
from django.test import TestCase, override_settings
from apps.voting.models import (
    RawVote,
    InboundEvent,
    OutboundMessage,
    CleanedSong,
    CleanedSongTally,
    MatchKeyMapping,
    ReviewAudit,
    LLMDecisionLog,
)
from apps.voting.services import VotingService, parse_vote_input
from apps.voting.pipeline import process_vote, review_song, rebuild_tallies
from apps.voting.text_cleaning import clean_vote_text


@override_settings(
    SECURE_SSL_REDIRECT=False,
    TELEGRAM_WEBHOOK_SECRET="accuracy-secret",
    AUTO_AI_MATCH=False,
    ALLOW_REPEAT_SONG=False,
)
class VoteAccuracyTests(TestCase):
    def song(
        self,
        artist="Example Artist",
        title="Morning Sunshine",
        station="radio_zimbabwe",
    ):
        return CleanedSong.objects.create(
            station=station,
            artist=artist,
            title=title,
            canonical_name=f"{artist} - {title}",
            status="verified",
        )

    def vote(self, text, listener="listener", day=None):
        result = VotingService("telegram", listener).handle_incoming_text(
            text, vote_date=day
        )
        self.assertIn("Submission received", result)
        return RawVote.objects.filter(user__user_ref=listener).latest("pk")

    def webhook(self, text=None, **message):
        message.update(chat={"id": 123, "type": "private"})
        if text is not None:
            message["text"] = text
        return self.client.post(
            "/webhook/telegram/",
            json.dumps({"update_id": 12, "message": message}),
            content_type="application/json",
            HTTP_X_TELEGRAM_BOT_API_SECRET_TOKEN="accuracy-secret",
        )

    def test_invalid_messages_never_enter_database(self):
        for text in [
            "123456789",
            "@@@ - ###",
            "Artist - !!!",
            "hello",
            "https://spam.com",
            "Artist - " + "x" * 101,
            "Artist\n- Song",
            "Artist - \x00Song",
        ]:
            with self.subTest(text=text):
                self.assertTrue(self.webhook(text).json()["ignored"])
                VotingService("telegram", "bad").handle_incoming_text(text)
        self.assertFalse(InboundEvent.objects.exists())
        self.assertFalse(RawVote.objects.exists())
        self.assertFalse(OutboundMessage.objects.exists())

    def test_media_with_valid_caption_is_dropped(self):
        response = self.webhook(
            photo=[{"file_id": "photo"}], caption="Example Artist - Morning Sunshine"
        )
        self.assertTrue(response.json()["ignored"])
        self.assertFalse(InboundEvent.objects.exists())

    def test_commands_still_work_and_valid_delivery_deduplicates(self):
        self.assertEqual(self.webhook("/help").status_code, 202)
        self.assertEqual(self.webhook("/help").status_code, 200)
        self.assertEqual(InboundEvent.objects.count(), 1)

    def test_separators_and_hyphenated_artist(self):
        for separator in ["–", "—", "−", "|", "-"]:
            self.assertEqual(
                parse_vote_input(f"Example Artist {separator} Morning Sunshine"),
                ("Example Artist", "Morning Sunshine"),
            )
        self.assertEqual(
            parse_vote_input("Anne-Marie - Morning Sunshine"),
            ("Anne-Marie", "Morning Sunshine"),
        )

    def test_recording_versions_are_not_stripped(self):
        for suffix in ["(Remix)", "(Live)", "(Acoustic)", "(Radio Edit)"]:
            self.assertEqual(
                clean_vote_text("Example Artist", "Morning Sunshine " + suffix)[1],
                "Morning Sunshine " + suffix,
            )

    def test_artist_and_title_typos_match_without_external_calls(self):
        song = self.song()
        vote = self.vote("Exampl Artist - Morning Sunshin")
        with patch("apps.voting.ai.match_vote_with_llm") as ai:
            process_vote(vote)
            ai.assert_not_called()
        self.assertEqual(MatchKeyMapping.objects.get().cleaned_song, song)
        self.assertEqual(CleanedSongTally.objects.get().count, 1)
        self.assertEqual(ReviewAudit.objects.filter(action="auto_match").count(), 1)
        process_vote(vote)
        self.assertEqual(ReviewAudit.objects.filter(action="auto_match").count(), 1)

    def test_exact_match_wins_over_nearby_title(self):
        song = self.song()
        self.song(title="Morning Sunshines")
        process_vote(self.vote("Example Artist - Morning Sunshine"))
        self.assertEqual(MatchKeyMapping.objects.get().cleaned_song, song)

    @override_settings(AUTO_AI_MATCH=True)
    def test_ambiguous_candidates_are_not_automatically_merged(self):
        a = self.song(title="Morning Sunshine A")
        self.song(title="Morning Sunshine B")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": True,
                "confidence": "high",
                "matched_song_id": a.pk,
                "reasoning": "guess",
            },
        ):
            process_vote(self.vote("Example Artist - Morning Sunshine"))
        self.assertEqual(MatchKeyMapping.objects.get().cleaned_song.status, "pending")
        self.assertFalse(CleanedSongTally.objects.exists())
        self.assertFalse(LLMDecisionLog.objects.get().was_applied)

    @override_settings(AUTO_AI_MATCH=True)
    def test_ai_shortlist_includes_misspelled_artist_and_rejects_foreign_id(self):
        self.song()
        foreign = self.song(station="national_fm")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": True,
                "confidence": "high",
                "matched_song_id": foreign.pk,
            },
        ) as ai:
            process_vote(self.vote("Exmple Artst - Morng Sunshne"))
        self.assertTrue(ai.called)
        self.assertNotIn(foreign.pk, [item["id"] for item in ai.call_args.args[2]])
        self.assertEqual(MatchKeyMapping.objects.get().cleaned_song.status, "pending")

    def test_short_titles_and_versions_require_review(self):
        self.song(title="Fire")
        self.song()
        for i, text in enumerate(
            ["Example Artist - Fira", "Example Artist - Morning Sunshine (Remix)"]
        ):
            process_vote(self.vote(text, listener=str(i)))
        self.assertFalse(CleanedSongTally.objects.exists())

    def test_merge_deduplicates_by_listener_and_day_preserving_submissions(self):
        song = self.song()
        day = date(2026, 9, 1)
        first = self.vote("Example Artist - Morning Sunshine", day=day)
        alias = self.vote("Other spelling - Alternate title", day=day)
        for vote in [first, alias]:
            process_vote(vote)
        pending = MatchKeyMapping.objects.get(match_key=alias.match_key).cleaned_song
        process_vote(
            self.vote("Other spelling - Alternate title", listener="second", day=day)
        )
        process_vote(
            self.vote("Other spelling - Alternate title", day=day + timedelta(days=1))
        )
        review_song("radio_zimbabwe", None, pending.pk, "merge", target_id=song.pk)
        self.assertEqual(CleanedSongTally.objects.get(date=day).count, 2)
        self.assertEqual(
            CleanedSongTally.objects.get(date=day + timedelta(days=1)).count, 1
        )
        self.assertEqual(RawVote.objects.count(), 4)
        rebuild_tallies("radio_zimbabwe")
        self.assertEqual(CleanedSongTally.objects.get(date=day).count, 2)
        with override_settings(ALLOW_REPEAT_SONG=True):
            rebuild_tallies("radio_zimbabwe")
        self.assertEqual(CleanedSongTally.objects.get(date=day).count, 3)

    def test_late_mapping_excludes_second_variant_from_live_total(self):
        song = self.song()
        first = self.vote("Example Artist - Morning Sunshine")
        second = self.vote("Exampl Artist - Morning Sunshin")
        for vote in [first, second, second]:
            process_vote(vote)
        self.assertEqual(CleanedSongTally.objects.get(cleaned_song=song).count, 1)
        self.assertEqual(RawVote.objects.count(), 2)

    @override_settings(AUTO_AI_MATCH=True)
    def test_ai_match_is_logged_once_and_cannot_invent_a_song(self):
        song = self.song()
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={
                "matched": True,
                "confidence": "high",
                "matched_song_id": song.pk,
                "reasoning": "Artist and title refer to the candidate.",
            },
        ) as ai:
            vote = self.vote("Exmple Artst - Morng Sunshne")
            process_vote(vote)
            process_vote(vote)
        self.assertEqual(ai.call_count, 1)
        self.assertEqual(CleanedSong.objects.count(), 1)
        self.assertEqual(CleanedSongTally.objects.get().count, 1)
        self.assertTrue(LLMDecisionLog.objects.get().was_applied)

    @override_settings(AUTO_AI_MATCH=True)
    def test_ai_failure_keeps_raw_vote_for_retry_without_mapping(self):
        self.song()
        vote = self.vote("Exmple Artst - Morng Sunshne")
        with patch(
            "apps.voting.ai.match_vote_with_llm",
            return_value={"reasoning": "LLM error: Timeout"},
        ):
            with self.assertRaises(RuntimeError):
                process_vote(vote)
        self.assertEqual(RawVote.objects.count(), 1)
        self.assertFalse(MatchKeyMapping.objects.exists())

    def test_invalid_manual_submission_creates_no_event_or_audit(self):
        import uuid
        from django.contrib.auth import get_user_model

        admin = get_user_model().objects.create_superuser(
            "accuracy-admin", password="test-only"
        )
        self.client.force_login(admin)
        response = self.client.post(
            "/api/workspace/votes",
            json.dumps(
                {
                    "listener": "listener",
                    "text": "@@@ - ###",
                    "request_id": str(uuid.uuid4()),
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertFalse(InboundEvent.objects.exists())
        self.assertFalse(ReviewAudit.objects.exists())
