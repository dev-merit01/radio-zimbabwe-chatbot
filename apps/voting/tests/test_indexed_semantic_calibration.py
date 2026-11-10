"""End-to-end coverage for indexed/semantic retrieval and threshold activation.

Never mock the final vote count to make a weak semantic candidate 'approved'.
"""
import io
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.core.cache import cache
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection
from django.test import TestCase, override_settings

from apps.voting.calibration import DEFAULTS, active_thresholds
from apps.voting.catalogue_matching import automatic_candidate, ranked_candidates
from apps.voting.indexed_retrieval import narrowed_catalogue
from apps.voting.models import CleanedSong, MatchCalibration, SongEmbedding
from apps.voting.semantic_matching import (
    embed_texts, embedding_text, fingerprint, semantic_candidates,
)


class IndexedRetrievalTests(TestCase):
    def setUp(self):
        self.target = CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Hulengende feat. Madedido",
            title="Bhura Dhanzi", canonical_name="Hulengende feat. Madedido - Bhura Dhanzi",
            status="verified",
        )
        self.other = CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Unrelated Singer",
            title="Another Ballad", canonical_name="Unrelated Singer - Another Ballad",
            status="verified",
        )

    @override_settings(AI_INDEXED_SEARCH_ENABLED=True)
    def test_indexed_query_keeps_relevant_song(self):
        queryset, filtered = narrowed_catalogue(
            "radio_zimbabwe", "Unknown Artist", "Hulingende Bhura"
        )
        self.assertIn(self.target.pk, list(queryset.values_list("pk", flat=True)))
        if connection.vendor == "postgresql":
            self.assertTrue(filtered)
            # Native full-text indexes must exist regardless of whether a
            # database administrator enabled the optional pg_trgm extension.
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT to_regclass('song_search_fts_idx'), "
                    "to_regclass('alias_search_fts_idx')"
                )
                self.assertTrue(all(cursor.fetchone()))
            self.assertNotIn(self.other.pk, list(queryset.values_list("pk", flat=True)))

    @override_settings(AI_INDEXED_SEARCH_ENABLED=True)
    def test_inexact_indexed_ambiguity_still_prevents_approval(self):
        CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Hulengende feat. Madedido",
            title="Bhura Love", canonical_name="Hulengende feat. Madedido - Bhura Love",
            status="verified",
        )
        ranked = ranked_candidates("radio_zimbabwe", "Unknown Artist", "Hulingende Bhura")
        self.assertEqual(len(ranked), 2)
        self.assertIsNone(
            automatic_candidate(ranked, "Unknown Artist", "Hulingende Bhura")
        )

    def test_station_and_status_isolation(self):
        CleanedSong.objects.create(
            station="national_fm", artist="Hulengende", title="Bhura Dhanzi",
            canonical_name="Hulengende - Bhura Dhanzi", status="verified",
        )
        CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Hulengende", title="Bhura",
            canonical_name="Hulengende - Bhura", status="rejected",
        )
        queryset, _ = narrowed_catalogue("radio_zimbabwe", "Unknown Artist", "Bhura")
        self.assertEqual(list(queryset), [self.target])


@override_settings(
    OPENAI_API_KEY="test-only-key",
    OPENAI_EMBEDDING_MODEL="text-embedding-3-small",
    AI_VECTOR_ENABLED=True,
    AI_VECTOR_RETRIEVAL_MIN_SIMILARITY=0.60,
)
class SemanticRetrievalTests(TestCase):
    def setUp(self):
        cache.clear()
        self.song = CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Jnr Spragga", title="Daka",
            canonical_name="Jnr Spragga - Daka", status="verified",
        )
        self.vector = [1.0] + [0.0] * 15

    def add_embedding(self):
        return SongEmbedding.objects.create(
            song=self.song, station="radio_zimbabwe",
            model="text-embedding-3-small",
            text_fingerprint=fingerprint(embedding_text(self.song)),
            vector=self.vector,
        )

    def test_batch_embedding_calls_the_official_endpoint(self):
        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {"data": [
                    {"index": 0, "embedding": [1.0] + [0.0] * 15},
                ]}
        with patch("apps.voting.semantic_matching.requests.post",
                   return_value=FakeResponse()) as post:
            vectors = embed_texts(["Jnr Spragga Daka"])
        self.assertEqual(len(vectors[0]), 16)
        self.assertEqual(vectors[0][0], 1.0)
        self.assertEqual(
            post.call_args.kwargs["json"]["model"], "text-embedding-3-small"
        )
        self.assertEqual(
            post.call_args.args[0], "https://api.openai.com/v1/embeddings"
        )

    def test_query_embedding_is_skipped_when_corpus_empty(self):
        with patch("apps.voting.semantic_matching.embed_texts") as api:
            self.assertEqual(
                semantic_candidates("radio_zimbabwe", "Unknown Artist", "unfamiliar words"),
                [],
            )
        api.assert_not_called()

    def test_semantic_retrieval_is_a_hint_not_an_auto_count(self):
        self.add_embedding()
        with patch(
            "apps.voting.semantic_matching.embed_texts",
            return_value=[self.vector],
        ) as api:
            results = ranked_candidates(
                "radio_zimbabwe", "Unknown Artist", "unfamiliar words"
            )
        api.assert_called_once()
        self.assertEqual(results[0][3].pk, self.song.pk)
        self.assertEqual(results[0][1:3], (0.0, 0.0))
        self.assertIsNone(
            automatic_candidate(results, "Unknown Artist", "unfamiliar words")
        )
        with patch("apps.voting.semantic_matching.embed_texts") as second:
            ranked_candidates("radio_zimbabwe", "Unknown Artist", "unfamiliar words")
        second.assert_not_called()  # query embedding cached

    def test_embedding_backfill_is_idempotent_and_refreshes_edited_songs(self):
        with patch(
            "apps.voting.management.commands.refresh_song_embeddings.embed_texts",
            return_value=[self.vector],
        ) as api:
            call_command("refresh_song_embeddings", station="radio_zimbabwe",
                         stdout=io.StringIO())
        api.assert_called_once()
        self.assertEqual(SongEmbedding.objects.count(), 1)
        with patch(
            "apps.voting.management.commands.refresh_song_embeddings.embed_texts",
        ) as api:
            call_command("refresh_song_embeddings", station="radio_zimbabwe",
                         stdout=io.StringIO())
        api.assert_not_called()
        self.song.title = "Daka Remix"
        self.song.save(update_fields=["title"])
        with patch(
            "apps.voting.management.commands.refresh_song_embeddings.embed_texts",
            return_value=[self.vector],
        ) as api:
            call_command("refresh_song_embeddings", station="radio_zimbabwe",
                         stdout=io.StringIO())
        api.assert_called_once()
        self.assertEqual(SongEmbedding.objects.count(), 1)


class ThresholdCalibrationTests(TestCase):
    def test_unvalidated_profiles_do_not_change_live_thresholds(self):
        MatchCalibration.objects.create(
            station="radio_zimbabwe", is_active=True, labelled_count=100,
            evaluated_precision=1.0, min_score=0.86, min_gap=0.05,
        )
        self.assertEqual(active_thresholds("radio_zimbabwe"), DEFAULTS)

    def test_evaluated_profile_can_change_only_fuzzy_gate(self):
        profile = MatchCalibration.objects.create(
            station="radio_zimbabwe", is_active=True, labelled_count=600,
            evaluated_precision=1.0, min_score=0.94, min_gap=0.15,
            min_artist=0.90, min_title=0.90,
        )
        self.assertEqual(active_thresholds("radio_zimbabwe").min_score, 0.94)
        profile.is_active = False
        profile.save(update_fields=["is_active"])
        self.assertEqual(active_thresholds("radio_zimbabwe"), DEFAULTS)

    def test_tiny_dataset_cannot_activate_thresholds(self):
        song = CleanedSong.objects.create(
            station="radio_zimbabwe", artist="Jnr Spragga", title="Daka",
            canonical_name="Jnr Spragga - Daka", status="verified",
        )
        examples = [
            {"input": f"Jnr Spragga - Daka {n}", "canonical": song.canonical_name}
            for n in range(15)
        ]
        examples.extend({
            "input": f"Unrelated Song {n}", "canonical": None
        } for n in range(10))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "labels.jsonl"
            path.write_text(
                "\n".join(json.dumps(x) for x in examples), encoding="utf-8"
            )
            with self.assertRaises(CommandError):
                call_command(
                    "calibrate_vote_matching", station="radio_zimbabwe",
                    dataset=str(path), activate=True, stdout=io.StringIO(),
                )
        self.assertFalse(MatchCalibration.objects.exists())
