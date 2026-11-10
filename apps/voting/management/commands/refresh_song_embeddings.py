"""Precompute and refresh OpenAI song vectors without touching vote tallies.

python manage.py refresh_song_embeddings --station radio_zimbabwe --limit 500
Every request is batched; unchanged catalogue records are never paid for again.
"""
from itertools import islice

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from apps.accounts.models import station_exists
from apps.voting.models import CleanedSong, SongEmbedding
from apps.voting.semantic_matching import embed_texts, embedding_text, fingerprint


def groups(items, size):
    iterator = iter(items)
    while True:
        group = list(islice(iterator, size))
        if not group:
            return
        yield group


class Command(BaseCommand):
    help = "Idempotently precompute station catalogue embeddings; no votes are changed."

    def add_arguments(self, parser):
        parser.add_argument("--station", required=True)
        parser.add_argument("--limit", type=int, default=1000)
        parser.add_argument("--batch-size", type=int, default=32)
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        station = options["station"]
        if not station_exists(station):
            raise CommandError("Unknown station")
        limit = options["limit"]
        size = options["batch_size"]
        if not 1 <= limit <= 10000 or not 1 <= size <= 64:
            raise CommandError("limit must be 1..10000 and batch-size 1..64")
        songs = list(
            CleanedSong.objects.filter(station=station, status="verified")
            .only("pk", "station", "artist", "title").order_by("pk")[:limit]
        )
        old = {
            row.song_id: row
            for row in SongEmbedding.objects.filter(
                station=station,
                song_id__in=[song.pk for song in songs],
            ).only("song_id", "model", "text_fingerprint")
        }
        pending = []
        for song in songs:
            text = embedding_text(song)
            digest = fingerprint(text)
            row = old.get(song.pk)
            if row is None or (
                row.model != settings.OPENAI_EMBEDDING_MODEL
                or row.text_fingerprint != digest
            ):
                pending.append((song, text, digest))
        self.stdout.write(
            f"station={station} verified={len(songs)} missing_or_stale={len(pending)}"
        )
        if options["dry_run"] or not pending:
            return
        for group in groups(pending, size):
            vectors = embed_texts([text for _, text, _ in group])
            with transaction.atomic():
                for (song, _text, digest), vector in zip(group, vectors):
                    SongEmbedding.objects.update_or_create(
                        song=song,
                        defaults={
                            "station": station, "model": settings.OPENAI_EMBEDDING_MODEL,
                            "text_fingerprint": digest, "vector": vector,
                        },
                    )
        self.stdout.write(f"Embedded {len(pending)} catalogue songs for {station}.")
