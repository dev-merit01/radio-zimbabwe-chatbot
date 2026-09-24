from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from apps.accounts.models import Station
from apps.voting.models import CleanedSong, ReviewAudit
from apps.voting.pipeline import lock_station
from apps.spotify.search import resolve_with_confidence, is_high_confidence


class Command(BaseCommand):
    help = "Add Spotify metadata without changing song identity, approval or vote mappings."

    def add_arguments(self, parser):
        parser.add_argument("--station", required=True, choices=Station.values)
        parser.add_argument("--limit", type=int, default=25)
        parser.add_argument("--song-id", type=int)

    def handle(self, *args, **options):
        if not 1 <= options["limit"] <= 100:
            raise CommandError("Limit must be between 1 and 100.")
        qs = CleanedSong.objects.filter(station=options["station"]).exclude(
            status="rejected"
        )
        if options["song_id"]:
            qs = qs.filter(pk=options["song_id"])
        for song in qs.order_by("id")[: options["limit"]]:
            try:
                track, score = resolve_with_confidence(song.artist, song.title)
            except Exception as exc:
                raise CommandError(
                    "Spotify lookup failed: " + type(exc).__name__
                ) from None
            if not track or not is_high_confidence(score):
                continue
            with transaction.atomic():
                lock_station(song.station)
                current = CleanedSong.objects.get(pk=song.pk, station=song.station)
                if (
                    current.canonical_name != song.canonical_name
                    or current.status == "rejected"
                ):
                    continue
                current.spotify_track_id = track.get("id", "")
                current.album = track.get("album", "")[:256]
                current.image_url = track.get("image_url", "")[:200]
                current.preview_url = (track.get("preview_url") or "")[:200]
                current.save(
                    update_fields=[
                        "spotify_track_id",
                        "album",
                        "image_url",
                        "preview_url",
                        "updated_at",
                    ]
                )
                ReviewAudit.objects.create(
                    station=song.station,
                    action="spotify_metadata",
                    details={"id": song.pk},
                )
                self.stdout.write(f"Updated metadata for song {song.pk}.")
