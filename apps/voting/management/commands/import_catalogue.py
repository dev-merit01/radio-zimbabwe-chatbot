"""Append a sourced JSON catalogue to one or all station review queues."""

import hashlib
import json
from pathlib import Path
import unicodedata

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from apps.accounts.models import Station
from apps.voting.models import CleanedSong, ReviewAudit
from apps.voting.pipeline import lock_station


def key(value):
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def read_catalogue(path):
    try:
        raw = Path(path).read_bytes()
        data = json.loads(raw)
    except (OSError, ValueError, UnicodeError) as exc:
        raise CommandError(f"Cannot read catalogue: {exc}") from exc
    if not isinstance(data, dict) or data.get("schema_version") != 1:
        raise CommandError("Expected a schema_version 1 catalogue object.")
    songs = data.get("songs")
    if not isinstance(songs, list) or not songs:
        raise CommandError("songs must be a non-empty list.")
    result, seen = [], set()
    for number, row in enumerate(songs, 1):
        if not isinstance(row, dict):
            raise CommandError(f"Song {number} must be an object.")
        fields = {}
        for field in ("artist", "title", "album"):
            value = row.get(field, "" if field == "album" else None)
            if not isinstance(value, str) or (field != "album" and not value.strip()):
                raise CommandError(f"Song {number}: invalid {field}.")
            fields[field] = " ".join(value.split())
            if len(fields[field]) > 256:
                raise CommandError(f"Song {number}: {field} exceeds 256 characters.")
        features = row.get("featured_artists", [])
        if not isinstance(features, list) or any(
            not isinstance(item, str) or not item.strip() for item in features
        ):
            raise CommandError(f"Song {number}: invalid featured_artists.")
        credit = fields["artist"]
        if features:
            credit += " feat. " + " & ".join(
                " ".join(item.split()) for item in features
            )
        fields["canonical_name"] = f"{credit} - {fields['title']}"
        if len(fields["canonical_name"]) > 512:
            raise CommandError(f"Song {number}: full credit exceeds 512 characters.")
        identity = (key(fields["artist"]), key(fields["title"]))
        if identity in seen:
            raise CommandError(
                f"Song {number}: repeated artist/title; disambiguate recording versions."
            )
        seen.add(identity)
        result.append(fields)
    return result, hashlib.sha256(raw).hexdigest()


class Command(BaseCommand):
    help = "Import JSON songs as pending, preserving existing songs and decisions."

    def add_arguments(self, parser):
        parser.add_argument("catalogue", type=Path)
        scope = parser.add_mutually_exclusive_group(required=True)
        scope.add_argument("--station", choices=Station.values)
        scope.add_argument("--all-stations", action="store_true")
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        songs, digest = read_catalogue(options["catalogue"])
        stations = (
            sorted(Station.values) if options["all_stations"] else [options["station"]]
        )
        total = 0
        with transaction.atomic():
            for station in stations:
                if not options["dry_run"]:
                    lock_station(station)
                existing = list(
                    CleanedSong.objects.filter(station=station).values_list(
                        "artist", "title", "canonical_name"
                    )
                )
                pairs = {(key(a), key(t)) for a, t, _ in existing}
                names = {key(n) for _, _, n in existing}
                additions = [
                    song
                    for song in songs
                    if (key(song["artist"]), key(song["title"])) not in pairs
                    and key(song["canonical_name"]) not in names
                ]
                if not options["dry_run"]:
                    CleanedSong.objects.bulk_create(
                        [
                            CleanedSong(station=station, status="pending", **song)
                            for song in additions
                        ]
                    )
                    ReviewAudit.objects.create(
                        station=station,
                        action="seed_catalogue",
                        details={
                            "file": options["catalogue"].name,
                            "sha256": digest,
                            "created": len(additions),
                            "skipped": len(songs) - len(additions),
                        },
                    )
                total += len(additions)
                verb = "Would add" if options["dry_run"] else "Added"
                self.stdout.write(f"{station}: {verb} {len(additions)} pending songs.")
        self.stdout.write(
            f"Total: {total}. Existing songs and review decisions preserved."
        )
