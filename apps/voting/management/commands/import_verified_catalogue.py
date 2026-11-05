"""Import the reviewed, source-linked November catalogue once per station."""

import hashlib
import json
import re
import unicodedata
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from apps.accounts.models import Station
from apps.voting.models import CleanedSong, ReviewAudit
from apps.voting.pipeline import lock_station

DATA_PATH = Path('apps/voting/data/screenshot_20261105.json')


def normalise(value):
    value = unicodedata.normalize('NFKC', value).casefold()
    return ''.join(c for c in value if c.isalnum())


def identity(artist, title):
    # Only compare the leading artist. A changed guest-credit display must not
    # create another copy of a recording; keep version text in the title.
    lead = re.split(r'\s+feat\.?\s+|\s+ft\.?\s+|,|\s+&\s+', artist, maxsplit=1, flags=re.IGNORECASE)[0]
    lead = normalise(lead)
    lead = {'juniorspragga': 'jnrspragga'}.get(lead, lead)
    title = re.sub(r'\s*\(feat\..*?\)', '', title, flags=re.IGNORECASE)
    return lead, normalise(title)


class Command(BaseCommand):
    help = 'Import the source-verified 5 November 2026 batch without changing existing decisions or votes.'

    def add_arguments(self, parser):
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--station', choices=Station.values)
        group.add_argument('--all-stations', action='store_true')

    def handle(self, *args, **options):
        raw = (settings.BASE_DIR / DATA_PATH).read_bytes()
        payload = json.loads(raw)
        batch = payload['batch_id']
        digest = hashlib.sha256(raw).hexdigest()
        records = payload['songs']
        seen = set()
        for record in records:
            key = identity(record['artist'], record['title'])
            if key in seen or not all(key) or not record.get('source', '').startswith('https://'):
                raise CommandError('Duplicate, empty or unsourced catalogue entry')
            seen.add(key)
            if len(record['artist']) > 256 or len(record['title']) > 256 or len(record['artist'] + ' - ' + record['title']) > 512:
                raise CommandError('Catalogue entry exceeds database field limits')
        stations = Station.values if options['all_stations'] else [options['station']]
        for station in stations:
            with transaction.atomic():
                lock_station(station)
                previous = ReviewAudit.objects.filter(
                    station=station, action='verified_seed', details__batch_id=batch,
                ).first()
                if previous:
                    if previous.details.get('sha256') != digest:
                        raise CommandError('Previously imported batch changed; use a new reviewed batch')
                    self.stdout.write(f'{station}: batch already imported; no changes')
                    continue
                existing = list(CleanedSong.objects.filter(station=station))
                by_key = {}
                by_spotify = {}
                names = set()
                for song in existing:
                    by_key.setdefault(identity(song.artist, song.title), song)
                    names.add(normalise(song.canonical_name))
                    if song.spotify_track_id:
                        by_spotify[song.spotify_track_id] = song
                created = []
                preserved = []
                for record in records:
                    key = identity(record['artist'], record['title'])
                    candidates = [key] + [identity(a['artist'], a['title']) for a in record.get('aliases', [])]
                    match = next((by_key[k] for k in candidates if k in by_key), None)
                    match = match or by_spotify.get(record.get('spotify_track_id'))
                    canonical = f"{record['artist']} - {record['title']}"
                    if match or normalise(canonical) in names:
                        preserved.append({'requested': canonical, 'existing_id': match.pk if match else None,
                                          'status': match.status if match else 'canonical_collision'})
                        continue
                    song = CleanedSong.objects.create(
                        station=station, artist=record['artist'], title=record['title'],
                        canonical_name=canonical, status='verified',
                        spotify_track_id=record.get('spotify_track_id'), album=record.get('album', ''),
                    )
                    for candidate in candidates:
                        by_key[candidate] = song
                    names.add(normalise(canonical))
                    created.append({'song_id': song.pk, 'name': canonical, 'source': record['source']})
                # Verification is of catalogue metadata only. No raw votes,
                # match-key mappings, or tally tables are touched here.
                ReviewAudit.objects.create(station=station, action='verified_seed', details={
                    'batch_id': batch, 'sha256': digest, 'created': len(created),
                    'preserved': len(preserved), 'songs': created, 'existing': preserved,
                    'held': payload.get('held', []),
                })
            self.stdout.write(self.style.SUCCESS(
                f'{station}: created={len(created)} verified, preserved={len(preserved)}, held={len(payload.get("held", []))}; votes unchanged'
            ))
