"""
Management command to enrich pending songs with Spotify data.

Usage:
    python manage.py enrich_spotify           # Enrich all pending songs
    python manage.py enrich_spotify --limit 10
    python manage.py enrich_spotify --song-id 42
"""
from django.core.management.base import BaseCommand

from apps.voting.models import CleanedSong
from apps.voting.cleaning import CleaningService


class Command(BaseCommand):
    help = 'Enrich pending songs with Spotify data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit',
            type=int,
            default=50,
            help='Maximum number of songs to process (default: 50)',
        )
        parser.add_argument(
            '--song-id',
            type=int,
            help='Enrich a specific song by ID',
        )
        parser.add_argument(
            '--include-verified',
            action='store_true',
            help='Also try to enrich verified songs without Spotify ID',
        )

    def handle(self, *args, **options):
        service = CleaningService()
        
        if options['song_id']:
            self._enrich_single(service, options['song_id'])
            return
        
        self._enrich_batch(service, options['limit'], options['include_verified'])
    
    def _enrich_single(self, service, song_id):
        """Enrich a single song."""
        try:
            song = CleanedSong.objects.get(id=song_id)
        except CleanedSong.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Song ID {song_id} not found"))
            return
        
        self.stdout.write(f"\n🔍 Enriching: {song.canonical_name}")
        
        if song.spotify_track_id:
            self.stdout.write(self.style.WARNING(
                f"   Already has Spotify ID: {song.spotify_track_id}"
            ))
            return
        
        success = service.enrich_song_with_spotify(song_id)
        
        if success:
            song.refresh_from_db()
            self.stdout.write(self.style.SUCCESS(
                f"   ✅ Enriched! Spotify ID: {song.spotify_track_id}\n"
                f"   Album: {song.album}\n"
                f"   Status: {song.status}"
            ))
        else:
            self.stdout.write(self.style.WARNING(
                f"   ❌ No Spotify match found"
            ))
    
    def _enrich_batch(self, service, limit, include_verified):
        """Enrich multiple songs."""
        # Get songs without Spotify ID
        queryset = CleanedSong.objects.filter(spotify_track_id__isnull=True)
        
        if not include_verified:
            queryset = queryset.filter(status='pending')
        
        songs = list(queryset[:limit])
        
        if not songs:
            self.stdout.write(self.style.SUCCESS(
                "\n✅ No songs need Spotify enrichment!\n"
            ))
            return
        
        self.stdout.write(f"\n🔍 Enriching {len(songs)} songs with Spotify data...\n")
        
        success_count = 0
        fail_count = 0
        
        for song in songs:
            self.stdout.write(f"  Processing: {song.canonical_name[:50]}... ", ending='')
            
            success = service.enrich_song_with_spotify(song.id)
            
            if success:
                song.refresh_from_db()
                self.stdout.write(self.style.SUCCESS("✅"))
                success_count += 1
            else:
                self.stdout.write(self.style.WARNING("❌"))
                fail_count += 1
        
        self.stdout.write(f"\n📊 Results:")
        self.stdout.write(self.style.SUCCESS(f"   ✅ Enriched: {success_count}"))
        self.stdout.write(self.style.WARNING(f"   ❌ No match: {fail_count}"))
        self.stdout.write("")
