"""
Data Cleaning Service for Radio Zimbabwe Voting Bot.

Enhanced Cleaning Flow with Spotify Integration:
1️⃣ Receive raw vote - stored in RawVote/RawSongTally
2️⃣ Normalize text - lowercase, trim spaces, remove unwanted characters
3️⃣ Local fuzzy matching - compare to existing CleanedSongs
   - If highly similar (≥90%) → auto-merge
   - If unsure (75-90%) → try Spotify verification
   - If low match (<75%) → Spotify search for new song
4️⃣ Spotify Search (fallback matching)
   - Multiple search strategies (combined, track-only, artist-only)
   - Compare official names using fuzzy similarity
   - If scores meet thresholds → confirm match
5️⃣ Assign Spotify ID - guarantees future votes match correctly
"""
import logging
import re
from difflib import SequenceMatcher
from typing import List, Tuple, Optional, Dict
from collections import defaultdict

from django.db import transaction
from django.db.models import Sum, Count
from django.utils import timezone

from .models import (
    RawVote,
    RawSongTally,
    CleanedSong,
    MatchKeyMapping,
    CleanedSongTally,
    normalize_text,
)

logger = logging.getLogger(__name__)

# Similarity thresholds for local matching
AUTO_MERGE_THRESHOLD = 0.90      # Auto-merge to existing CleanedSong
SPOTIFY_CHECK_THRESHOLD = 0.75  # Check Spotify if between 75-90%
NEW_SONG_THRESHOLD = 0.75       # Below this, treat as new song

# Spotify matching thresholds
SPOTIFY_CONFIRM_THRESHOLD = 0.80  # Accept Spotify match if ≥80%
SPOTIFY_ARTIST_WEIGHT = 0.4       # Weight for artist similarity
SPOTIFY_TITLE_WEIGHT = 0.6        # Weight for title similarity


def string_similarity(a: str, b: str) -> float:
    """Calculate similarity ratio between two strings."""
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def clean_text(text: str) -> str:
    """
    Clean text for better matching:
    - Remove emojis and special characters
    - Normalize whitespace
    - Lowercase
    """
    # Remove emojis and most special characters (keep alphanumeric, spaces, hyphens)
    text = re.sub(r'[^\w\s\-\']', '', text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip().lower()


def find_best_local_match(
    artist: str, 
    title: str, 
    cleaned_songs: List[CleanedSong]
) -> Optional[Tuple[CleanedSong, float]]:
    """
    Find the best matching CleanedSong using fuzzy matching.
    
    Returns:
        Tuple of (CleanedSong, similarity_score) or None if no good match.
    """
    if not cleaned_songs:
        return None
    
    best_match = None
    best_score = 0
    
    clean_artist = clean_text(artist)
    clean_title = clean_text(title)
    
    for song in cleaned_songs:
        song_artist = clean_text(song.artist)
        song_title = clean_text(song.title)
        
        # Calculate weighted similarity
        artist_score = string_similarity(clean_artist, song_artist)
        title_score = string_similarity(clean_title, song_title)
        combined_score = (artist_score * SPOTIFY_ARTIST_WEIGHT) + (title_score * SPOTIFY_TITLE_WEIGHT)
        
        if combined_score > best_score:
            best_score = combined_score
            best_match = song
    
    if best_match and best_score >= NEW_SONG_THRESHOLD:
        return (best_match, best_score)
    return None


class CleaningService:
    """
    Service for cleaning and grouping raw votes with Spotify integration.
    """
    
    def __init__(self):
        self._spotify_enabled = True
        self._spotify_client = None
    
    def _get_spotify_search(self):
        """Lazy load Spotify search module."""
        try:
            from apps.spotify.search import resolve_with_confidence, SpotifyNotConfiguredError
            return resolve_with_confidence
        except ImportError:
            logger.warning("Spotify search module not available")
            return None
    
    def _search_spotify(self, artist: str, title: str) -> Optional[Tuple[dict, float]]:
        """
        Search Spotify for a song and return match with confidence.
        
        Returns:
            Tuple of (spotify_track_dict, confidence) or None
        """
        if not self._spotify_enabled:
            return None
        
        resolve_fn = self._get_spotify_search()
        if not resolve_fn:
            return None
        
        try:
            from apps.spotify.search import SpotifyNotConfiguredError, SpotifyLookupError
            
            result, confidence = resolve_fn(artist, title)
            if result and confidence >= SPOTIFY_CONFIRM_THRESHOLD:
                return (result, confidence)
            return None
            
        except Exception as e:
            logger.warning(f"Spotify search failed: {e}")
            return None
    
    def _find_by_spotify_id(self, spotify_id: str) -> Optional[CleanedSong]:
        """Find CleanedSong by Spotify track ID."""
        try:
            return CleanedSong.objects.get(spotify_track_id=spotify_id)
        except CleanedSong.DoesNotExist:
            return None
    
    def process_new_votes(self, date=None, use_spotify=True):
        """
        Process raw votes and create/update cleaned entries.
        
        Flow:
        1. Get all match_keys from raw tallies for the date
        2. For each unmapped match_key:
           a. Try local fuzzy match against existing CleanedSongs
           b. If ≥90% → auto-merge
           c. If 75-90% → verify with Spotify
           d. If <75% or no local match → search Spotify for new song
           e. If Spotify finds match → create/merge with Spotify data
           f. If no Spotify → create pending CleanedSong
        3. Update CleanedSongTally counts
        
        Args:
            date: Date to process (default: today)
            use_spotify: Whether to use Spotify for verification
        
        Returns:
            Dict with stats: {'new': N, 'auto_merged': N, 'spotify_matched': N}
        """
        if date is None:
            date = timezone.localdate()
        
        self._spotify_enabled = use_spotify
        
        logger.info(f"Processing votes for {date} (Spotify: {'enabled' if use_spotify else 'disabled'})")
        
        # Get all match_keys from today's raw tallies
        raw_tallies = RawSongTally.objects.filter(date=date)
        
        # Get existing mappings
        existing_mappings = set(
            MatchKeyMapping.objects.values_list('match_key', flat=True)
        )
        
        # Get all CleanedSongs for similarity matching
        cleaned_songs = list(CleanedSong.objects.all())
        
        stats = {
            'new': 0,
            'auto_merged': 0,
            'spotify_matched': 0,
            'pending_review': 0,
        }
        
        for tally in raw_tallies:
            if tally.match_key in existing_mappings:
                continue  # Already mapped
            
            result = self._process_single_tally(tally, cleaned_songs)
            
            if result['action'] == 'auto_merged':
                stats['auto_merged'] += 1
            elif result['action'] == 'spotify_matched':
                stats['spotify_matched'] += 1
                if result.get('cleaned_song'):
                    cleaned_songs.append(result['cleaned_song'])
            elif result['action'] == 'new':
                stats['new'] += 1
                if result.get('cleaned_song'):
                    cleaned_songs.append(result['cleaned_song'])
            
            if result.get('pending'):
                stats['pending_review'] += 1
        
        # Update tallies for cleaned songs
        self._update_cleaned_tallies(date)
        
        logger.info(f"Processing complete: {stats}")
        return stats
    
    def _process_single_tally(self, tally: RawSongTally, cleaned_songs: List[CleanedSong]) -> Dict:
        """
        Process a single raw tally and return the result.
        
        Returns:
            Dict with 'action', 'cleaned_song', 'pending' keys
        """
        # Parse the display name
        parts = tally.display_name.split(' - ', 1)
        if len(parts) == 2:
            artist, title = parts
        else:
            artist = "Unknown"
            title = tally.display_name
        
        artist = artist.strip()
        title = title.strip()
        
        # Step 1: Local fuzzy matching
        local_match = find_best_local_match(artist, title, cleaned_songs)
        
        if local_match and local_match[1] >= AUTO_MERGE_THRESHOLD:
            # High confidence local match - auto-merge
            matched_song, score = local_match
            self._create_mapping(tally, matched_song, is_auto=True)
            logger.info(f"Auto-merged '{tally.display_name}' → '{matched_song.canonical_name}' ({score:.0%})")
            return {'action': 'auto_merged', 'cleaned_song': matched_song, 'pending': False}
        
        # Step 2: Try Spotify search
        spotify_result = self._search_spotify(artist, title)
        
        if spotify_result:
            track, confidence = spotify_result
            spotify_id = track['id']
            
            # Check if we already have a CleanedSong with this Spotify ID
            existing_by_spotify = self._find_by_spotify_id(spotify_id)
            
            if existing_by_spotify:
                # Merge with existing song that has same Spotify ID
                self._create_mapping(tally, existing_by_spotify, is_auto=True)
                logger.info(f"Spotify ID match '{tally.display_name}' → '{existing_by_spotify.canonical_name}'")
                return {'action': 'spotify_matched', 'cleaned_song': existing_by_spotify, 'pending': False}
            
            # Check if local match exists and update with Spotify data
            if local_match and local_match[1] >= SPOTIFY_CHECK_THRESHOLD:
                matched_song, _ = local_match
                # Update existing song with Spotify data if it doesn't have it
                if not matched_song.spotify_track_id:
                    self._enrich_with_spotify(matched_song, track)
                self._create_mapping(tally, matched_song, is_auto=True)
                logger.info(f"Enriched & merged '{tally.display_name}' → '{matched_song.canonical_name}' (Spotify: {confidence:.0%})")
                return {'action': 'spotify_matched', 'cleaned_song': matched_song, 'pending': False}
            
            # Create new CleanedSong with Spotify data (verified!)
            cleaned_song = self._create_cleaned_song_from_spotify(track)
            self._create_mapping(tally, cleaned_song, is_auto=True)
            logger.info(f"Created verified song from Spotify: '{cleaned_song.canonical_name}'")
            return {'action': 'spotify_matched', 'cleaned_song': cleaned_song, 'pending': False}
        
        # Step 3: No Spotify match - create pending song
        if local_match and local_match[1] >= SPOTIFY_CHECK_THRESHOLD:
            # Moderate local match, use it but mark pending
            matched_song, score = local_match
            self._create_mapping(tally, matched_song, is_auto=False)
            logger.info(f"Tentative match '{tally.display_name}' → '{matched_song.canonical_name}' ({score:.0%}) - pending review")
            return {'action': 'auto_merged', 'cleaned_song': matched_song, 'pending': True}
        
        # Create brand new pending song
        cleaned_song = self._create_cleaned_song_from_tally(tally)
        self._create_mapping(tally, cleaned_song, is_auto=True)
        logger.info(f"Created new pending song: '{cleaned_song.canonical_name}'")
        return {'action': 'new', 'cleaned_song': cleaned_song, 'pending': True}
    
    def _create_cleaned_song_from_spotify(self, track: dict) -> CleanedSong:
        """Create a CleanedSong with Spotify data (auto-verified)."""
        artist = ', '.join(track['artists'])
        title = track['title']
        canonical_name = f"{artist} - {title}"
        
        # Check for case-insensitive duplicate before creating
        existing = CleanedSong.objects.filter(
            canonical_name__iexact=canonical_name
        ).first()
        
        if existing:
            # Update existing with Spotify data if it doesn't have it
            if not existing.spotify_track_id:
                self._enrich_with_spotify(existing, track)
            logger.info(f"Found existing song (case-insensitive): '{existing.canonical_name}'")
            return existing
        
        cleaned_song = CleanedSong.objects.create(
            artist=artist,
            title=title,
            canonical_name=canonical_name,
            spotify_track_id=track['id'],
            album=track.get('album', ''),
            image_url=track.get('image_url', ''),
            preview_url=track.get('preview_url', ''),
            status='verified',  # Auto-verified because Spotify confirmed
        )
        return cleaned_song
    
    def _create_cleaned_song_from_tally(self, tally: RawSongTally) -> CleanedSong:
        """Create a new CleanedSong from a raw tally (pending review)."""
        # Parse display_name to get artist and title
        parts = tally.display_name.split(' - ', 1)
        if len(parts) == 2:
            artist, title = parts
        else:
            artist = "Unknown"
            title = tally.display_name
        
        # Title case for nicer display
        artist = artist.strip().title()
        title = title.strip().title()
        
        canonical_name = f"{artist} - {title}"
        
        # Check for case-insensitive duplicate before creating
        existing = CleanedSong.objects.filter(
            canonical_name__iexact=canonical_name
        ).first()
        
        if existing:
            logger.info(f"Found existing song (case-insensitive): '{existing.canonical_name}'")
            return existing
        
        cleaned_song = CleanedSong.objects.create(
            artist=artist,
            title=title,
            canonical_name=canonical_name,
            status='pending',  # Needs manual review
        )
        return cleaned_song
    
    def _enrich_with_spotify(self, song: CleanedSong, track: dict):
        """Update an existing CleanedSong with Spotify data."""
        song.spotify_track_id = track['id']
        song.album = track.get('album', '') or song.album
        song.image_url = track.get('image_url', '') or song.image_url
        song.preview_url = track.get('preview_url', '') or song.preview_url
        # Auto-verify since Spotify confirmed
        if song.status == 'pending':
            song.status = 'verified'
        song.save()
        logger.info(f"Enriched '{song.canonical_name}' with Spotify data")
    
    def _create_mapping(self, tally: RawSongTally, cleaned_song: CleanedSong, is_auto: bool):
        """Create a mapping from match_key to CleanedSong."""
        MatchKeyMapping.objects.update_or_create(
            match_key=tally.match_key,
            defaults={
                'cleaned_song': cleaned_song,
                'sample_display_name': tally.display_name,
                'vote_count': tally.count,
                'is_auto_mapped': is_auto,
            }
        )
    
    def _update_cleaned_tallies(self, date):
        """Update CleanedSongTally counts based on mappings."""
        # Get all mappings
        mappings = MatchKeyMapping.objects.select_related('cleaned_song').all()
        mapping_dict = {m.match_key: m.cleaned_song for m in mappings}
        
        # Get raw tallies for the date
        raw_tallies = RawSongTally.objects.filter(date=date)
        
        # Aggregate by cleaned_song
        song_counts = defaultdict(int)
        for tally in raw_tallies:
            cleaned_song = mapping_dict.get(tally.match_key)
            if cleaned_song:
                song_counts[cleaned_song.id] += tally.count
        
        # Update CleanedSongTally
        with transaction.atomic():
            for song_id, count in song_counts.items():
                CleanedSongTally.objects.update_or_create(
                    date=date,
                    cleaned_song_id=song_id,
                    defaults={'count': count}
                )
        
        logger.info(f"Updated tallies for {len(song_counts)} cleaned songs")
    
    def get_pending_review(self) -> List[CleanedSong]:
        """Get all songs pending review."""
        return list(CleanedSong.objects.filter(status='pending').order_by('-created_at'))
    
    def verify_song(self, song_id: int) -> bool:
        """Mark a song as verified."""
        try:
            song = CleanedSong.objects.get(id=song_id)
            song.status = 'verified'
            song.save()
            return True
        except CleanedSong.DoesNotExist:
            return False
    
    def reject_song(self, song_id: int) -> bool:
        """Mark a song as rejected."""
        try:
            song = CleanedSong.objects.get(id=song_id)
            song.status = 'rejected'
            song.save()
            return True
        except CleanedSong.DoesNotExist:
            return False
    
    def merge_songs(self, source_id: int, target_id: int) -> bool:
        """
        Merge one CleanedSong into another.
        All mappings from source are moved to target, then source is deleted.
        """
        try:
            source = CleanedSong.objects.get(id=source_id)
            target = CleanedSong.objects.get(id=target_id)
            
            with transaction.atomic():
                # Move all mappings from source to target
                MatchKeyMapping.objects.filter(cleaned_song=source).update(cleaned_song=target)
                
                # Move all tallies from source to target (combine counts)
                for tally in CleanedSongTally.objects.filter(cleaned_song=source):
                    existing, created = CleanedSongTally.objects.get_or_create(
                        date=tally.date,
                        cleaned_song=target,
                        defaults={'count': 0}
                    )
                    existing.count += tally.count
                    existing.save()
                    tally.delete()
                
                # Delete source
                source.delete()
            
            logger.info(f"Merged '{source.canonical_name}' into '{target.canonical_name}'")
            return True
        except CleanedSong.DoesNotExist:
            return False
    
    def enrich_song_with_spotify(self, song_id: int) -> bool:
        """
        Manually trigger Spotify enrichment for a song.
        """
        try:
            song = CleanedSong.objects.get(id=song_id)
            
            if song.spotify_track_id:
                logger.info(f"Song already has Spotify ID: {song.canonical_name}")
                return True
            
            result = self._search_spotify(song.artist, song.title)
            if result:
                track, confidence = result
                self._enrich_with_spotify(song, track)
                return True
            
            logger.warning(f"No Spotify match found for: {song.canonical_name}")
            return False
            
        except CleanedSong.DoesNotExist:
            return False
