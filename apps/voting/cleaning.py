"""
Data Cleaning Service for Radio Zimbabwe Voting Bot.

HYBRID APPROACH - Strong Fuzzy First, LLM Only on Button Press:

1️⃣ Receive raw vote - stored in RawVote/RawSongTally
2️⃣ Normalize text - lowercase, trim spaces, remove unwanted characters
3️⃣ Strong fuzzy matching - compare to VERIFIED CleanedSongs
   - If highly similar (≥92%) AND confidence gap (≥10%) → auto-merge
   - Otherwise → create PENDING entry for manual review
4️⃣ LLM Matching (GPT-4o-mini) - ONLY via admin button press
   - Admin clicks "LLM Match" button for pending items
   - Reviews and approves/rejects LLM suggestions
5️⃣ New unique songs stay PENDING until manually verified
"""
import logging
import re
from difflib import SequenceMatcher
from typing import List, Tuple, Optional, Dict
from collections import defaultdict

from django.db import transaction, IntegrityError
from django.db.models import Sum, Count
from django.utils import timezone

from .models import (
    RawVote,
    RawSongTally,
    CleanedSong,
    MatchKeyMapping,
    CleanedSongTally,
    LLMDecisionLog,
    normalize_text,
)
from .matching import (
    combined_similarity, 
    token_overlap_ratio, 
    AUTO_MERGE_THRESHOLD, 
    CONFIDENCE_GAP,
    MIN_TOKEN_OVERLAP,
)

logger = logging.getLogger(__name__)

# Similarity thresholds (imported from matching.py for consistency)
# AUTO_MERGE_THRESHOLD = 0.92  # From matching.py
# CONFIDENCE_GAP = 0.10        # From matching.py

# Spotify matching thresholds (for enrichment only)
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
    cleaned_songs: List[CleanedSong],
    verified_only: bool = True
) -> Optional[Tuple[CleanedSong, float, float]]:
    """
    Find the best matching CleanedSong using improved fuzzy matching.
    
    Uses combined similarity (character + token based) and returns
    both best and second-best scores for confidence gap check.
    
    Args:
        artist: Artist name to match
        title: Song title to match
        cleaned_songs: List of CleanedSong entries to search
        verified_only: If True, only match against verified songs
    
    Returns:
        Tuple of (CleanedSong, best_score, second_best_score) or None if no match.
    """
    if not cleaned_songs:
        return None
    
    scores: List[Tuple[CleanedSong, float]] = []
    
    clean_artist = clean_text(artist)
    clean_title = clean_text(title)
    
    for song in cleaned_songs:
        # Skip non-verified songs if verified_only is True
        if verified_only and song.status != 'verified':
            continue
            
        song_artist = clean_text(song.artist)
        song_title = clean_text(song.title)
        
        # Use combined similarity (character + token based)
        artist_score = combined_similarity(clean_artist, song_artist)
        title_score = combined_similarity(clean_title, song_title)
        
        # Check token overlap for additional safety
        artist_tokens = token_overlap_ratio(clean_artist, song_artist)
        title_tokens = token_overlap_ratio(clean_title, song_title)
        
        # Weighted score: title matters more
        score = (artist_score * SPOTIFY_ARTIST_WEIGHT) + (title_score * SPOTIFY_TITLE_WEIGHT)
        
        # Boost if good token overlap on both
        if artist_tokens >= MIN_TOKEN_OVERLAP and title_tokens >= MIN_TOKEN_OVERLAP:
            score = max(score, (artist_score + title_score) / 2)
        
        scores.append((song, score))
    
    if not scores:
        return None
    
    # Sort by score descending
    scores.sort(key=lambda x: x[1], reverse=True)
    
    best_song, best_score = scores[0]
    second_best_score = scores[1][1] if len(scores) > 1 else 0.0
    
    return (best_song, best_score, second_best_score)


class CleaningService:
    """
    Service for cleaning and grouping raw votes.
    
    HYBRID APPROACH:
    1. Strong fuzzy match against VERIFIED songs (≥92% + gap = auto-merge)
    2. Everything else → PENDING for manual review
    3. LLM (GPT-4o-mini) only called via admin button, not automatic
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
        
        HYBRID Flow (Strong Fuzzy, Manual LLM):
        1. Get all match_keys from raw tallies for the date
        2. For each unmapped match_key:
           a. Try strong fuzzy match against VERIFIED CleanedSongs
           b. If ≥92% AND confidence gap ≥10% → auto-merge
           c. Otherwise → create PENDING entry for manual review
        3. Update CleanedSongTally counts for verified songs
        4. LLM matching is done separately via admin button
        
        Args:
            date: Date to process (default: today)
            use_spotify: Whether to use Spotify for enrichment
        
        Returns:
            Dict with stats: {'auto_merged': N, 'new': N, 'pending_review': N}
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
        
        # Get VERIFIED CleanedSongs for similarity matching
        verified_songs = list(CleanedSong.objects.filter(status='verified'))
        
        stats = {
            'new': 0,
            'auto_merged': 0,
            'pending_review': 0,
        }
        
        for tally in raw_tallies:
            if tally.match_key in existing_mappings:
                continue  # Already mapped
            
            result = self._process_single_tally(tally, verified_songs)
            
            if result['action'] == 'auto_merged':
                stats['auto_merged'] += 1
            elif result['action'] == 'new':
                stats['new'] += 1
            
            if result.get('pending'):
                stats['pending_review'] += 1
        
        # Update tallies for cleaned songs
        self._update_cleaned_tallies(date)
        
        logger.info(f"Processing complete: {stats}")
        return stats
    
    def _process_single_tally(self, tally: RawSongTally, verified_songs: List[CleanedSong]) -> Dict:
        """
        Process a single raw tally using STRONG FUZZY matching only.
        
        NO automatic LLM - LLM is only called via admin button.
        
        Flow:
        1. Try strong fuzzy match against VERIFIED songs
        2. If match ≥92% AND confidence gap ≥10% → auto-merge
        3. Otherwise → create PENDING entry for manual review
        
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
        
        # Step 1: Try strong fuzzy match against verified songs
        match_result = find_best_local_match(artist, title, verified_songs, verified_only=True)
        
        if match_result:
            matched_song, best_score, second_best_score = match_result
            confidence_gap = best_score - second_best_score
            
            # SAFE AUTO-MERGE: Only if high score AND clear winner
            if best_score >= AUTO_MERGE_THRESHOLD and confidence_gap >= CONFIDENCE_GAP:
                self._create_mapping(tally, matched_song, is_auto=True)
                self._log_decision(
                    tally.display_name, 'raw_vote', 'auto_merge', 'high',
                    f'Fuzzy match: {best_score:.2f} (gap: {confidence_gap:.2f})', matched_song
                )
                logger.info(f"Auto-merged '{tally.display_name}' → '{matched_song.canonical_name}' (score: {best_score:.2f})")
                return {'action': 'auto_merged', 'cleaned_song': matched_song, 'pending': False}
        
        # Step 2: No confident match - create PENDING song for manual review
        cleaned_song = self._create_cleaned_song_from_tally(tally)
        self._create_mapping(tally, cleaned_song, is_auto=True)
        self._log_decision(
            tally.display_name, 'raw_vote', 'new', 'low',
            'No confident fuzzy match - pending manual review', None
        )
        logger.info(f"Created pending song: '{cleaned_song.canonical_name}'")
        return {'action': 'new', 'cleaned_song': cleaned_song, 'pending': True}
    
    def _log_decision(self, input_text: str, input_type: str, action: str, 
                      confidence: str, reasoning: str, matched_song: Optional[CleanedSong]):
        """Log a matching decision for auditing."""
        try:
            LLMDecisionLog.objects.create(
                input_text=input_text[:512],
                input_type=input_type,
                action=action,
                confidence=confidence,
                reasoning=reasoning[:500] if reasoning else '',
                matched_song=matched_song,
                matched_song_name=matched_song.canonical_name if matched_song else '',
                was_applied=True,
            )
        except Exception as e:
            logger.error(f"Failed to log decision: {e}")
    
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
        
        # Use get_or_create with IntegrityError fallback for race conditions
        try:
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
        except IntegrityError:
            # Race condition: another process created this song, fetch and return it
            existing = CleanedSong.objects.filter(
                canonical_name__iexact=canonical_name
            ).first()
            if existing:
                if not existing.spotify_track_id:
                    self._enrich_with_spotify(existing, track)
                logger.info(f"Found existing song after race condition: '{existing.canonical_name}'")
                return existing
            raise  # Re-raise if we still can't find it
    
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
        
        # Use get_or_create with IntegrityError fallback for race conditions
        try:
            cleaned_song = CleanedSong.objects.create(
                artist=artist,
                title=title,
                canonical_name=canonical_name,
                status='pending',  # Needs manual review
            )
            return cleaned_song
        except IntegrityError:
            # Race condition: another process created this song, fetch and return it
            existing = CleanedSong.objects.filter(
                canonical_name__iexact=canonical_name
            ).first()
            if existing:
                logger.info(f"Found existing song after race condition: '{existing.canonical_name}'")
                return existing
            raise  # Re-raise if we still can't find it
    
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
        """
        Update CleanedSongTally counts based on mappings.
        Only counts votes that are mapped to VERIFIED songs.
        """
        # Get all mappings to verified songs
        mappings = MatchKeyMapping.objects.select_related('cleaned_song').filter(
            cleaned_song__status='verified'
        )
        mapping_dict = {m.match_key: m.cleaned_song for m in mappings}
        
        # Get raw tallies for the date
        raw_tallies = RawSongTally.objects.filter(date=date)
        
        # Aggregate by cleaned_song (only verified songs)
        song_counts = defaultdict(int)
        for tally in raw_tallies:
            cleaned_song = mapping_dict.get(tally.match_key)
            if cleaned_song:  # Already filtered to verified
                song_counts[cleaned_song.id] += tally.count
        
        # Update CleanedSongTally
        with transaction.atomic():
            for song_id, count in song_counts.items():
                CleanedSongTally.objects.update_or_create(
                    date=date,
                    cleaned_song_id=song_id,
                    defaults={'count': count}
                )
        
        logger.info(f"Updated tallies for {len(song_counts)} verified songs")
    
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
