"""
Data Cleaning Service for Radio Zimbabwe Voting Bot.

Enhanced Cleaning Flow with LLM Integration (Anthropic Claude):
1️⃣ Receive raw vote - stored in RawVote/RawSongTally
2️⃣ Normalize text - lowercase, trim spaces, remove unwanted characters
3️⃣ Local fuzzy matching - compare to existing VERIFIED CleanedSongs
   - If highly similar (≥90%) → auto-merge & correct vote to match verified
   - If unsure (75-90%) → use LLM for smart matching
   - If low match (<75%) → use LLM to decide: match/reject/new
4️⃣ LLM Matching (Anthropic Claude) - for uncertain cases
   - Analyze vote against verified songs database
   - If LLM finds high-confidence match → auto-link & correct vote
   - If LLM unsure → leave as pending for manual review
5️⃣ Vote Correction - when matched, update vote to use verified song's canonical name
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

logger = logging.getLogger(__name__)

# Similarity thresholds for local matching
AUTO_MERGE_THRESHOLD = 0.90      # Auto-merge to existing CleanedSong
LLM_CHECK_THRESHOLD = 0.75       # Use LLM if between 75-90%
NEW_SONG_THRESHOLD = 0.75        # Below this, treat as new song (send to LLM)

# LLM matching thresholds
LLM_HIGH_CONFIDENCE = 'high'     # Auto-link LLM matches with high confidence
LLM_MEDIUM_CONFIDENCE = 'medium' # Auto-link LLM matches with medium confidence

# Spotify matching thresholds (legacy, keeping for enrichment)
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
) -> Optional[Tuple[CleanedSong, float]]:
    """
    Find the best matching CleanedSong using fuzzy matching.
    
    Args:
        artist: Artist name to match
        title: Song title to match
        cleaned_songs: List of CleanedSong entries to search
        verified_only: If True, only match against verified songs
    
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
        # Skip non-verified songs if verified_only is True
        if verified_only and song.status != 'verified':
            continue
            
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
    Service for cleaning and grouping raw votes with LLM and Spotify integration.
    
    Primary matching flow:
    1. Local fuzzy match against VERIFIED songs (≥90% = auto-merge)
    2. LLM matching for uncertain cases (Anthropic Claude)
    3. Spotify enrichment for verified matches
    4. Create pending entries for truly new songs
    """
    
    def __init__(self):
        self._spotify_enabled = True
        self._llm_enabled = True
        self._spotify_client = None
    
    def _get_llm_matcher(self):
        """Lazy load LLM matcher module."""
        try:
            from . import llm_matcher
            return llm_matcher
        except ImportError:
            logger.warning("LLM matcher module not available")
            return None
    
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
    
    def _match_with_llm(self, display_name: str, match_key: str, verified_songs: List[Dict]) -> Optional[Dict]:
        """
        Use LLM to match a vote against verified songs.
        
        Returns:
            Dict with match result or None if LLM is not available/fails
        """
        if not self._llm_enabled:
            return None
        
        llm = self._get_llm_matcher()
        if not llm:
            return None
        
        try:
            # Use single vote matching
            vote_data = [{
                'display_name': display_name,
                'match_key': match_key,
                'vote_count': 1,
            }]
            
            results = llm.match_votes_with_llm(vote_data, verified_songs)
            
            if results and len(results) > 0:
                result = results[0]
                return {
                    'matched_song_id': result.matched_song_id,
                    'matched_song_name': result.matched_song_name,
                    'confidence': result.confidence,
                    'reasoning': result.reasoning,
                    'should_auto_link': result.should_auto_link,
                }
            return None
            
        except Exception as e:
            logger.warning(f"LLM matching failed: {e}")
            return None
    
    def _find_by_spotify_id(self, spotify_id: str) -> Optional[CleanedSong]:
        """Find CleanedSong by Spotify track ID."""
        try:
            return CleanedSong.objects.get(spotify_track_id=spotify_id)
        except CleanedSong.DoesNotExist:
            return None
    
    def process_new_votes(self, date=None, use_spotify=True, use_llm=True):
        """
        Process raw votes and create/update cleaned entries.
        
        Enhanced Flow with LLM:
        1. Get all match_keys from raw tallies for the date
        2. For each unmapped match_key:
           a. Try local fuzzy match against existing VERIFIED CleanedSongs
           b. If ≥90% → auto-merge & correct vote to match verified song
           c. If 75-90% → use LLM for smart matching
           d. If <75% or no local match → use LLM to decide
           e. If LLM finds high/medium confidence match → auto-link & correct
           f. If LLM unsure → create pending CleanedSong for manual review
        3. Update CleanedSongTally counts for verified songs
        
        Args:
            date: Date to process (default: today)
            use_spotify: Whether to use Spotify for enrichment
            use_llm: Whether to use LLM for matching
        
        Returns:
            Dict with stats: {'matched': N, 'pending': N, 'llm_matched': N}
        """
        if date is None:
            date = timezone.localdate()
        
        self._spotify_enabled = use_spotify
        self._llm_enabled = use_llm
        
        logger.info(f"Processing votes for {date} (LLM: {'enabled' if use_llm else 'disabled'}, Spotify: {'enabled' if use_spotify else 'disabled'})")
        
        # Get all match_keys from today's raw tallies
        raw_tallies = RawSongTally.objects.filter(date=date)
        
        # Get existing mappings
        existing_mappings = set(
            MatchKeyMapping.objects.values_list('match_key', flat=True)
        )
        
        # Get VERIFIED CleanedSongs for similarity matching (only match against verified!)
        verified_songs = list(CleanedSong.objects.filter(status='verified'))
        
        # Prepare verified songs list for LLM
        verified_songs_data = [
            {
                'id': song.id,
                'artist': song.artist,
                'title': song.title,
                'canonical_name': song.canonical_name,
                'spotify_id': song.spotify_track_id or '',
            }
            for song in verified_songs
        ]
        
        stats = {
            'new': 0,
            'auto_merged': 0,
            'llm_matched': 0,
            'spotify_enriched': 0,
            'pending_review': 0,
        }
        
        for tally in raw_tallies:
            if tally.match_key in existing_mappings:
                continue  # Already mapped
            
            result = self._process_single_tally(tally, verified_songs, verified_songs_data)
            
            if result['action'] == 'auto_merged':
                stats['auto_merged'] += 1
            elif result['action'] == 'llm_matched':
                stats['llm_matched'] += 1
            elif result['action'] == 'spotify_matched':
                stats['spotify_enriched'] += 1
                if result.get('cleaned_song'):
                    verified_songs.append(result['cleaned_song'])
            elif result['action'] == 'new':
                stats['new'] += 1
                if result.get('cleaned_song'):
                    verified_songs.append(result['cleaned_song'])
            
            if result.get('pending'):
                stats['pending_review'] += 1
        
        # Update tallies for cleaned songs
        self._update_cleaned_tallies(date)
        
        logger.info(f"Processing complete: {stats}")
        return stats
    
    def _process_single_tally(self, tally: RawSongTally, verified_songs: List[CleanedSong], verified_songs_data: List[Dict]) -> Dict:
        """
        Process a single raw tally and return the result.
        
        LLM-only matching flow (no fuzzy matching to avoid false positives):
        1. Use LLM (Anthropic Claude) for smart matching against VERIFIED songs
        2. If LLM high/medium confidence → auto-link to verified song
        3. If LLM unsure → create pending entry for manual review
        
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
        
        # Step 1: Use LLM matching ONLY (no fuzzy matching to avoid false positives)
        if self._llm_enabled and verified_songs_data:
            llm_result = self._match_with_llm(tally.display_name, tally.match_key, verified_songs_data)
            
            if llm_result and llm_result.get('matched_song_id'):
                confidence = llm_result.get('confidence', 'low')
                
                # Auto-link high and medium confidence LLM matches
                if confidence in (LLM_HIGH_CONFIDENCE, LLM_MEDIUM_CONFIDENCE):
                    matched_song_id = llm_result['matched_song_id']
                    matched_song = next((s for s in verified_songs if s.id == matched_song_id), None)
                    
                    if matched_song:
                        self._create_mapping(tally, matched_song, is_auto=True)
                        self._log_llm_decision(
                            tally.display_name, 'raw_vote', 'auto_merge', confidence,
                            llm_result.get('reasoning', 'LLM matched'), matched_song
                        )
                        logger.info(f"LLM matched '{tally.display_name}' → '{matched_song.canonical_name}' ({confidence})")
                        return {'action': 'llm_matched', 'cleaned_song': matched_song, 'pending': False}
            
            # LLM didn't find a confident match - leave as pending
            logger.info(f"LLM no confident match for '{tally.display_name}' - creating pending")
        
        # Step 2: No LLM match - create pending song for manual review
        cleaned_song = self._create_cleaned_song_from_tally(tally)
        self._create_mapping(tally, cleaned_song, is_auto=True)
        self._log_llm_decision(
            tally.display_name, 'raw_vote', 'new', 'low',
            'No confident LLM match - pending manual review', None
        )
        logger.info(f"Created new pending song: '{cleaned_song.canonical_name}'")
        return {'action': 'new', 'cleaned_song': cleaned_song, 'pending': True}
    
    def _log_llm_decision(self, input_text: str, input_type: str, action: str, 
                          confidence: str, reasoning: str, matched_song: Optional[CleanedSong]):
        """Log an LLM/auto decision for auditing."""
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
