"""
Data Cleaning Service for Radio Zimbabwe Voting Bot.

LLM-ONLY APPROACH using GPT-4o-mini:

1️⃣ Receive raw vote from RawSongTally
2️⃣ Validate format is "Artist - Song"
3️⃣ Call GPT-4o-mini to match against VERIFIED songs only
4️⃣ If confident match (high confidence) → auto-merge to verified song
5️⃣ If no match or low confidence → create PENDING entry for human review
6️⃣ Never guess - only auto-merge when LLM is highly confident
"""
import json
import logging
import re
import requests
from typing import List, Optional, Dict
from collections import defaultdict

from django.conf import settings
from django.db import transaction, IntegrityError
from django.db.models import Sum
from django.utils import timezone

from .models import (
    RawSongTally,
    CleanedSong,
    MatchKeyMapping,
    CleanedSongTally,
    LLMDecisionLog,
    normalize_text,
)

logger = logging.getLogger(__name__)

# OpenAI API settings
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-4o-mini"

# Maximum verified songs to include in prompt
MAX_SONGS_IN_PROMPT = 500


def get_openai_api_key() -> str:
    """Get OpenAI API key from settings."""
    api_key = getattr(settings, 'OPENAI_API_KEY', '')
    if not api_key:
        raise RuntimeError('OPENAI_API_KEY is not configured in settings')
    return api_key


def call_openai_api(system_prompt: str, user_prompt: str) -> str:
    """
    Call OpenAI GPT-4o-mini API and return the response text.
    
    Args:
        system_prompt: System message for the model
        user_prompt: User message/query
        
    Returns:
        The model's response text
    """
    api_key = get_openai_api_key()
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "model": OPENAI_MODEL,
        "max_tokens": 2048,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.0,  # Deterministic for matching
    }
    
    try:
        response = requests.post(OPENAI_API_URL, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        choices = data.get("choices", [])
        if choices and len(choices) > 0:
            return choices[0].get("message", {}).get("content", "")
        return ""
    except requests.exceptions.RequestException as e:
        logger.error(f"OpenAI API error: {e}")
        raise


# System prompt for vote matching
MATCHING_SYSTEM_PROMPT = """You are a music database matching assistant for Radio Zimbabwe.

Your task is to match incoming song votes against a list of VERIFIED songs in the database.

RULES:
1. You will receive an incoming vote in format "Artist - Song"
2. You will receive a list of verified songs to match against
3. Match ONLY if you are HIGHLY CONFIDENT (95%+) the vote refers to a verified song
4. Handle common variations:
   - Slight spelling differences: "Winky D" vs "Winkyd"
   - Case differences: "IJIPITA" vs "Ijipita"
   - Minor typos: "Jah Prayza" vs "Jah Prayzah"
   - "ft", "feat", "featuring" variations
5. DO NOT GUESS. If you're not highly confident, return no match.
6. A vote must clearly refer to the same artist AND same song to match.

RESPONSE FORMAT (JSON only, no other text):
{
  "matched": true/false,
  "matched_song_id": <id or null>,
  "matched_song_name": "<canonical name or null>",
  "confidence": "high"/"medium"/"low"/"none",
  "reasoning": "<brief explanation>"
}

If matched=true, confidence MUST be "high". Otherwise we don't auto-merge."""


def get_verified_songs_for_prompt() -> List[Dict]:
    """
    Get list of verified songs formatted for the LLM prompt.
    
    Returns:
        List of dicts with id, artist, title, canonical_name
    """
    songs = CleanedSong.objects.filter(status='verified').order_by('artist', 'title')[:MAX_SONGS_IN_PROMPT]
    
    return [
        {
            'id': song.id,
            'artist': song.artist,
            'title': song.title,
            'canonical_name': song.canonical_name,
        }
        for song in songs
    ]


def format_songs_for_prompt(songs: List[Dict]) -> str:
    """Format the songs list for inclusion in the prompt."""
    if not songs:
        return "No verified songs in database."
    
    lines = []
    for song in songs:
        lines.append(f"ID:{song['id']} | {song['canonical_name']}")
    
    return "\n".join(lines)


def match_vote_with_llm(
    artist: str, 
    title: str, 
    verified_songs: List[Dict]
) -> Dict:
    """
    Use GPT-4o-mini to match a vote against verified songs.
    
    Args:
        artist: Artist name from the vote
        title: Song title from the vote
        verified_songs: List of verified songs to match against
        
    Returns:
        Dict with matched, matched_song_id, matched_song_name, confidence, reasoning
    """
    if not verified_songs:
        return {
            'matched': False,
            'matched_song_id': None,
            'matched_song_name': None,
            'confidence': 'none',
            'reasoning': 'No verified songs in database to match against'
        }
    
    vote_text = f"{artist} - {title}"
    songs_text = format_songs_for_prompt(verified_songs)
    
    user_prompt = f"""INCOMING VOTE: {vote_text}

VERIFIED SONGS DATABASE:
{songs_text}

Match the incoming vote to a verified song. Return JSON only."""

    try:
        response_text = call_openai_api(MATCHING_SYSTEM_PROMPT, user_prompt)
        
        # Parse JSON response
        # Clean up response if it has markdown code blocks
        response_text = response_text.strip()
        if response_text.startswith("```"):
            # Remove markdown code block
            lines = response_text.split("\n")
            response_text = "\n".join(lines[1:-1])
        
        result = json.loads(response_text)
        
        # Validate response structure
        if not isinstance(result, dict):
            raise ValueError("Response is not a dict")
        
        # Ensure required fields
        return {
            'matched': result.get('matched', False) and result.get('confidence') == 'high',
            'matched_song_id': result.get('matched_song_id'),
            'matched_song_name': result.get('matched_song_name'),
            'confidence': result.get('confidence', 'none'),
            'reasoning': result.get('reasoning', 'No reasoning provided')
        }
        
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Failed to parse LLM response: {e}")
        return {
            'matched': False,
            'matched_song_id': None,
            'matched_song_name': None,
            'confidence': 'none',
            'reasoning': f'LLM response parsing error: {str(e)}'
        }
    except Exception as e:
        logger.error(f"LLM matching error: {e}")
        return {
            'matched': False,
            'matched_song_id': None,
            'matched_song_name': None,
            'confidence': 'none',
            'reasoning': f'LLM error: {str(e)}'
        }


def is_valid_vote_format(display_name: str) -> tuple[bool, str, str]:
    """
    Check if vote is in valid "Artist - Song" format.
    
    Args:
        display_name: The vote display name to check
        
    Returns:
        Tuple of (is_valid, artist, title)
    """
    if ' - ' not in display_name:
        return False, '', ''
    
    parts = display_name.split(' - ', 1)
    if len(parts) != 2:
        return False, '', ''
    
    artist = parts[0].strip()
    title = parts[1].strip()
    
    # Both parts must have content
    if len(artist) < 2 or len(title) < 2:
        return False, '', ''
    
    return True, artist, title


class CleaningService:
    """
    Service for cleaning and matching raw votes using GPT-4o-mini.
    
    LLM-ONLY APPROACH:
    1. Validate vote format is "Artist - Song"
    2. Use GPT-4o-mini to match against verified songs
    3. Auto-merge only if high confidence match
    4. Everything else → PENDING for human review
    """
    
    def __init__(self):
        self._verified_songs_cache = None
        self._cache_time = None
    
    def _get_verified_songs(self, force_refresh: bool = False) -> List[Dict]:
        """Get verified songs with caching (5 minute cache)."""
        now = timezone.now()
        
        if (
            force_refresh 
            or self._verified_songs_cache is None 
            or self._cache_time is None
            or (now - self._cache_time).total_seconds() > 300
        ):
            self._verified_songs_cache = get_verified_songs_for_prompt()
            self._cache_time = now
            
        return self._verified_songs_cache
    
    def process_new_votes(self, date=None, **kwargs) -> Dict:
        """
        Process raw votes and create/update cleaned entries using LLM.
        
        Flow:
        1. Get all unmapped match_keys from raw tallies for the date
        2. For each unmapped vote:
           a. Check if format is valid "Artist - Song"
           b. If invalid format → create PENDING entry
           c. If valid, use LLM to match against verified songs
           d. If high confidence match → auto-merge to verified song
           e. If no match or low confidence → create PENDING entry
        3. Update CleanedSongTally counts for verified songs
        
        Args:
            date: Date to process (default: today)
            **kwargs: Ignored (for backward compatibility)
        
        Returns:
            Dict with stats: {'auto_merged': N, 'new_pending': N, 'invalid_format': N, 'llm_errors': N}
        """
        if date is None:
            date = timezone.localdate()
        
        logger.info(f"Processing votes for {date} using LLM matching")
        
        # Get all match_keys from today's raw tallies
        raw_tallies = RawSongTally.objects.filter(date=date)
        
        # Get existing mappings
        existing_mappings = set(
            MatchKeyMapping.objects.values_list('match_key', flat=True)
        )
        
        # Get verified songs for LLM prompt
        verified_songs = self._get_verified_songs(force_refresh=True)
        
        stats = {
            'auto_merged': 0,
            'new_pending': 0,
            'invalid_format': 0,
            'llm_errors': 0,
            'already_mapped': 0,
        }
        
        for tally in raw_tallies:
            if tally.match_key in existing_mappings:
                stats['already_mapped'] += 1
                continue
            
            result = self._process_single_tally(tally, verified_songs)
            
            if result['action'] == 'auto_merged':
                stats['auto_merged'] += 1
            elif result['action'] == 'new_pending':
                stats['new_pending'] += 1
            elif result['action'] == 'invalid_format':
                stats['invalid_format'] += 1
            elif result['action'] == 'llm_error':
                stats['llm_errors'] += 1
        
        # Update tallies for cleaned songs (verified only)
        self._update_cleaned_tallies(date)
        
        logger.info(f"Processing complete: {stats}")
        return stats
    
    def _process_single_tally(self, tally: RawSongTally, verified_songs: List[Dict]) -> Dict:
        """
        Process a single raw tally using LLM matching.
        
        Flow:
        1. Validate format is "Artist - Song"
        2. If invalid → create PENDING entry
        3. If valid → use LLM to match against verified songs
        4. If high confidence match → auto-merge
        5. If no match → create PENDING entry
        
        Returns:
            Dict with 'action' and 'cleaned_song' keys
        """
        # Step 1: Validate format
        is_valid, artist, title = is_valid_vote_format(tally.display_name)
        
        if not is_valid:
            # Invalid format → create pending entry
            cleaned_song = self._create_pending_song(tally)
            self._create_mapping(tally, cleaned_song, is_auto=False)
            self._log_decision(
                tally.display_name, 'raw_vote', 'new', 'none',
                'Invalid format - not "Artist - Song"', None
            )
            logger.info(f"Invalid format, created pending: '{tally.display_name}'")
            return {'action': 'invalid_format', 'cleaned_song': cleaned_song}
        
        # Step 2: Use LLM to match against verified songs
        if not verified_songs:
            # No verified songs to match against - create pending
            cleaned_song = self._create_pending_song(tally, artist, title)
            self._create_mapping(tally, cleaned_song, is_auto=False)
            self._log_decision(
                tally.display_name, 'raw_vote', 'new', 'none',
                'No verified songs in database', None
            )
            logger.info(f"No verified songs, created pending: '{tally.display_name}'")
            return {'action': 'new_pending', 'cleaned_song': cleaned_song}
        
        try:
            llm_result = match_vote_with_llm(artist, title, verified_songs)
        except Exception as e:
            # LLM error → create pending entry
            logger.error(f"LLM error for '{tally.display_name}': {e}")
            cleaned_song = self._create_pending_song(tally, artist, title)
            self._create_mapping(tally, cleaned_song, is_auto=False)
            self._log_decision(
                tally.display_name, 'raw_vote', 'new', 'none',
                f'LLM error: {str(e)}', None
            )
            return {'action': 'llm_error', 'cleaned_song': cleaned_song}
        
        # Step 3: Process LLM result
        if llm_result['matched'] and llm_result['matched_song_id']:
            # High confidence match → auto-merge
            try:
                matched_song = CleanedSong.objects.get(
                    id=llm_result['matched_song_id'],
                    status='verified'
                )
                self._create_mapping(tally, matched_song, is_auto=True)
                self._log_decision(
                    tally.display_name, 'raw_vote', 'auto_merge', 'high',
                    llm_result['reasoning'], matched_song
                )
                logger.info(f"Auto-merged '{tally.display_name}' → '{matched_song.canonical_name}'")
                return {'action': 'auto_merged', 'cleaned_song': matched_song}
            except CleanedSong.DoesNotExist:
                logger.warning(f"LLM matched to non-existent song ID: {llm_result['matched_song_id']}")
                # Fall through to create pending
        
        # Step 4: No confident match → create pending entry
        cleaned_song = self._create_pending_song(tally, artist, title)
        self._create_mapping(tally, cleaned_song, is_auto=False)
        self._log_decision(
            tally.display_name, 'raw_vote', 'new', llm_result['confidence'],
            llm_result['reasoning'], None
        )
        logger.info(f"No confident match, created pending: '{tally.display_name}'")
        return {'action': 'new_pending', 'cleaned_song': cleaned_song}
    
    def _create_pending_song(
        self, 
        tally: RawSongTally, 
        artist: str = None, 
        title: str = None
    ) -> CleanedSong:
        """
        Create a new CleanedSong in pending status.
        
        Args:
            tally: The raw tally to create from
            artist: Optional artist name (if already parsed)
            title: Optional title (if already parsed)
            
        Returns:
            The created or existing CleanedSong
        """
        if artist is None or title is None:
            # Try to parse from display_name
            parts = tally.display_name.split(' - ', 1)
            if len(parts) == 2:
                artist = parts[0].strip().title()
                title = parts[1].strip().title()
            else:
                artist = "Unknown"
                title = tally.display_name.strip().title()
        else:
            artist = artist.strip().title()
            title = title.strip().title()
        
        canonical_name = f"{artist} - {title}"
        
        # Check for existing song (case-insensitive)
        existing = CleanedSong.objects.filter(
            canonical_name__iexact=canonical_name
        ).first()
        
        if existing:
            logger.info(f"Found existing song: '{existing.canonical_name}'")
            return existing
        
        # Create new pending song
        try:
            cleaned_song = CleanedSong.objects.create(
                artist=artist,
                title=title,
                canonical_name=canonical_name,
                status='pending',
            )
            return cleaned_song
        except IntegrityError:
            # Race condition - fetch existing
            existing = CleanedSong.objects.filter(
                canonical_name__iexact=canonical_name
            ).first()
            if existing:
                return existing
            raise
    
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
    
    def _log_decision(
        self, 
        input_text: str, 
        input_type: str, 
        action: str, 
        confidence: str, 
        reasoning: str, 
        matched_song: Optional[CleanedSong]
    ):
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
        
        logger.info(f"Updated tallies for {len(song_counts)} verified songs on {date}")
    
    # =========================================================================
    # Manual review helper methods
    # =========================================================================
    
    def get_pending_review(self) -> List[CleanedSong]:
        """Get all songs pending review."""
        return list(CleanedSong.objects.filter(status='pending').order_by('-created_at'))
    
    def verify_song(self, song_id: int) -> bool:
        """Mark a song as verified."""
        try:
            song = CleanedSong.objects.get(id=song_id)
            song.status = 'verified'
            song.save()
            # Clear cache so new votes can match against this song
            self._verified_songs_cache = None
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
    
    def get_merge_suggestions(self) -> List[Dict]:
        """
        Get suggestions for songs that might be duplicates.
        Uses simple text comparison to find potential matches.
        """
        pending = CleanedSong.objects.filter(status='pending')
        verified = CleanedSong.objects.filter(status='verified')
        
        suggestions = []
        
        for p in pending:
            p_artist = normalize_text(p.artist)
            p_title = normalize_text(p.title)
            
            for v in verified:
                v_artist = normalize_text(v.artist)
                v_title = normalize_text(v.title)
                
                # Simple check: if artist and title are very similar
                if (
                    p_artist == v_artist and p_title == v_title
                ) or (
                    p.canonical_name.lower() == v.canonical_name.lower()
                ):
                    suggestions.append({
                        'pending_song': p,
                        'verified_song': v,
                        'reason': 'Exact match (case-insensitive)'
                    })
        
        return suggestions
    
    # =========================================================================
    # Spotify enrichment (optional)
    # =========================================================================
    
    def enrich_song_with_spotify(self, song_id: int) -> bool:
        """
        Manually trigger Spotify enrichment for a song.
        """
        try:
            song = CleanedSong.objects.get(id=song_id)
            
            if song.spotify_track_id:
                logger.info(f"Song already has Spotify ID: {song.canonical_name}")
                return True
            
            # Try to import and use Spotify search
            try:
                from apps.spotify.search import resolve_with_confidence
                result, confidence = resolve_with_confidence(song.artist, song.title)
                
                if result and confidence >= 0.8:
                    song.spotify_track_id = result['id']
                    song.album = result.get('album', '') or song.album
                    song.image_url = result.get('image_url', '') or song.image_url
                    song.preview_url = result.get('preview_url', '') or song.preview_url
                    song.save()
                    logger.info(f"Enriched '{song.canonical_name}' with Spotify data")
                    return True
                    
            except ImportError:
                logger.warning("Spotify search module not available")
            except Exception as e:
                logger.error(f"Spotify search error: {e}")
            
            logger.warning(f"No Spotify match found for: {song.canonical_name}")
            return False
            
        except CleanedSong.DoesNotExist:
            return False
