"""
LLM-powered vote matching using Anthropic Claude.

This module handles messy votes that don't follow the "artist - song" format:
- Just a song name: "Ibotso"
- Just a number: "1" (meaning #1 song?)
- Wrong format: "Winky D Ijipita" (no dash)
- Typos: "winkyd ijipitha"

The LLM matches these against your verified CleanedSong database.
"""
import json
import logging
import requests
from typing import Optional, List, Tuple, Dict
from dataclasses import dataclass

from django.conf import settings
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from .models import (
    CleanedSong,
    RawSongTally,
    MatchKeyMapping,
    CleanedSongTally,
    LLMDecisionLog,
    normalize_text,
)

logger = logging.getLogger(__name__)

# Maximum songs to include in prompt (to stay within token limits)
MAX_SONGS_IN_PROMPT = 500  # Claude can handle larger contexts
# Batch size for processing
BATCH_SIZE = 20  # Claude handles larger batches well

# Anthropic API settings
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-3-haiku-20240307"  # Fast and cost-effective model


def get_anthropic_api_key() -> str:
    """Get Anthropic API key from settings."""
    api_key = getattr(settings, 'ANTHROPIC_API_KEY', '')
    if not api_key:
        raise RuntimeError('ANTHROPIC_API_KEY is not configured in .env')
    return api_key


@dataclass
class MatchResult:
    """Result of LLM matching attempt."""
    raw_input: str
    match_key: str
    matched_song_id: Optional[int]
    matched_song_name: Optional[str]
    confidence: str  # "high", "medium", "low", "none"
    reasoning: str
    should_auto_link: bool


SYSTEM_PROMPT = """You are a Zimbabwean music expert matching messy user votes to a database of verified songs.

Your job is to match user-submitted votes (which may be misspelled, incomplete, or in wrong format) to the correct song from the verified database.

RULES:
1. Match votes to the EXACT song from the provided list when possible
2. Handle common issues:
   - Missing artist name (just song title): Match if song title is unique enough
   - No dash separator: "Winky D Ijipita" should match "Winky D - Ijipita"
   - Typos: "winkyd", "jah prayza", "holy10" etc.
   - Numbers: Some users vote by typing just a number - this might mean chart position, ignore these
3. Only return HIGH confidence if you're 95%+ sure
4. Return MEDIUM if you're 70-95% sure
5. Return LOW or NONE if unsure - don't guess!

IMPORTANT: You must match to songs in the provided list ONLY. Do not invent matches."""


def call_anthropic_api(prompt: str) -> str:
    """Call Anthropic Claude API and return the response text."""
    api_key = get_anthropic_api_key()
    
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 4096,
        "system": SYSTEM_PROMPT,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
    }
    
    response = requests.post(ANTHROPIC_API_URL, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    
    data = response.json()
    content = data.get("content", [])
    if content and len(content) > 0:
        return content[0].get("text", "")
    return ""


def get_verified_songs_list() -> List[Dict]:
    """Get list of verified songs for the prompt with enriched data."""
    songs = CleanedSong.objects.filter(status='verified').order_by('artist', 'title')[:MAX_SONGS_IN_PROMPT]
    return [
        {
            'id': song.id,
            'artist': song.artist,
            'title': song.title,
            'canonical_name': song.canonical_name,
            'spotify_id': song.spotify_track_id or '',
        }
        for song in songs
    ]


def get_pending_songs(limit: int = 100) -> List[CleanedSong]:
    """
    Get CleanedSong entries with 'pending' status.
    These need to be reviewed and matched to verified songs.
    """
    return list(CleanedSong.objects.filter(status='pending').order_by('-created_at')[:limit])


def get_unmatched_tallies(date=None, limit: int = 100) -> List[RawSongTally]:
    """
    Get RawSongTally entries that don't have a MatchKeyMapping.
    These are the "pending" votes that need manual review or LLM matching.
    """
    # Get all match_keys that already have mappings
    mapped_keys = set(MatchKeyMapping.objects.values_list('match_key', flat=True))
    
    # Get tallies without mappings
    queryset = RawSongTally.objects.exclude(match_key__in=mapped_keys)
    
    if date:
        queryset = queryset.filter(date=date)
    
    return list(queryset.order_by('-count')[:limit])


def build_pending_songs_prompt(pending_songs: List[Dict], verified_songs: List[Dict]) -> str:
    """Build prompt for matching pending songs to verified songs."""
    verified_text = "\n".join([
        f"  [{s['id']}] {s['canonical_name']}"
        for s in verified_songs
    ])
    
    pending_text = "\n".join([
        f"  {i+1}. [PENDING_ID:{s['id']}] \"{s['canonical_name']}\""
        for i, s in enumerate(pending_songs)
    ])
    
    return f"""Match these PENDING songs to VERIFIED songs in the database.

VERIFIED SONGS DATABASE (these are correct, canonical song names):
{verified_text}

PENDING SONGS TO REVIEW (may have typos, wrong format, or be duplicates):
{pending_text}

TASK: For each pending song, determine if it matches a verified song.
- If it's the same song (even with typos), return the verified song's ID
- If it's spam, gibberish, or not a real song, mark as "reject"
- If it's a valid song but NOT in verified list, mark as "new"

Respond with ONLY a JSON array:
[
  {{
    "pending_id": 123,
    "action": "match" or "reject" or "new",
    "matched_verified_id": 456 or null,
    "confidence": "high" or "medium" or "low",
    "reasoning": "brief explanation"
  }}
]

Examples:
- "Winkyd - Ijipitha" matches "Winky D - Ijipita" -> action: "match", matched_verified_id: [ID of Winky D - Ijipita]
- "Get Free Data at xyz.com" -> action: "reject" (spam)
- "New Artist - New Song" (not in verified list) -> action: "new", matched_verified_id: null"""


def build_matching_prompt(votes: List[Dict], songs: List[Dict]) -> str:
    """Build the prompt for batch matching."""
    songs_text = "\n".join([
        f"  [{s['id']}] {s['canonical_name']}"
        for s in songs
    ])
    
    votes_text = "\n".join([
        f"  {i+1}. \"{v['display_name']}\" (match_key: {v['match_key']})"
        for i, v in enumerate(votes)
    ])
    
    return f"""Match these user votes to the verified songs database.

VERIFIED SONGS DATABASE (format: [ID] Artist - Song Title):
{songs_text}

USER VOTES TO MATCH:
{votes_text}

INSTRUCTIONS:
1. For each vote, find the EXACT or closest matching song from the database above
2. Match by BOTH artist AND song title - they should both match
3. Handle typos: "winkyd" = "Winky D", "jah prayza" = "Jah Prayzah"
4. If vote says "Winky D - Kasong Kejecha", match to "[ID] Winky D - Kasong Kejecha"
5. Return the song ID number from the database in matched_song_id

Respond with ONLY a JSON array:
[
  {{
    "vote_index": 0,
    "match_key": "copy the match_key from the vote",
    "matched_song_id": 41,
    "confidence": "high",
    "reasoning": "Exact match for Winky D - Kasong Kejecha"
  }}
]

Confidence levels:
- "high": Exact or near-exact match (same artist + same song)
- "medium": Likely match but artist OR song has significant differences
- "low" or "none": Cannot find a match, set matched_song_id to null  
- "low" or "none": Don't match, leave for manual review
- If vote is just a number or gibberish, set matched_song_id to null"""


def match_votes_with_llm(
    votes: List[Dict], 
    songs: List[Dict]
) -> List[MatchResult]:
    """
    Use LLM to match votes to verified songs.
    
    Args:
        votes: List of vote dicts with display_name, match_key, count
        songs: List of verified song dicts with id, artist, title, canonical_name
        
    Returns:
        List of MatchResult objects
    """
    if not votes:
        return []
    
    prompt = build_matching_prompt(votes, songs)
    
    try:
        response_text = call_anthropic_api(prompt)
        response_text = response_text.strip()
        
        # Clean up response - remove markdown code blocks if present
        if response_text.startswith('```'):
            lines = response_text.split('\n')
            response_text = '\n'.join(lines[1:-1])
        if response_text.endswith('```'):
            response_text = response_text[:-3]
        
        # Try to find JSON array in response
        start_idx = response_text.find('[')
        end_idx = response_text.rfind(']') + 1
        if start_idx != -1 and end_idx > start_idx:
            response_text = response_text[start_idx:end_idx]
        
        results = json.loads(response_text)
        
        match_results = []
        songs_by_id = {s['id']: s for s in songs}
        
        for r in results:
            vote_idx = r.get('vote_index', 0)
            if vote_idx >= len(votes):
                continue
                
            vote = votes[vote_idx]
            matched_id = r.get('matched_song_id')
            
            # Handle case where LLM returns ID as string
            if matched_id is not None:
                try:
                    matched_id = int(matched_id)
                except (ValueError, TypeError):
                    matched_id = None
            
            matched_song = songs_by_id.get(matched_id) if matched_id else None
            confidence = r.get('confidence', 'none').lower()
            
            # Get canonical name from matched song, or try to get from LLM response
            matched_song_name = None
            if matched_song:
                matched_song_name = matched_song['canonical_name']
            elif matched_id and 'matched_song_name' in r:
                matched_song_name = r.get('matched_song_name')
            
            match_results.append(MatchResult(
                raw_input=vote['display_name'],
                match_key=vote['match_key'],
                matched_song_id=matched_id,
                matched_song_name=matched_song_name,
                confidence=confidence,
                reasoning=r.get('reasoning', ''),
                should_auto_link=(confidence == 'high' and matched_id is not None),
            ))
        
        return match_results
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response: {e}")
        logger.error(f"Response was: {response_text[:500]}")
        return []
    except Exception as e:
        logger.exception(f"LLM matching error: {e}")
        return []


@transaction.atomic
def create_match_mapping(
    match_key: str,
    cleaned_song_id: int,
    sample_display_name: str,
    vote_count: int = 0,
    is_auto_mapped: bool = True,
) -> MatchKeyMapping:
    """Create a MatchKeyMapping linking a raw match_key to a CleanedSong."""
    mapping, created = MatchKeyMapping.objects.get_or_create(
        match_key=match_key,
        defaults={
            'cleaned_song_id': cleaned_song_id,
            'sample_display_name': sample_display_name,
            'vote_count': vote_count,
            'is_auto_mapped': is_auto_mapped,
        }
    )
    
    if not created:
        # Update existing mapping
        mapping.cleaned_song_id = cleaned_song_id
        mapping.vote_count = vote_count
        mapping.is_auto_mapped = is_auto_mapped
        mapping.save()
    
    return mapping


@transaction.atomic
def update_cleaned_song_tallies(date=None):
    """
    Recalculate CleanedSongTally based on MatchKeyMappings.
    This aggregates votes from RawSongTally through the mappings.
    """
    from django.db.models import F
    
    if date is None:
        date = timezone.localdate()
    
    # Get all mappings
    mappings = MatchKeyMapping.objects.select_related('cleaned_song').all()
    mapping_dict = {m.match_key: m.cleaned_song for m in mappings}
    
    # Get raw tallies for the date
    raw_tallies = RawSongTally.objects.filter(date=date)
    
    # Aggregate by cleaned song
    song_counts = {}
    for tally in raw_tallies:
        cleaned_song = mapping_dict.get(tally.match_key)
        if cleaned_song and cleaned_song.status == 'verified':
            if cleaned_song.id not in song_counts:
                song_counts[cleaned_song.id] = 0
            song_counts[cleaned_song.id] += tally.count
    
    # Update CleanedSongTally
    for song_id, count in song_counts.items():
        CleanedSongTally.objects.update_or_create(
            date=date,
            cleaned_song_id=song_id,
            defaults={'count': count}
        )
    
    return len(song_counts)


def process_unmatched_votes(
    date=None,
    limit: int = 100,
    auto_link_high_confidence: bool = True,
    dry_run: bool = False,
) -> Dict:
    """
    Main function to process unmatched votes using LLM.
    
    Args:
        date: Filter by date (None for all dates)
        limit: Max votes to process
        auto_link_high_confidence: If True, auto-create mappings for high confidence matches
        dry_run: If True, don't save anything, just return results
        
    Returns:
        Dict with statistics and results
    """
    # Get verified songs
    songs = get_verified_songs_list()
    if not songs:
        return {
            'error': 'No verified songs in database. Add some songs first.',
            'processed': 0,
        }
    
    # Get unmatched tallies
    unmatched = get_unmatched_tallies(date=date, limit=limit)
    if not unmatched:
        return {
            'message': 'No unmatched votes found.',
            'processed': 0,
        }
    
    # Convert to dicts for the prompt
    votes_data = [
        {
            'display_name': t.display_name,
            'match_key': t.match_key,
            'count': t.count,
            'date': str(t.date),
        }
        for t in unmatched
    ]
    
    # Process in batches
    all_results = []
    stats = {
        'total_processed': 0,
        'high_confidence': 0,
        'medium_confidence': 0,
        'low_confidence': 0,
        'no_match': 0,
        'auto_linked': 0,
        'errors': 0,
    }
    
    for i in range(0, len(votes_data), BATCH_SIZE):
        batch = votes_data[i:i + BATCH_SIZE]
        logger.info(f"Processing batch {i//BATCH_SIZE + 1}, {len(batch)} votes")
        
        try:
            results = match_votes_with_llm(batch, songs)
            all_results.extend(results)
            
            for result in results:
                stats['total_processed'] += 1
                
                if result.confidence == 'high':
                    stats['high_confidence'] += 1
                elif result.confidence == 'medium':
                    stats['medium_confidence'] += 1
                elif result.confidence == 'low':
                    stats['low_confidence'] += 1
                else:
                    stats['no_match'] += 1
                
                # Auto-link high confidence matches
                if auto_link_high_confidence and result.should_auto_link and not dry_run:
                    try:
                        # Find the tally to get vote count
                        tally = next(
                            (t for t in unmatched if t.match_key == result.match_key),
                            None
                        )
                        vote_count = tally.count if tally else 0
                        
                        create_match_mapping(
                            match_key=result.match_key,
                            cleaned_song_id=result.matched_song_id,
                            sample_display_name=result.raw_input,
                            vote_count=vote_count,
                            is_auto_mapped=True,
                        )
                        stats['auto_linked'] += 1
                        logger.info(
                            f"Auto-linked: '{result.raw_input}' -> '{result.matched_song_name}'"
                        )
                    except Exception as e:
                        logger.error(f"Failed to create mapping: {e}")
                        stats['errors'] += 1
                        
        except Exception as e:
            logger.exception(f"Batch processing error: {e}")
            stats['errors'] += 1
    
    # Update tallies if we auto-linked anything
    if stats['auto_linked'] > 0 and not dry_run:
        from django.utils import timezone
        update_cleaned_song_tallies(date or timezone.localdate())
    
    return {
        'stats': stats,
        'results': [
            {
                'raw_input': r.raw_input,
                'match_key': r.match_key,
                'matched_song': r.matched_song_name,
                'confidence': r.confidence,
                'reasoning': r.reasoning,
                'auto_linked': r.should_auto_link and auto_link_high_confidence,
            }
            for r in all_results
        ],
    }


def match_single_vote(raw_input: str) -> Optional[MatchResult]:
    """
    Match a single vote input against verified songs.
    Useful for real-time matching when a vote comes in.
    
    Args:
        raw_input: The raw user input (could be any format)
        
    Returns:
        MatchResult or None if LLM is not configured
    """
    songs = get_verified_songs_list()
    if not songs:
        return None
    
    vote_data = [{
        'display_name': raw_input,
        'match_key': normalize_text(raw_input),
        'count': 1,
    }]
    
    results = match_votes_with_llm(vote_data, songs)
    return results[0] if results else None


@dataclass
class PendingSongResult:
    """Result of LLM review for a pending song."""
    pending_id: int
    pending_name: str
    action: str  # "match", "reject", "new"
    matched_verified_id: Optional[int]
    matched_verified_name: Optional[str]
    confidence: str
    reasoning: str


def match_pending_songs_with_llm(
    pending_songs: List[Dict],
    verified_songs: List[Dict]
) -> List[PendingSongResult]:
    """
    Use LLM to match pending songs to verified songs.
    """
    if not pending_songs:
        return []
    
    prompt = build_pending_songs_prompt(pending_songs, verified_songs)
    
    try:
        response_text = call_anthropic_api(prompt)
        response_text = response_text.strip()
        
        # Clean up response
        if response_text.startswith('```'):
            lines = response_text.split('\n')
            response_text = '\n'.join(lines[1:-1])
        if response_text.endswith('```'):
            response_text = response_text[:-3]
        
        # Find JSON array
        start_idx = response_text.find('[')
        end_idx = response_text.rfind(']') + 1
        if start_idx != -1 and end_idx > start_idx:
            response_text = response_text[start_idx:end_idx]
        
        results = json.loads(response_text)
        
        verified_by_id = {s['id']: s for s in verified_songs}
        pending_by_id = {s['id']: s for s in pending_songs}
        
        match_results = []
        for r in results:
            pending_id = r.get('pending_id')
            if pending_id is None:
                continue
            
            try:
                pending_id = int(pending_id)
            except (ValueError, TypeError):
                continue
            
            pending_song = pending_by_id.get(pending_id)
            if not pending_song:
                continue
            
            matched_id = r.get('matched_verified_id')
            if matched_id is not None:
                try:
                    matched_id = int(matched_id)
                except (ValueError, TypeError):
                    matched_id = None
            
            matched_song = verified_by_id.get(matched_id) if matched_id else None
            
            match_results.append(PendingSongResult(
                pending_id=pending_id,
                pending_name=pending_song['canonical_name'],
                action=r.get('action', 'new').lower(),
                matched_verified_id=matched_id,
                matched_verified_name=matched_song['canonical_name'] if matched_song else None,
                confidence=r.get('confidence', 'low').lower(),
                reasoning=r.get('reasoning', ''),
            ))
        
        return match_results
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response: {e}")
        logger.error(f"Response was: {response_text[:500]}")
        return []
    except Exception as e:
        logger.exception(f"LLM matching error: {e}")
        return []


@transaction.atomic
def merge_pending_to_verified(pending_id: int, verified_id: int) -> bool:
    """
    Merge a pending song into a verified song.
    - Transfer all MatchKeyMappings from pending to verified
    - Delete the pending song
    """
    try:
        pending = CleanedSong.objects.get(id=pending_id)
        verified = CleanedSong.objects.get(id=verified_id)
        
        # Transfer all match key mappings
        MatchKeyMapping.objects.filter(cleaned_song=pending).update(cleaned_song=verified)
        
        # Also create a mapping for the pending song's canonical name
        pending_match_key = f"{normalize_text(pending.artist)}::{normalize_text(pending.title)}"
        MatchKeyMapping.objects.get_or_create(
            match_key=pending_match_key,
            defaults={
                'cleaned_song': verified,
                'sample_display_name': pending.canonical_name,
                'vote_count': 0,
                'is_auto_mapped': True,
            }
        )
        
        # Delete the pending song
        pending.delete()
        
        logger.info(f"Merged '{pending.canonical_name}' into '{verified.canonical_name}'")
        return True
        
    except CleanedSong.DoesNotExist:
        logger.error(f"Song not found: pending={pending_id}, verified={verified_id}")
        return False
    except Exception as e:
        logger.exception(f"Merge failed: {e}")
        return False


def process_pending_songs(
    limit: int = 100,
    auto_merge: bool = True,
    auto_reject: bool = True,
    dry_run: bool = False,
) -> Dict:
    """
    Main function to process pending songs using LLM.
    
    Args:
        limit: Max pending songs to process
        auto_merge: If True, auto-merge high confidence matches
        auto_reject: If True, auto-reject spam/invalid entries
        dry_run: If True, don't save anything
        
    Returns:
        Dict with statistics and results
    """
    verified_songs = get_verified_songs_list()
    if not verified_songs:
        return {
            'error': 'No verified songs in database.',
            'processed': 0,
        }
    
    pending = get_pending_songs(limit=limit)
    if not pending:
        return {
            'message': 'No pending songs to review.',
            'processed': 0,
        }
    
    # Convert to dicts
    pending_data = [
        {
            'id': s.id,
            'artist': s.artist,
            'title': s.title,
            'canonical_name': s.canonical_name,
        }
        for s in pending
    ]
    
    stats = {
        'total_processed': 0,
        'matched': 0,
        'rejected': 0,
        'new_songs': 0,
        'auto_merged': 0,
        'auto_rejected': 0,
        'errors': 0,
    }
    
    all_results = []
    
    # Process in batches
    for i in range(0, len(pending_data), BATCH_SIZE):
        batch = pending_data[i:i + BATCH_SIZE]
        logger.info(f"Processing pending batch {i//BATCH_SIZE + 1}, {len(batch)} songs")
        
        try:
            results = match_pending_songs_with_llm(batch, verified_songs)
            all_results.extend(results)
            
            for result in results:
                stats['total_processed'] += 1
                was_applied = False
                action_for_log = result.action
                
                if result.action == 'match':
                    stats['matched'] += 1
                    
                    # Auto-merge high AND medium confidence matches
                    if auto_merge and result.confidence in ('high', 'medium') and result.matched_verified_id and not dry_run:
                        if merge_pending_to_verified(result.pending_id, result.matched_verified_id):
                            stats['auto_merged'] += 1
                            was_applied = True
                            action_for_log = 'auto_merge'
                            logger.info(f"Auto-merged ({result.confidence}): '{result.pending_name}' -> '{result.matched_verified_name}'")
                        else:
                            stats['errors'] += 1
                            
                elif result.action == 'reject':
                    stats['rejected'] += 1
                    # Mark as rejected so it doesn't get re-processed
                    # User can review in admin by filtering status='rejected'
                    if not dry_run:
                        try:
                            CleanedSong.objects.filter(id=result.pending_id).update(status='rejected')
                            was_applied = True
                            action_for_log = 'auto_reject'
                            logger.info(f"Auto-rejected: '{result.pending_name}' - {result.reasoning}")
                        except Exception as e:
                            logger.error(f"Failed to reject: {e}")
                            stats['errors'] += 1
                            
                else:  # "new"
                    stats['new_songs'] += 1
                    # Mark as verified - it's a legitimate new song
                    if not dry_run:
                        try:
                            CleanedSong.objects.filter(id=result.pending_id).update(status='verified')
                            was_applied = True
                            logger.info(f"Verified as new song: '{result.pending_name}'")
                        except Exception as e:
                            logger.error(f"Failed to verify new song: {e}")
                
                # Log the decision
                if not dry_run:
                    try:
                        LLMDecisionLog.objects.create(
                            input_text=result.pending_name,
                            input_type='pending_song',
                            action=action_for_log,
                            confidence=result.confidence,
                            reasoning=result.reasoning or '',
                            matched_song_id=result.matched_verified_id,
                            matched_song_name=result.matched_verified_name or '',
                            was_applied=was_applied,
                        )
                    except Exception as e:
                        logger.error(f"Failed to log LLM decision: {e}")
                    
        except Exception as e:
            logger.exception(f"Batch processing error: {e}")
            stats['errors'] += 1
    
    return {
        'stats': stats,
        'results': [
            {
                'pending_id': r.pending_id,
                'pending_name': r.pending_name,
                'action': r.action,
                'matched_to': r.matched_verified_name,
                'confidence': r.confidence,
                'reasoning': r.reasoning,
            }
            for r in all_results
        ],
    }
