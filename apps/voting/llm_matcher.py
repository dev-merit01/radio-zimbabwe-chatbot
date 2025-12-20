"""
LLM-powered vote matching using Cohere.

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
    normalize_text,
)

logger = logging.getLogger(__name__)

# Maximum songs to include in prompt (to stay within token limits)
MAX_SONGS_IN_PROMPT = 500  # Increased to handle larger song databases
# Batch size for processing
BATCH_SIZE = 15  # Smaller batches for Cohere token limits

# Cohere API settings
COHERE_API_URL = "https://api.cohere.com/v2/chat"
COHERE_MODEL = "command-r-08-2024"  # Updated model name


def get_cohere_api_key() -> str:
    """Get Cohere API key from settings."""
    api_key = getattr(settings, 'COHERE_API_KEY', '')
    if not api_key:
        raise RuntimeError('COHERE_API_KEY is not configured in .env')
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
4. Return MEDIUM if you're 70-95% sure (human should review)
5. Return LOW or NONE if unsure - don't guess!

IMPORTANT: You must match to songs in the provided list ONLY. Do not invent matches."""


def call_cohere_api(prompt: str) -> str:
    """Call Cohere Chat API and return the response text."""
    api_key = get_cohere_api_key()
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    
    # Cohere v2 Chat API format
    payload = {
        "model": COHERE_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 4000,
    }
    
    response = requests.post(COHERE_API_URL, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    
    data = response.json()
    # v2 API returns message.content[0].text
    message = data.get("message", {})
    content = message.get("content", [])
    if content and len(content) > 0:
        return content[0].get("text", "")
    return ""


def get_verified_songs_list() -> List[Dict]:
    """Get list of verified songs for the prompt."""
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
        response_text = call_cohere_api(prompt)
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
