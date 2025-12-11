"""
Fuzzy matching and verified artist integration for vote normalization.

This module provides intelligent matching to group similar votes together,
even with typos or spelling variations.
"""
import re
from functools import lru_cache
from difflib import SequenceMatcher
from typing import Optional
from django.db.models import Q

from .models import VerifiedArtist, RawSongTally, normalize_text
from .text_cleaning import clean_vote_text, correct_artist_typo


# Similarity threshold for fuzzy matching (0.0 to 1.0)
# 0.85 = 85% similar (allows minor typos)
ARTIST_SIMILARITY_THRESHOLD = 0.85
SONG_SIMILARITY_THRESHOLD = 0.80


def similarity_ratio(a: str, b: str) -> float:
    """
    Calculate similarity between two strings using SequenceMatcher.
    Returns a value between 0.0 (completely different) and 1.0 (identical).
    """
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate the Levenshtein (edit) distance between two strings.
    This is the minimum number of single-character edits needed to change one into the other.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    
    if len(s2) == 0:
        return len(s1)
    
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # j+1 instead of j since previous_row and current_row are one character longer
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def is_similar(text1: str, text2: str, threshold: float = 0.85) -> bool:
    """Check if two strings are similar enough to be considered the same."""
    # Exact match after normalization
    if normalize_text(text1) == normalize_text(text2):
        return True
    
    # Check similarity ratio
    ratio = similarity_ratio(normalize_text(text1), normalize_text(text2))
    return ratio >= threshold


@lru_cache(maxsize=500)
def get_verified_artists_cache() -> dict:
    """
    Get a cached dictionary of all verified artists and their aliases.
    Returns: {normalized_name: VerifiedArtist.name (canonical)}
    """
    cache = {}
    for artist in VerifiedArtist.objects.filter(is_active=True):
        # Add main name
        cache[artist.name_normalized] = artist.name
        # Add aliases
        for alias in artist.get_all_names():
            cache[alias] = artist.name
    return cache


def clear_artist_cache():
    """Clear the verified artists cache (call after adding new artists)."""
    get_verified_artists_cache.cache_clear()


def match_verified_artist(artist_input: str) -> Optional[str]:
    """
    Try to match input against verified artists.
    
    Returns the canonical artist name if found, None otherwise.
    
    Matching strategy:
    1. Exact match (normalized)
    2. Fuzzy match against verified artists and aliases
    """
    artist_norm = normalize_text(artist_input)
    verified_artists = get_verified_artists_cache()
    
    # 1. Exact match
    if artist_norm in verified_artists:
        return verified_artists[artist_norm]
    
    # 2. Fuzzy match against all verified names
    best_match = None
    best_score = 0.0
    
    for verified_norm, canonical_name in verified_artists.items():
        score = similarity_ratio(artist_norm, verified_norm)
        if score > best_score and score >= ARTIST_SIMILARITY_THRESHOLD:
            best_score = score
            best_match = canonical_name
    
    return best_match


def find_existing_song_match(artist_normalized: str, song_normalized: str, date) -> Optional[RawSongTally]:
    """
    Find an existing song tally that fuzzy-matches the input.
    
    This helps group votes like:
    - "Winky D - Ijipita" and "Winky D - Ijipitha" (typo)
    - "Holy Ten - Pressure" and "Holyten - Pressure" (spacing)
    
    Returns the matching RawSongTally if found, None otherwise.
    """
    # Get today's tallies
    existing_tallies = RawSongTally.objects.filter(date=date)
    
    for tally in existing_tallies:
        # Parse the existing match_key
        parts = tally.match_key.split('::', 1)
        if len(parts) != 2:
            continue
        
        existing_artist, existing_song = parts
        
        # Check if artist is similar
        artist_similar = is_similar(artist_normalized, existing_artist, ARTIST_SIMILARITY_THRESHOLD)
        if not artist_similar:
            continue
        
        # Check if song is similar
        song_similar = is_similar(song_normalized, existing_song, SONG_SIMILARITY_THRESHOLD)
        if song_similar:
            return tally
    
    return None


def normalize_vote_input(artist_raw: str, song_raw: str) -> tuple[str, str, str, str]:
    """
    Normalize vote input with verified artist matching and fuzzy matching.
    
    Returns: (artist_display, song_display, match_key, display_name)
    
    Process:
    1. Apply text cleaning (normalize words, extract features, clean title)
    2. Correct common typos
    3. Try to match artist against verified artists
    4. Use canonical name if matched, otherwise clean the input
    5. Create match_key for grouping
    """
    # Step 1: Apply text cleaning (handles "ft", "&", "(Official Video)", etc.)
    artist_cleaned, song_cleaned = clean_vote_text(artist_raw, song_raw)
    
    # Step 2: Correct common typos
    artist_cleaned = correct_artist_typo(artist_cleaned)
    
    # Step 3: Try to match against verified artists
    verified_artist = match_verified_artist(artist_cleaned)
    
    if verified_artist:
        # Use the canonical artist name
        artist_display = verified_artist
    else:
        # Clean up the input (title case, remove extra spaces)
        artist_display = re.sub(r'\s+', ' ', artist_cleaned.strip())
        # Title case for consistency
        artist_display = artist_display.title()
    
    # Clean up song name
    song_display = re.sub(r'\s+', ' ', song_cleaned.strip())
    # Title case for consistency
    song_display = song_display.title()
    
    # Create normalized match key
    artist_normalized = normalize_text(artist_display)
    song_normalized = normalize_text(song_display)
    match_key = f"{artist_normalized}::{song_normalized}"
    
    # Create display name
    display_name = f"{artist_display} - {song_display}"
    
    return artist_display, song_display, match_key, display_name


def smart_normalize_vote(artist_raw: str, song_raw: str, vote_date) -> tuple[str, str, str, str]:
    """
    Smart vote normalization that combines:
    1. Verified artist matching
    2. Fuzzy matching against existing votes
    
    Returns: (artist_display, song_display, match_key, display_name)
    """
    # First, normalize with verified artist matching
    artist_display, song_display, match_key, display_name = normalize_vote_input(artist_raw, song_raw)
    
    # Check for fuzzy match against existing tallies
    artist_norm = normalize_text(artist_display)
    song_norm = normalize_text(song_display)
    
    existing_match = find_existing_song_match(artist_norm, song_norm, vote_date)
    
    if existing_match:
        # Use the existing match_key and display_name for consistency
        return (
            artist_display,  # Keep user's version for raw vote record
            song_display,
            existing_match.match_key,  # Use existing match_key for grouping
            existing_match.display_name  # Use existing display_name
        )
    
    return artist_display, song_display, match_key, display_name


def find_song_by_title_only(song_raw: str) -> Optional[tuple[str, str, str, str]]:
    """
    Find an existing song by title only (for song-only votes).
    
    If the song title matches an existing song, return its artist info.
    Uses fuzzy matching to handle typos.
    
    Args:
        song_raw: The raw song title from user input
        
    Returns:
        Tuple of (artist_display, song_display, match_key, display_name) or None
    """
    from .models import CleanedSong, RawSongTally
    from django.utils import timezone
    
    song_normalized = normalize_text(song_raw)
    
    # First, check CleanedSong (verified/cleaned songs)
    cleaned_songs = CleanedSong.objects.all()
    best_match = None
    best_score = 0.0
    
    for song in cleaned_songs:
        song_title_norm = normalize_text(song.title)
        score = similarity_ratio(song_normalized, song_title_norm)
        
        if score > best_score and score >= SONG_SIMILARITY_THRESHOLD:
            best_score = score
            best_match = song
    
    if best_match:
        artist = best_match.artist
        title = best_match.title
        match_key = f"{normalize_text(artist)}::{normalize_text(title)}"
        display_name = best_match.canonical_name
        return (artist, title, match_key, display_name)
    
    # Second, check RawSongTally (recent votes)
    today = timezone.localdate()
    tallies = RawSongTally.objects.filter(date=today)
    
    for tally in tallies:
        parts = tally.match_key.split('::', 1)
        if len(parts) != 2:
            continue
        
        existing_artist, existing_song = parts
        
        # Skip "unknown" artist entries
        if existing_artist == 'unknown':
            continue
        
        score = similarity_ratio(song_normalized, existing_song)
        if score > best_score and score >= SONG_SIMILARITY_THRESHOLD:
            best_score = score
            # Extract artist from display_name
            display_parts = tally.display_name.split(' - ', 1)
            if len(display_parts) == 2:
                return (display_parts[0], display_parts[1], tally.match_key, tally.display_name)
    
    return None


# ============================================================
# Utility functions for admin/management
# ============================================================

def find_similar_songs(threshold: float = 0.80) -> list[tuple]:
    """
    Find potentially duplicate songs across all tallies.
    Useful for manual review and merging.
    
    Returns list of (tally1, tally2, similarity_score) tuples.
    """
    from django.utils import timezone
    
    today = timezone.localdate()
    tallies = list(RawSongTally.objects.filter(date=today).order_by('-count'))
    
    duplicates = []
    
    for i, t1 in enumerate(tallies):
        for t2 in tallies[i+1:]:
            score = similarity_ratio(t1.match_key, t2.match_key)
            if score >= threshold and score < 1.0:  # Similar but not identical
                duplicates.append((t1, t2, score))
    
    return sorted(duplicates, key=lambda x: -x[2])  # Sort by similarity descending


def merge_song_tallies(source_tally: RawSongTally, target_tally: RawSongTally):
    """
    Merge votes from source_tally into target_tally.
    Use this to combine duplicate entries.
    
    If a user voted for both songs on the same day, the duplicate vote is deleted
    (keeps the vote for the target song).
    
    WARNING: This updates the database. Use with caution.
    """
    from .models import RawVote
    from django.db import transaction
    
    with transaction.atomic():
        # Get votes for source match_key
        source_votes = RawVote.objects.filter(
            match_key=source_tally.match_key,
            vote_date=source_tally.date
        )
        
        # Check for users who already voted for target
        target_user_ids = set(
            RawVote.objects.filter(
                match_key=target_tally.match_key,
                vote_date=target_tally.date
            ).values_list('user_id', flat=True)
        )
        
        # Delete duplicate votes (user already voted for target)
        duplicates = source_votes.filter(user_id__in=target_user_ids)
        duplicate_count = duplicates.count()
        duplicates.delete()
        
        # Update remaining votes to target match_key
        remaining_votes = source_votes.exclude(user_id__in=target_user_ids)
        votes_moved = remaining_votes.count()
        remaining_votes.update(
            match_key=target_tally.match_key,
            display_name=target_tally.display_name
        )
        
        # Update target count (only add non-duplicate votes)
        target_tally.count += votes_moved
        target_tally.save()
        
        # Delete source tally
        source_tally.delete()
        
        return {
            'votes_moved': votes_moved,
            'duplicates_deleted': duplicate_count,
            'new_total': target_tally.count
        }
        source_tally.delete()
