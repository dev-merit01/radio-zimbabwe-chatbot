"""
Text cleaning utilities for vote processing.

Features:
1. Common Typo Dictionary - Pre-mapped common misspellings
2. Auto-Correct Common Words - "ft"→"feat.", "&"→"and"
3. Feature Artist Extraction - Parse "Winky D ft Holy Ten - Song"
4. Clean song titles - Remove "(Official Video)" etc.
"""
import re
from typing import Optional, Tuple, List


# ============================================================
# 1. Common Typo Dictionary for Zimbabwean Artists
# ============================================================

ARTIST_TYPO_CORRECTIONS = {
    # Winky D variations
    'winkyd': 'Winky D',
    'winky': 'Winky D',
    'winkyd': 'Winky D',
    'winki d': 'Winky D',
    'winkie d': 'Winky D',
    'winked': 'Winky D',
    
    # Jah Prayzah variations
    'jah prayza': 'Jah Prayzah',
    'jahprayzah': 'Jah Prayzah',
    'jah praiza': 'Jah Prayzah',
    'jah prayzer': 'Jah Prayzah',
    'jah prazah': 'Jah Prayzah',
    'ja prayzah': 'Jah Prayzah',
    'jahprayza': 'Jah Prayzah',
    
    # Holy Ten variations
    'holyten': 'Holy Ten',
    'holy 10': 'Holy Ten',
    'holy10': 'Holy Ten',
    'hollyten': 'Holy Ten',
    'holly ten': 'Holy Ten',
    
    # Enzo Ishall variations
    'enzoishall': 'Enzo Ishall',
    'enzo ishal': 'Enzo Ishall',
    'enzo ishaal': 'Enzo Ishall',
    'enzoishal': 'Enzo Ishall',
    
    # Freeman variations
    'freemn': 'Freeman',
    'freman': 'Freeman',
    'free man': 'Freeman',
    
    # Killer T variations
    'killert': 'Killer T',
    'killer': 'Killer T',
    'killa t': 'Killer T',
    'killat': 'Killer T',
    
    # Alick Macheso variations
    'macheso': 'Alick Macheso',
    'alik macheso': 'Alick Macheso',
    'aleck macheso': 'Alick Macheso',
    
    # Jah Signal variations
    'jahsignal': 'Jah Signal',
    'jah singal': 'Jah Signal',
    'ja signal': 'Jah Signal',
    
    # Tocky Vibes variations
    'tockyvibes': 'Tocky Vibes',
    'toky vibes': 'Tocky Vibes',
    'tocky vibe': 'Tocky Vibes',
    
    # ExQ variations
    'ex q': 'ExQ',
    'ex-q': 'ExQ',
    'exque': 'ExQ',
    
    # Nutty O variations
    'nuttyo': 'Nutty O',
    'nutty': 'Nutty O',
    'nuty o': 'Nutty O',
    
    # Ti Gonzi variations
    'tigonzi': 'Ti Gonzi',
    'ti gonzy': 'Ti Gonzi',
    'tigonzy': 'Ti Gonzi',
    
    # Ammara Brown variations
    'amara brown': 'Ammara Brown',
    'ammara': 'Ammara Brown',
    'ammarabrown': 'Ammara Brown',
    
    # Oliver Mtukudzi variations
    'tuku': 'Oliver Mtukudzi',
    'mtukudzi': 'Oliver Mtukudzi',
    'oliver mtukudzi': 'Oliver Mtukudzi',
    
    # Suluman Chimbetu variations
    'sulu': 'Suluman Chimbetu',
    'sulumani': 'Suluman Chimbetu',
    'chimbetu': 'Suluman Chimbetu',
    
    # Takura variations
    'takura teemba': 'Takura',
    'takurateemba': 'Takura',
    
    # Voltz JT variations
    'voltzjt': 'Voltz JT',
    'voltz': 'Voltz JT',
    'voltsjt': 'Voltz JT',
}


# ============================================================
# 2. Auto-Correct Common Words/Patterns
# ============================================================

def normalize_common_words(text: str) -> str:
    """
    Normalize common abbreviations and symbols for consistency.
    """
    # Feature artist indicators - normalize to "feat."
    # Order matters: handle longer patterns first
    text = re.sub(r'\bfeaturing\b', 'feat.', text, flags=re.IGNORECASE)
    text = re.sub(r'\bfeat\.?\b', 'feat.', text, flags=re.IGNORECASE)
    text = re.sub(r'\bft\.?\b', 'feat.', text, flags=re.IGNORECASE)
    
    # Clean up any double periods that might result
    text = re.sub(r'feat\.\.+', 'feat.', text)
    
    # Ampersand to "and" (but preserve in artist names like "ExQ & Holy Ten")
    # Only replace standalone & with spaces around it
    text = re.sub(r'\s+&\s+', ' and ', text)
    
    # "x" as collaboration indicator to "feat."
    text = re.sub(r'\s+x\s+', ' feat. ', text, flags=re.IGNORECASE)
    
    # Normalize "prod." and "produced by"
    text = re.sub(r'\bprod\.?\s*by\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\bproduced\s*by\b', '', text, flags=re.IGNORECASE)
    
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


def clean_song_title(title: str) -> str:
    """
    Clean song title by removing common suffixes like:
    - (Official Video)
    - (Official Audio)
    - (Lyrics Video)
    - [Official Music Video]
    - etc.
    """
    # Patterns to remove (in parentheses or brackets)
    patterns_to_remove = [
        r'\(official\s*(music\s*)?(video|audio|lyric[s]?)\)',
        r'\[official\s*(music\s*)?(video|audio|lyric[s]?)\]',
        r'\(lyric[s]?\s*video\)',
        r'\[lyric[s]?\s*video\]',
        r'\(audio\)',
        r'\[audio\]',
        r'\(video\)',
        r'\[video\]',
        r'\(visualizer\)',
        r'\[visualizer\]',
        r'\(official\)',
        r'\[official\]',
        r'\(hd\)',
        r'\[hd\]',
        r'\(4k\)',
        r'\[4k\]',
        r'\(live\)',
        r'\[live\]',
        r'\(acoustic\)',
        r'\[acoustic\]',
        r'\(remix\)',  # Keep this info but clean the format
        r'\(radio\s*edit\)',
        r'\(extended\s*(mix|version)?\)',
    ]
    
    cleaned = title
    for pattern in patterns_to_remove:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # Clean up extra whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    # Remove trailing/leading punctuation that might be left over
    cleaned = cleaned.strip('- ')
    
    return cleaned


# ============================================================
# 3. Feature Artist Extraction
# ============================================================

def extract_featured_artists(text: str) -> Tuple[str, List[str]]:
    """
    Extract main artist and featured artists from text.
    
    Examples:
        "Winky D feat. Holy Ten" -> ("Winky D", ["Holy Ten"])
        "Winky D and Holy Ten" -> ("Winky D", ["Holy Ten"])
        "Winky D, Holy Ten, Freeman" -> ("Winky D", ["Holy Ten", "Freeman"])
    
    Returns:
        Tuple of (main_artist, list_of_featured_artists)
    """
    featured = []
    main_artist = text
    
    # Check for "feat." or "featuring"
    feat_match = re.search(r'\s+feat\.?\s+(.+)$', text, flags=re.IGNORECASE)
    if feat_match:
        main_artist = text[:feat_match.start()].strip()
        featured_part = feat_match.group(1).strip()
        # Split by comma or "and"
        featured = [a.strip() for a in re.split(r',\s*|\s+and\s+', featured_part) if a.strip()]
        return main_artist, featured
    
    # Check for "and" as collaboration (only if no dash present - to not confuse with song title)
    if ' and ' in text.lower() and '-' not in text:
        parts = re.split(r'\s+and\s+', text, flags=re.IGNORECASE)
        if len(parts) >= 2:
            main_artist = parts[0].strip()
            featured = [p.strip() for p in parts[1:] if p.strip()]
            return main_artist, featured
    
    return main_artist, featured


def parse_artist_with_features(artist_raw: str) -> Tuple[str, str]:
    """
    Parse artist string and return main artist and formatted featured string.
    
    Examples:
        "Winky D ft Holy Ten" -> ("Winky D", "feat. Holy Ten")
        "Winky D" -> ("Winky D", "")
    
    Returns:
        Tuple of (main_artist, featured_string)
    """
    # First normalize common words
    artist_normalized = normalize_common_words(artist_raw)
    
    # Extract featured artists
    main_artist, featured = extract_featured_artists(artist_normalized)
    
    if featured:
        featured_str = "feat. " + ", ".join(featured)
        return main_artist, featured_str
    
    return main_artist, ""


# ============================================================
# 4. Typo Correction
# ============================================================

def correct_artist_typo(artist: str) -> str:
    """
    Check if artist name matches a known typo and return correction.
    
    Returns corrected artist name or original if no match.
    """
    # Normalize for lookup
    artist_lower = artist.lower().strip()
    
    # Direct match
    if artist_lower in ARTIST_TYPO_CORRECTIONS:
        return ARTIST_TYPO_CORRECTIONS[artist_lower]
    
    # Try without spaces
    artist_no_spaces = artist_lower.replace(' ', '')
    if artist_no_spaces in ARTIST_TYPO_CORRECTIONS:
        return ARTIST_TYPO_CORRECTIONS[artist_no_spaces]
    
    return artist


# ============================================================
# Main Cleaning Function
# ============================================================

def clean_vote_text(artist_raw: str, song_raw: str) -> Tuple[str, str]:
    """
    Apply all text cleaning to artist and song.
    
    Returns:
        Tuple of (cleaned_artist, cleaned_song)
    """
    # 1. Normalize common words in both
    artist = normalize_common_words(artist_raw)
    song = normalize_common_words(song_raw)
    
    # 2. Clean song title (remove Official Video, etc.)
    song = clean_song_title(song)
    
    # 3. Extract featured artists and format properly
    main_artist, featured_str = parse_artist_with_features(artist)
    
    # 4. Correct typos in main artist
    main_artist = correct_artist_typo(main_artist)
    
    # 5. Correct typos in featured artists too
    if featured_str:
        featured_names = featured_str.replace('feat. ', '').split(', ')
        corrected_featured = [correct_artist_typo(name.strip()) for name in featured_names]
        featured_str = 'feat. ' + ', '.join(corrected_featured)
    
    # 6. Reconstruct artist string
    if featured_str:
        artist = f"{main_artist} {featured_str}"
    else:
        artist = main_artist
    
    # 7. Check if featured artists are in the song title (common mistake)
    # e.g., "Song feat. Holy Ten" should move featured to artist
    song_main, song_featured = parse_artist_with_features(song)
    if song_featured:
        # Move featured artists from song to artist
        existing_featured = featured_str.replace('feat. ', '') if featured_str else ''
        all_featured = [a.strip() for a in existing_featured.split(',') if a.strip()]
        # Correct typos in song-featured artists too
        song_featured_names = song_featured.replace('feat. ', '').split(', ')
        all_featured.extend([correct_artist_typo(a.strip()) for a in song_featured_names if a.strip()])
        if all_featured:
            artist = f"{main_artist} feat. {', '.join(all_featured)}"
        song = song_main
    
    return artist.strip(), song.strip()
