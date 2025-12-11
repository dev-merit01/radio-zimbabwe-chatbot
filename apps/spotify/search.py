import logging
from difflib import SequenceMatcher
from typing import Optional, List, Tuple, Set

import spotipy
from django.conf import settings
from spotipy import SpotifyException
from spotipy.oauth2 import SpotifyClientCredentials

logger = logging.getLogger(__name__)

_client = None

# Confidence thresholds
HIGH_CONFIDENCE = 0.80  # Auto-accept (lowered from 0.85 for better local music matching)
LOW_CONFIDENCE = 0.40   # Reject below this (lowered from 0.50 to catch more Zim tracks)

# Zimbabwe market code for Spotify
SPOTIFY_MARKET = 'ZW'

# Boost for Zimbabwean artists (added to confidence score)
ZIM_ARTIST_BOOST = 0.15

# Fallback list of known Zimbabwean artists (lowercase for matching)
# This is used if the database is not available
ZIMBABWEAN_ARTISTS_FALLBACK: Set[str] = {
    # Zimdancehall / Urban Grooves
    'winky d', 'vigilance', 'tocky vibes', 'killer t', 'freeman', 'takura',
    'exq', 'nutty o', 'holy ten', 'ti gonzi', 'voltz jt', 'enzo ishall',
    'jah signal', 'soul jah love', 'seh calaz', 'dobba don', 'pumacol',
    'silent killer', 'hwindi president', 'djembe monk', 'poptain',
    'shinsoman', 'ricky fire', 'dhadza d', 'blot', 'guspy warrior',
    'caption', 'boom beto', 'bagga don', 'larry lovah',
    
    # Sungura / Jiti
    'alick macheso', 'macheso', 'suluman chimbetu', 'simon chimbetu',
    'dendera kings', 'leonard dembo', 'system tazvida', 'somandla ndebele',
    'tongai moyo', 'nicholas zakaria', 'leonard zhakata', 'mark ngwazi',
    'tryson chimbetu', 'peter moyo', 'betserai',
    
    # Chimurenga / Afro-Jazz / Legends
    'oliver mtukudzi', 'tuku', 'thomas mapfumo', 'mapfumo',
    'stella chiweshe', 'chiwoniso maraire', 'hope masike', 'mokoomba',
    'mbira dzebongo', 'zimpraise', 'mechanic manyeruke', 'sabastian magacha',
    
    # Contemporary / Afropop / R&B
    'jah prayzah', 'ammara brown', 'gemma griffiths', 'shashl',
    'sha sha', 'cindy munyavi', 'hillzy', 'novuyo seagirl', 'feli nandi',
    'janet manyowa', 'tammy moyo', 'kae chaps', 'zirree',
    'simba tagz', 'asaph', 'tehn diamond', 'roki', 'buffalo souljah',
    'jaydee taurus', 'noluntu j', 'ishan', 'xtra large', 'crooger',
    
    # Gospel
    'minister michael mahendere', 'mathias mhere', 'blessing shumba',
    'olinda zimuto', 'charles charamba', 'fungisai zvakavapano',
    'prudence katomeni', 'bethany pasinawako', 'mkhululi bhebhe',
    
    # Saintfloew and newer artists
    'saintfloew', 'uncle epatan', 'levixone', 'tamy moyo', 'nobuntu',
    'Gze', 'dj tamuka', 'mc chita', 'platinum prince', 'stunner',
    
    # Bands
    'mokoomba', 'the ramones zw', 'tsunami',
}

# Cache for verified artists from database
_verified_artists_cache: Optional[Set[str]] = None
_cache_timestamp: Optional[float] = None
CACHE_TTL = 300  # 5 minutes


def _get_verified_artists() -> Set[str]:
    """Get verified artists from database with caching."""
    global _verified_artists_cache, _cache_timestamp
    import time
    
    current_time = time.time()
    
    # Return cache if still valid
    if _verified_artists_cache is not None and _cache_timestamp is not None:
        if current_time - _cache_timestamp < CACHE_TTL:
            return _verified_artists_cache
    
    try:
        from apps.voting.models import VerifiedArtist
        
        artists = set()
        for artist in VerifiedArtist.objects.filter(is_active=True):
            artists.update(artist.get_all_names())
        
        # Merge with fallback list
        artists.update(ZIMBABWEAN_ARTISTS_FALLBACK)
        
        _verified_artists_cache = artists
        _cache_timestamp = current_time
        return artists
    except Exception:
        # Database not ready, use fallback
        return ZIMBABWEAN_ARTISTS_FALLBACK


class SpotifyNotConfiguredError(RuntimeError):
    """Raised when Spotify credentials are missing."""


class SpotifyLookupError(RuntimeError):
    """Raised when an unexpected Spotify error occurs."""


def _get_client():
    global _client
    if _client is None:
        client_id = getattr(settings, 'SPOTIFY_CLIENT_ID', '')
        client_secret = getattr(settings, 'SPOTIFY_CLIENT_SECRET', '')
        if not client_id or not client_secret:
            raise SpotifyNotConfiguredError('Spotify credentials are not configured.')
        auth = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        _client = spotipy.Spotify(auth_manager=auth)
    return _client


def _normalize(text: str) -> str:
    """Normalize text for comparison: lowercase, strip extra spaces."""
    return ' '.join(text.lower().split())


def _remove_spaces(text: str) -> str:
    """Remove all spaces from text for comparison."""
    return text.replace(' ', '').lower()


def _similarity(a: str, b: str) -> float:
    """
    Calculate similarity ratio between two strings (0.0 to 1.0).
    Also tries comparing with spaces removed to handle cases like
    'wafa wanaka' vs 'wafawanaka'.
    """
    norm_a = _normalize(a)
    norm_b = _normalize(b)
    
    # Standard comparison
    standard_sim = SequenceMatcher(None, norm_a, norm_b).ratio()
    
    # Also try with spaces removed (for cases like 'wafa wanaka' vs 'wafawanaka')
    no_space_sim = SequenceMatcher(None, _remove_spaces(a), _remove_spaces(b)).ratio()
    
    # Return the better match
    return max(standard_sim, no_space_sim)


def _is_zimbabwean_artist(artists: List[str]) -> bool:
    """Check if any of the artists are known Zimbabwean artists."""
    verified_artists = _get_verified_artists()
    
    for artist in artists:
        artist_lower = artist.lower().strip()
        # Check exact match
        if artist_lower in verified_artists:
            return True
        # Check if known artist name is contained in the result
        for zim_artist in verified_artists:
            if zim_artist in artist_lower or artist_lower in zim_artist:
                return True
    return False


def _combined_similarity(query_artist: str, query_title: str, result: dict) -> float:
    """
    Calculate combined similarity score for a Spotify result.
    Title match is weighted more heavily (60%) than artist (40%).
    Also penalizes results where title match is very poor.
    """
    result_artists = ', '.join(result['artists'])
    artist_sim = _similarity(query_artist, result_artists)
    title_sim = _similarity(query_title, result['title'])
    
    # If title similarity is very low, penalize heavily
    # This prevents "right artist, wrong song" matches
    if title_sim < 0.3:
        # Cap the score based on how bad the title match is
        return title_sim * 0.5  # Very low score for wrong titles
    
    # Weight title more heavily than artist (60/40)
    return (artist_sim * 0.4) + (title_sim * 0.6)


def _parse_track(item: dict) -> dict:
    """Parse Spotify track item into our format."""
    images = item.get('album', {}).get('images', []) or [{}]
    return {
        'id': item['id'],
        'title': item['name'],
        'artists': [a['name'] for a in item['artists']],
        'album': item.get('album', {}).get('name', ''),
        'image_url': images[0].get('url', ''),
        'preview_url': item.get('preview_url') or '',
        'popularity': item.get('popularity', 0),
    }


def _search_spotify(query: str, limit: int = 5, market: str = SPOTIFY_MARKET) -> List[dict]:
    """Execute Spotify search and return parsed results."""
    try:
        client = _get_client()
    except SpotifyNotConfiguredError:
        raise

    try:
        results = client.search(q=query, type='track', limit=limit, market=market)
    except SpotifyException as exc:
        logger.exception('Spotify search failed: %s', exc)
        raise SpotifyLookupError('Spotify search failed') from exc

    items = results.get('tracks', {}).get('items', [])
    return [_parse_track(item) for item in items]


def resolve_with_confidence(artist: str, title: str) -> Tuple[Optional[dict], float]:
    """
    Search Spotify with multiple strategies and fuzzy matching.
    Prioritizes Zimbabwean artists with a confidence boost.
    
    Returns:
        Tuple of (best_match, confidence_score)
        - best_match: dict with track info or None if no results
        - confidence_score: float 0.0-1.0 indicating match quality
    """
    # Use original input for search
    search_artist = artist
    search_title = title
    
    all_results = []
    
    # Strategy 1: Simple combined search
    try:
        results = _search_spotify(f'{search_artist} {search_title}', limit=10)
        all_results.extend(results)
    except (SpotifyNotConfiguredError, SpotifyLookupError):
        raise
    
    # Strategy 2: Exact field query
    try:
        results = _search_spotify(f'track:{search_title} artist:{search_artist}', limit=5)
        all_results.extend(results)
    except SpotifyLookupError:
        pass
    
    # Strategy 3: Title only
    try:
        results = _search_spotify(search_title, limit=5)
        all_results.extend(results)
    except SpotifyLookupError:
        pass
    
    # Strategy 4: Artist only
    try:
        results = _search_spotify(search_artist, limit=5)
        all_results.extend(results)
    except SpotifyLookupError:
        pass
    
    # Strategy 5: Search without market restriction (wider net)
    try:
        results = _search_spotify(f'{search_artist} {search_title}', limit=5, market=None)
        all_results.extend(results)
    except SpotifyLookupError:
        pass
    
    # Strategy 6: Try with spaces removed from title (e.g., 'wafa wanaka' -> 'wafawanaka')
    title_no_spaces = search_title.replace(' ', '')
    if title_no_spaces != search_title.replace(' ', ''):  # Only if there were spaces
        try:
            results = _search_spotify(f'{search_artist} {title_no_spaces}', limit=5)
            all_results.extend(results)
        except SpotifyLookupError:
            pass
    
    # Strategy 7: Try title without spaces only
    if ' ' in search_title:
        try:
            results = _search_spotify(title_no_spaces, limit=5)
            all_results.extend(results)
        except SpotifyLookupError:
            pass
    
    if not all_results:
        return None, 0.0
    
    # Deduplicate by track ID
    seen_ids = set()
    unique_results = []
    for r in all_results:
        if r['id'] not in seen_ids:
            seen_ids.add(r['id'])
            unique_results.append(r)
    
    # Score each result
    scored = []
    for result in unique_results:
        confidence = _combined_similarity(search_artist, search_title, result)
        
        # Boost score slightly for popular tracks (tie-breaker)
        popularity_boost = result.get('popularity', 0) / 1000  # max +0.1
        
        # Boost Zimbabwean artists
        zim_boost = 0.0
        if _is_zimbabwean_artist(result['artists']):
            zim_boost = ZIM_ARTIST_BOOST
            logger.debug(
                'Zimbabwean artist boost applied for: %s',
                ', '.join(result['artists'])
            )
        
        total_score = confidence + popularity_boost + zim_boost
        scored.append((result, total_score, zim_boost > 0))
    
    # Sort by score descending
    scored.sort(key=lambda x: x[1], reverse=True)
    
    best_match, best_score, is_zim = scored[0]
    # Cap score at 1.0
    best_score = min(best_score, 1.0)
    
    zim_label = " [ZIM]" if is_zim else ""
    logger.info(
        'Best match for "%s - %s": "%s" by %s%s (confidence: %.2f)',
        artist, title, best_match['title'], ', '.join(best_match['artists']),
        zim_label, best_score
    )
    
    return best_match, best_score


def resolve_top_match(artist: str, title: str) -> Optional[dict]:
    """
    Legacy function for backward compatibility.
    Returns top match only if confidence is above LOW_CONFIDENCE.
    """
    match, confidence = resolve_with_confidence(artist, title)
    if match and confidence >= LOW_CONFIDENCE:
        return match
    return None


def is_high_confidence(score: float) -> bool:
    """Check if score is high enough for auto-accept."""
    return score >= HIGH_CONFIDENCE


def is_low_confidence(score: float) -> bool:
    """Check if score is too low to even suggest."""
    return score < LOW_CONFIDENCE
