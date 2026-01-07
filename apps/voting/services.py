import re
import logging
from django.db import transaction
from django.utils import timezone
from django.core.cache import cache
from .models import (
    User, 
    RawVote, 
    RawSongTally,
    normalize_text,
    create_match_key,
    make_display_name,
)
from .matching import smart_normalize_vote, find_song_by_title_only
from .text_cleaning import clean_vote_text, correct_artist_typo

logger = logging.getLogger(__name__)

MAX_VOTES_PER_DAY = 5
SPAM_WINDOW_SECONDS = 60  # Time window for spam detection
SPAM_MAX_IDENTICAL = 3    # Max identical messages in window

# Pattern to match various dash separators: "artist - song", "artist- song", "artist -song", "artist-song"
SEPARATOR_PATTERN = re.compile(r'\s*-\s*')

# Pattern to detect URLs/links
URL_PATTERN = re.compile(
    r'https?://|www\.|'
    r'\b[a-zA-Z0-9-]+\.(com|org|net|io|co|me|buzz|info|biz|xyz|online|site|link|click)\b',
    re.IGNORECASE
)

# Pattern to detect emojis (common ranges)
EMOJI_PATTERN = re.compile(
    r'[\U0001F600-\U0001F64F]'  # Emoticons
    r'|[\U0001F300-\U0001F5FF]'  # Misc Symbols
    r'|[\U0001F680-\U0001F6FF]'  # Transport
    r'|[\U0001F1E0-\U0001F1FF]'  # Flags
    r'|[\U00002702-\U000027B0]'  # Dingbats
    r'|[\U0001F900-\U0001F9FF]'  # Supplemental
    r'|[\U0001FA00-\U0001FA6F]'  # Chess, etc
    r'|[\U0001FA70-\U0001FAFF]'  # More symbols
    r'|[\U00002600-\U000026FF]'  # Misc symbols
)

# Maximum allowed emojis in a vote
MAX_EMOJIS = 2

# Words/phrases that should be rejected (greetings, spam, etc.)
REJECTED_WORDS = {
    'link', 'hie', 'hi', 'hello', 'hey', 'helo', 'hallo',
    'good morning', 'good afternoon', 'good evening', 'good night',
    'how are you', 'how r u', 'whats up', 'wassup', 'watsup',
    'please', 'thanks', 'thank you', 'thanx',
    'send', 'give', 'share', 'forward',
    'join', 'subscribe', 'follow',
    'click', 'tap', 'open', 'visit',
    'free', 'win', 'winner', 'prize', 'money', 'cash',
    'call', 'contact', 'number', 'phone',
}


def validate_vote_content(text: str) -> tuple[bool, str | None]:
    """
    Validate that the vote content is acceptable.
    
    Returns:
        (is_valid, error_message) - error_message is None if valid
    """
    text_lower = text.lower().strip()
    
    # Check for URLs/links
    if URL_PATTERN.search(text):
        return False, (
            "❌ Links are not allowed.\n\n"
            "Please send only your vote:\n"
            "Artist - Song\n\n"
            "Example: Winky D - Ijipita"
        )
    
    # Check for rejected words/phrases (greetings, spam, etc.)
    for word in REJECTED_WORDS:
        # Check if the message IS just the word, or starts/ends with it
        if text_lower == word or text_lower.startswith(word + ' ') or text_lower.startswith(word + ','):
            return False, (
                "👋 This is a voting platform.\n\n"
                "To vote, send:\n"
                "Artist - Song\n\n"
                "Example: Winky D - Ijipita"
            )
    
    # Check for excessive emojis
    emojis = EMOJI_PATTERN.findall(text)
    if len(emojis) > MAX_EMOJIS:
        return False, (
            "❌ Too many emojis.\n\n"
            "Please send a simple vote:\n"
            "Artist - Song\n\n"
            "Example: Winky D - Ijipita"
        )
    
    # Check for very long messages (likely spam or paragraphs)
    if len(text) > 100:
        return False, (
            "❌ Message too long.\n\n"
            "Please send a simple vote:\n"
            "Artist - Song\n\n"
            "Example: Winky D - Ijipita"
        )
    
    # Check for multiple lines/paragraphs (newlines)
    if text.count('\n') > 1:
        return False, (
            "❌ Please send a single line vote.\n\n"
            "Format: Artist - Song\n\n"
            "Example: Winky D - Ijipita"
        )
    
    # Check for multiple sentences (multiple periods, question marks, etc.)
    sentence_enders = text.count('.') + text.count('?') + text.count('!')
    if sentence_enders > 2:
        return False, (
            "❌ Please send just the song vote.\n\n"
            "Format: Artist - Song\n\n"
            "Example: Winky D - Ijipita"
        )
    
    # Check for only emojis (no actual text)
    text_without_emojis = EMOJI_PATTERN.sub('', text).strip()
    if len(text_without_emojis) < 3:
        return False, (
            "❌ Please send a valid vote.\n\n"
            "Format: Artist - Song\n\n"
            "Example: Winky D - Ijipita"
        )
    
    return True, None


def check_spam(user_ref: str, message: str) -> tuple[bool, str | None]:
    """
    Check if a message is spam (repeated identical messages).
    
    Returns:
        (is_spam, error_message) - error_message is None if not spam
    """
    PRIVILEGED_USER_REF = '263786326862'
    if user_ref == PRIVILEGED_USER_REF:
        return False, None
    
    # Create a safe cache key (no spaces or special chars)
    import hashlib
    message_hash = hashlib.md5(normalize_text(message).encode()).hexdigest()[:16]
    cache_key = f"spam_{user_ref}_{message_hash}"
    
    try:
        count = cache.get(cache_key, 0)
        
        if count >= SPAM_MAX_IDENTICAL:
            return True, (
                "⚠️ You've sent this message too many times.\n\n"
                "Please wait a moment before trying again."
            )
        
        # Increment count with expiry
        cache.set(cache_key, count + 1, SPAM_WINDOW_SECONDS)
        
    except Exception as e:
        # If cache fails, don't block the user
        logger.warning(f"Spam check cache error: {e}")
    
    return False, None


def parse_vote_input(text: str):
    """
    Parse user input into artist and song parts.
    Handles various separator formats:
    - "Killer T - Hwahwa"
    - "Killer T- Hwahwa"  
    - "Killer T -Hwahwa"
    - "Killer T-Hwahwa"
    
    Also handles song-only input (no dash).
    
    Returns:
        (artist, song) - tuple of strings
        None - if completely invalid
        'song_only' - special marker + song name tuple for song-only votes
    """
    # Must contain at least one dash for standard format
    if '-' not in text:
        # Could be a song-only vote
        cleaned = text.strip()
        if len(cleaned) >= 3 and len(cleaned) <= 100:
            # Return as song-only (artist is None)
            return (None, cleaned)
        return None
    
    # Split on dash with optional surrounding spaces
    parts = SEPARATOR_PATTERN.split(text, maxsplit=1)
    
    if len(parts) != 2:
        return None
    
    artist = parts[0].strip()
    song = parts[1].strip()
    
    if len(artist) < 2 or len(song) < 2:
        return None
    
    return artist, song


class VotingService:
    def __init__(self, channel: str, user_ref: str):
        self.channel = channel
        self.user_ref = user_ref

    def handle_incoming_text(self, text: str) -> str:
        text = (text or '').strip()
        if not text:
            return self._welcome_message()

        normalized = text.lower()
        if normalized in {'/start', 'start'}:
            return self._welcome_message()
        if normalized in {'/help', 'help'}:
            return self._help_message()

        # Create or get user
        user, _ = User.objects.get_or_create(channel=self.channel, user_ref=self.user_ref)

        # Count today's votes
        today = timezone.localdate()
        todays_count = RawVote.objects.filter(user=user, vote_date=today).count()
        PRIVILEGED_USER_REF = '263786326862'
        if self.user_ref != PRIVILEGED_USER_REF and todays_count >= MAX_VOTES_PER_DAY:
            return (
                f"🚫 You have used all {MAX_VOTES_PER_DAY} votes for today.\n\n"
                "Come back tomorrow to vote again!"
            )

        # Validate content first (reject links, excessive emojis, etc.)
        is_valid, error_msg = validate_vote_content(text)
        if not is_valid:
            return error_msg
        
        # Check for spam (repeated identical messages)
        is_spam, spam_msg = check_spam(self.user_ref, text)
        if is_spam:
            return spam_msg
        
        # Parse input - handles "artist - song", "artist- song", "artist -song", "artist-song"
        parsed = parse_vote_input(text)
        if not parsed:
            return (
                "❌ Invalid format.\n\n"
                "Please use: Artist - Song\n"
                "Example: Winky D - Ijipita"
            )
        
        # Handle song-only votes (user doesn't know the artist)
        if parsed[0] is None:
            song_raw = parsed[1]
            
            # Try to find existing song with matching title
            existing_song = find_song_by_title_only(song_raw)
            
            if existing_song:
                # Found a matching song - use its artist
                artist_display, song_display, match_key, display_name = existing_song
                artist_raw = artist_display
                artist_normalized = normalize_text(artist_display)
                song_normalized = normalize_text(song_raw)
            else:
                # No match found - use Unknown Artist
                artist_raw = "Unknown Artist"
                artist_display = "Unknown Artist"
                song_display = song_raw.title()
                song_normalized = normalize_text(song_raw)
                match_key = f"unknown::{song_normalized}"
                display_name = f"Unknown Artist - {song_display}"
                artist_normalized = "unknown"
        else:
            artist_raw, song_raw = parsed

            # Smart normalization with verified artist matching + fuzzy matching
            artist_display, song_display, match_key, display_name = smart_normalize_vote(
                artist_raw, song_raw, today
            )
            
            # Keep normalized versions for the raw vote record
            artist_normalized = normalize_text(artist_raw)
            song_normalized = normalize_text(song_raw)

        # Record vote and update tally
        with transaction.atomic():
            RawVote.objects.create(
                user=user,
                raw_input=text,
                artist_raw=artist_raw,
                song_raw=song_raw,
                artist_normalized=artist_normalized,
                song_normalized=song_normalized,
                match_key=match_key,
                display_name=display_name,
                vote_date=today,
            )
            
            # Update or create tally
            tally, created = RawSongTally.objects.get_or_create(
                date=today,
                match_key=match_key,
                defaults={'display_name': display_name, 'count': 0}
            )
            tally.count += 1
            # Update display_name to use the most recent formatting
            tally.display_name = display_name
            tally.save()
        
        # Real-time vote processing - clean votes immediately
        self._process_vote_async(today)

        new_count = todays_count + 1
        remaining = MAX_VOTES_PER_DAY - new_count
        PRIVILEGED_USER_REF = '263786326862'
        if self.user_ref == PRIVILEGED_USER_REF:
            return (
                f"✅ Vote recorded!\n\n"
                f"🎵 {display_name}\n\n"
                f"(Privileged user: unlimited votes allowed)"
            )
        if remaining > 0:
            return (
                f"✅ Vote recorded!\n\n"
                f"🎵 {display_name}\n\n"
                f"You have {remaining} vote{'s' if remaining != 1 else ''} remaining today."
            )
        else:
            return (
                f"✅ Vote recorded!\n\n"
                f"🎵 {display_name}\n\n"
                f"🎉 You've used all your votes for today. Thanks for voting!"
            )

    @staticmethod
    def _welcome_message() -> str:
        return (
            "🎶 Welcome to Radio Zimbabwe Top 100!\n\n"
            "Vote for your favorite songs!\n\n"
            "Send: Artist - Song\n"
            "Example: Winky D - Ijipita\n\n"
            f"You can vote for up to {MAX_VOTES_PER_DAY} songs per day."
        )

    @staticmethod
    def _help_message() -> str:
        return (
            "📋 How to vote:\n\n"
            "Send: Artist - Song\n"
            "Example: Jah Prayzah - Mwana WaMambo\n\n"
            f"• You can vote for up to {MAX_VOTES_PER_DAY} different songs per day\n"
            "• You cannot vote for the same song twice in one day\n"
            "• Votes reset daily at midnight\n\n"
            "Type /start to begin!"
        )

    def _process_vote_async(self, date):
        """
        Process votes in real-time after each vote is recorded.
        Uses LLM (Anthropic) to match votes against verified songs.
        
        Flow:
        1. Get the vote's match_key
        2. Check if already mapped to a verified song
        3. If not, use CleaningService with LLM to find matches
        4. If matched, update the tally to link to verified song
        
        Errors are logged but don't affect the vote confirmation.
        """
        try:
            from .cleaning import CleaningService
            service = CleaningService()
            # Process with LLM enabled for smart matching
            stats = service.process_new_votes(date, use_spotify=True, use_llm=True)
            logger.info(f"Real-time vote processing completed for {date}: {stats}")
        except Exception as e:
            # Log error but don't fail the vote
            logger.error(f"Real-time vote processing error: {e}")
