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
from .text_cleaning import clean_vote_text, correct_artist_typo

logger = logging.getLogger(__name__)

MAX_VOTES_PER_DAY = 5
SPAM_WINDOW_SECONDS = 60  # Time window for spam detection
SPAM_MAX_IDENTICAL = 3  # Max identical messages in window

# Pattern to match various dash separators: "artist - song", "artist- song", "artist -song", "artist-song"
SEPARATOR_PATTERN = re.compile(r"\s*-\s*")

# Pattern to detect URLs/links
URL_PATTERN = re.compile(
    r"https?://|www\.|"
    r"\b[a-zA-Z0-9-]+\.(com|org|net|io|co|me|buzz|info|biz|xyz|online|site|link|click)\b",
    re.IGNORECASE,
)

# Pattern to detect emojis (common ranges)
EMOJI_PATTERN = re.compile(
    r"[\U0001F600-\U0001F64F]"  # Emoticons
    r"|[\U0001F300-\U0001F5FF]"  # Misc Symbols
    r"|[\U0001F680-\U0001F6FF]"  # Transport
    r"|[\U0001F1E0-\U0001F1FF]"  # Flags
    r"|[\U00002702-\U000027B0]"  # Dingbats
    r"|[\U0001F900-\U0001F9FF]"  # Supplemental
    r"|[\U0001FA00-\U0001FA6F]"  # Chess, etc
    r"|[\U0001FA70-\U0001FAFF]"  # More symbols
    r"|[\U00002600-\U000026FF]"  # Misc symbols
)

# Maximum allowed emojis in a vote
MAX_EMOJIS = 2

# Words/phrases that should be rejected (greetings, spam, etc.)
REJECTED_WORDS = {
    "link",
    "hie",
    "hi",
    "hello",
    "hey",
    "helo",
    "hallo",
    "good morning",
    "good afternoon",
    "good evening",
    "good night",
    "how are you",
    "how r u",
    "whats up",
    "wassup",
    "watsup",
    "please",
    "thanks",
    "thank you",
    "thanx",
    "send",
    "give",
    "share",
    "forward",
    "join",
    "subscribe",
    "follow",
    "click",
    "tap",
    "open",
    "visit",
    "free",
    "win",
    "winner",
    "prize",
    "money",
    "cash",
    "call",
    "contact",
    "number",
    "phone",
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
        if (
            text_lower == word
            or text_lower.startswith(word + " ")
            or text_lower.startswith(word + ",")
        ):
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
    if text.count("\n") > 1:
        return False, (
            "❌ Please send a single line vote.\n\n"
            "Format: Artist - Song\n\n"
            "Example: Winky D - Ijipita"
        )

    # Check for multiple sentences (multiple periods, question marks, etc.)
    sentence_enders = text.count(".") + text.count("?") + text.count("!")
    if sentence_enders > 2:
        return False, (
            "❌ Please send just the song vote.\n\n"
            "Format: Artist - Song\n\n"
            "Example: Winky D - Ijipita"
        )

    # Check for only emojis (no actual text)
    text_without_emojis = EMOJI_PATTERN.sub("", text).strip()
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
    if "-" not in text:
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
    def __init__(self, channel: str, user_ref: str, station="radio_zimbabwe"):
        from apps.accounts.models import Station

        if station not in Station.values:
            raise ValueError("Unknown station")
        self.channel, self.user_ref, self.station = channel, user_ref, station

    def handle_incoming_text(self, text: str, vote_date=None) -> str:
        """Record a vote and durable processing job. No external I/O on this path."""
        from django.conf import settings
        from django.db.models import F
        from .models import VoteJob, CleanedSong, MatchKeyMapping

        text = (text or "").strip()
        if not text or text.lower() in {"/start", "start"}:
            return self._welcome_message()
        if text.lower() in {"/help", "help"}:
            return self._help_message()
        valid, error = validate_vote_content(text)
        if not valid:
            return error
        parsed = parse_vote_input(text)
        if not parsed:
            return "Please use: Artist - Song. Example: Winky D - Ijipita"
        artist_raw, song_raw = parsed
        # Title-only votes are resolved only when there is exactly one verified candidate.
        if artist_raw is None:
            songs = list(
                CleanedSong.objects.filter(
                    station=self.station, status="verified", title__iexact=song_raw
                )[:2]
            )
            artist_raw = songs[0].artist if len(songs) == 1 else "Unknown Artist"
        artist, title = clean_vote_text(artist_raw, song_raw)
        artist = correct_artist_typo(artist)
        if not artist.strip() or not title.strip():
            return "Please include both an artist and a song title."
        match_key = create_match_key(artist, title)
        display_name = make_display_name(artist, title)
        today = vote_date or timezone.localdate()
        limit = settings.VOTING_DAILY_LIMIT
        # Lock the voter row before counting: different requests for one voter cannot
        # both use the final slot. Production requires PostgreSQL row locking.
        with transaction.atomic():
            user, _ = User.objects.get_or_create(
                channel=self.channel, user_ref=self.user_ref, station=self.station
            )
            user = User.objects.select_for_update().get(pk=user.pk)
            votes = RawVote.objects.filter(
                user=user, station=self.station, vote_date=today
            )
            if not settings.ALLOW_REPEAT_SONG:
                keys = [match_key]
                mapping = MatchKeyMapping.objects.filter(
                    station=self.station, match_key=match_key
                ).first()
                if mapping:
                    keys = MatchKeyMapping.objects.filter(
                        station=self.station, cleaned_song=mapping.cleaned_song
                    ).values_list("match_key", flat=True)
                if votes.filter(match_key__in=keys).exists():
                    return "⚠️ You already voted for this song today. Please choose another song."
            count = votes.count()
            if count >= limit:
                return (
                    f"🚫 You have used all {limit} votes for today. Come back tomorrow!"
                )
            vote = RawVote.objects.create(
                user=user,
                station=self.station,
                raw_input=text,
                artist_raw=artist_raw,
                song_raw=song_raw,
                artist_normalized=normalize_text(artist),
                song_normalized=normalize_text(title),
                match_key=match_key,
                display_name=display_name,
                vote_date=today,
            )
            tally, _ = RawSongTally.objects.get_or_create(
                station=self.station,
                date=today,
                match_key=match_key,
                defaults={"display_name": display_name, "count": 0},
            )
            RawSongTally.objects.filter(pk=tally.pk).update(count=F("count") + 1)
            VoteJob.objects.create(vote=vote)
        remaining = limit - count - 1
        return f"✅ Vote recorded!\n\n🎵 {display_name}\n\nYou have {remaining} vote{'s' if remaining != 1 else ''} remaining today."

    @staticmethod
    def _welcome_message():
        from django.conf import settings

        return f"🎶 Welcome to Radio Zimbabwe Top 100!\n\nSend: Artist - Song\nExample: Winky D - Ijipita\n\nUp to {settings.VOTING_DAILY_LIMIT} votes per day."

    @staticmethod
    def _help_message():
        from django.conf import settings

        policy = (
            "Repeat songs are allowed."
            if settings.ALLOW_REPEAT_SONG
            else "Choose a different song for each vote."
        )
        return f"📋 How to vote:\n\nSend: Artist - Song\n{settings.VOTING_DAILY_LIMIT} votes per day. {policy}\nVotes reset at midnight, Africa/Harare."
