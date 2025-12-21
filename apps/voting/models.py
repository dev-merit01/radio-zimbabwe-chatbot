import re
from django.db import models


def normalize_text(text: str) -> str:
    """
    Normalize text for matching:
    - Strip whitespace
    - Collapse multiple spaces to single space
    - Lowercase
    """
    text = text.strip()
    text = re.sub(r'\s+', ' ', text)  # collapse multiple spaces
    return text.lower()


def create_match_key(artist: str, song: str) -> str:
    """Create a normalized key for matching duplicate votes."""
    artist_norm = normalize_text(artist)
    song_norm = normalize_text(song)
    return f"{artist_norm}::{song_norm}"


def make_display_name(artist: str, song: str) -> str:
    """Create a clean display name from raw input."""
    artist = re.sub(r'\s+', ' ', artist.strip())
    song = re.sub(r'\s+', ' ', song.strip())
    return f"{artist} - {song}"


class User(models.Model):
    CHANNEL_CHOICES = (
        ('telegram', 'Telegram'),
        ('whatsapp', 'WhatsApp'),
    )
    channel = models.CharField(max_length=16, choices=CHANNEL_CHOICES)
    user_ref = models.CharField(max_length=64)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('channel', 'user_ref')


class RawVote(models.Model):
    """
    Stores raw user votes with normalization for grouping.
    No Spotify verification - just stores what the user typed.
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    raw_input = models.CharField(max_length=512)  # exactly what user typed
    artist_raw = models.CharField(max_length=256)  # artist part before normalization
    song_raw = models.CharField(max_length=256)    # song part before normalization
    artist_normalized = models.CharField(max_length=256)  # lowercase, trimmed
    song_normalized = models.CharField(max_length=256)    # lowercase, trimmed
    match_key = models.CharField(max_length=512, db_index=True)  # "artist::song" for grouping
    display_name = models.CharField(max_length=512)  # cleaned "Artist - Song" for display
    vote_date = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        # One vote per user per song per day
        unique_together = ('user', 'match_key', 'vote_date')
        ordering = ['-vote_date', '-created_at']  # Most recent first
        indexes = [
            models.Index(fields=['vote_date', 'match_key']),
            models.Index(fields=['match_key']),
        ]

    def __str__(self):
        return f"{self.user} -> {self.display_name} ({self.vote_date})"


class RawSongTally(models.Model):
    """
    Daily vote counts grouped by match_key.
    Updated whenever a RawVote is recorded.
    """
    date = models.DateField()
    match_key = models.CharField(max_length=512)
    display_name = models.CharField(max_length=512)  # Best display name for this match_key
    count = models.IntegerField(default=0)

    class Meta:
        unique_together = ('date', 'match_key')
        ordering = ['-date', '-count']  # Highest vote counts first
        indexes = [
            models.Index(fields=['date', '-count']),
        ]

    def __str__(self):
        return f"{self.display_name}: {self.count} votes ({self.date})"


# ============================================================
# Cleaned/Verified Song Models
# ============================================================

class CleanedSong(models.Model):
    """
    Canonical song entry after cleaning/verification.
    Multiple raw match_keys can map to one CleanedSong.
    """
    STATUS_CHOICES = (
        ('pending', 'Pending Review'),
        ('verified', 'Verified'),
        ('rejected', 'Rejected'),
    )
    
    # Canonical display info
    artist = models.CharField(max_length=256)
    title = models.CharField(max_length=256)
    canonical_name = models.CharField(max_length=512, unique=True)  # "Artist - Title"
    
    # Status
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default='pending')
    
    # Optional Spotify enrichment
    spotify_track_id = models.CharField(max_length=64, blank=True, null=True)
    album = models.CharField(max_length=256, blank=True)
    image_url = models.URLField(blank=True)
    preview_url = models.URLField(blank=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['artist', 'title']
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['canonical_name']),
        ]
    
    def __str__(self):
        status_icon = {'pending': '⏳', 'verified': '✅', 'rejected': '❌'}.get(self.status, '')
        return f"{status_icon} {self.canonical_name}"
    
    def clean(self):
        # Auto-generate canonical_name if not provided
        if not self.canonical_name:
            self.canonical_name = f"{self.artist} - {self.title}"
        
        # Note: Duplicate checking is now handled in admin.save_model() 
        # which will merge duplicates instead of blocking
    
    def save(self, *args, **kwargs):
        # Auto-generate canonical_name
        if not self.canonical_name:
            self.canonical_name = f"{self.artist} - {self.title}"
        
        # Note: Duplicate checking/merging is handled in admin.save_model()
        # Direct saves (outside admin) will rely on database unique constraint
        
        super().save(*args, **kwargs)


class MatchKeyMapping(models.Model):
    """
    Maps raw match_keys to CleanedSong entries.
    Allows multiple raw variations to point to one canonical song.
    """
    match_key = models.CharField(max_length=512, unique=True, db_index=True)
    cleaned_song = models.ForeignKey(CleanedSong, on_delete=models.CASCADE, related_name='match_keys')
    
    # For tracking which raw display_name was most common
    sample_display_name = models.CharField(max_length=512)
    vote_count = models.IntegerField(default=0)  # Total votes with this match_key
    
    # Auto-mapped or manually reviewed
    is_auto_mapped = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.match_key} → {self.cleaned_song.canonical_name}"


class CleanedSongTally(models.Model):
    """
    Daily vote counts for cleaned/verified songs.
    This is what the dashboard displays.
    """
    date = models.DateField()
    cleaned_song = models.ForeignKey(CleanedSong, on_delete=models.CASCADE)
    count = models.IntegerField(default=0)
    
    class Meta:
        unique_together = ('date', 'cleaned_song')
        indexes = [
            models.Index(fields=['date', '-count']),
        ]
    
    def __str__(self):
        return f"{self.cleaned_song.canonical_name}: {self.count} votes ({self.date})"


# ============================================================
# Legacy models (kept for migration compatibility, can remove later)
# ============================================================

class Song(models.Model):
    """Legacy: Spotify-verified songs. Kept for backward compatibility."""
    spotify_track_id = models.CharField(max_length=64, unique=True)
    title = models.CharField(max_length=256)
    artists = models.CharField(max_length=256)
    album = models.CharField(max_length=256, blank=True)
    image_url = models.URLField(blank=True)
    preview_url = models.URLField(blank=True)
    popularity = models.IntegerField(default=0)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)


class Vote(models.Model):
    """Legacy: Spotify-verified votes. Kept for backward compatibility."""
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    song = models.ForeignKey(Song, on_delete=models.CASCADE)
    vote_date = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user', 'song', 'vote_date')
        indexes = [
            models.Index(fields=['vote_date', 'song']),
        ]


class DailyTally(models.Model):
    """Legacy: Daily tallies for Spotify songs."""
    date = models.DateField()
    song = models.ForeignKey(Song, on_delete=models.CASCADE)
    count = models.IntegerField(default=0)

    class Meta:
        unique_together = ('date', 'song')
        indexes = [
            models.Index(fields=['date', 'count']),
        ]


class DailyChart(models.Model):
    """Legacy: Computed daily chart."""
    date = models.DateField()
    rank = models.IntegerField()
    song = models.ForeignKey(Song, on_delete=models.CASCADE)
    count = models.IntegerField(default=0)
    computed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('date', 'rank')
        indexes = [
            models.Index(fields=['date', 'rank']),
        ]


# ============================================================
# LLM Decision Logging
# ============================================================

class LLMDecisionLog(models.Model):
    """
    Logs LLM decisions for auditing and review.
    Helps verify if the LLM is making correct matching/rejection decisions.
    """
    ACTION_CHOICES = (
        ('match', 'Matched to Verified'),
        ('reject', 'Rejected as Spam'),
        ('new', 'Marked as New Song'),
        ('auto_merge', 'Auto-Merged'),
        ('auto_reject', 'Auto-Rejected'),
    )
    
    # What was being processed
    input_text = models.CharField(max_length=512, help_text="Original song name being processed")
    input_type = models.CharField(max_length=32, default='pending_song', help_text="Type: pending_song, raw_vote")
    
    # LLM decision
    action = models.CharField(max_length=32, choices=ACTION_CHOICES)
    confidence = models.CharField(max_length=16, help_text="high, medium, low, none")
    reasoning = models.TextField(blank=True, help_text="LLM's explanation")
    
    # What it was matched to (if applicable)
    matched_song = models.ForeignKey(
        'CleanedSong', 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        related_name='llm_matches'
    )
    matched_song_name = models.CharField(max_length=512, blank=True, help_text="Snapshot of matched song name")
    
    # Was the action applied?
    was_applied = models.BooleanField(default=False, help_text="Whether the action was actually applied")
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-created_at']
        verbose_name = 'LLM Decision Log'
        verbose_name_plural = 'LLM Decision Logs'
        indexes = [
            models.Index(fields=['-created_at']),
            models.Index(fields=['action']),
        ]
    
    def __str__(self):
        icon = {
            'match': '🔗',
            'reject': '🗑️',
            'new': '🆕',
            'auto_merge': '✅',
            'auto_reject': '❌',
        }.get(self.action, '❓')
        return f"{icon} {self.input_text[:50]} → {self.action} ({self.confidence})"


# ============================================================
# Verified Artists
# ============================================================

class VerifiedArtist(models.Model):
    """
    Known/verified Zimbabwean artists.
    Used for boosting Spotify search results and validating votes.
    """
    GENRE_CHOICES = (
        ('zimdancehall', 'Zimdancehall'),
        ('sungura', 'Sungura'),
        ('chimurenga', 'Chimurenga'),
        ('gospel', 'Gospel'),
        ('afropop', 'Afropop'),
        ('rnb', 'R&B'),
        ('hiphop', 'Hip Hop'),
        ('jazz', 'Jazz'),
        ('other', 'Other'),
    )
    
    name = models.CharField(max_length=256, unique=True, help_text="Artist name as commonly known")
    name_normalized = models.CharField(max_length=256, db_index=True, help_text="Lowercase for matching")
    aliases = models.TextField(blank=True, help_text="Other names/spellings, one per line")
    genre = models.CharField(max_length=32, choices=GENRE_CHOICES, default='other')
    is_active = models.BooleanField(default=True, help_text="Currently active in the industry")
    spotify_artist_id = models.CharField(max_length=64, blank=True, help_text="Spotify Artist ID if available")
    notes = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['name']
        verbose_name = 'Verified Artist'
        verbose_name_plural = 'Verified Artists'

    def save(self, *args, **kwargs):
        self.name_normalized = normalize_text(self.name)
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name

    def get_all_names(self) -> set:
        """Get all names including aliases (normalized)."""
        names = {self.name_normalized}
        if self.aliases:
            for alias in self.aliases.strip().split('\n'):
                alias = alias.strip()
                if alias:
                    names.add(normalize_text(alias))
        return names
