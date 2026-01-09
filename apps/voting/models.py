import re
from django.db import models
from apps.accounts.models import Station


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
    station = models.CharField(max_length=32, choices=Station.choices, default=Station.RADIO_ZIMBABWE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('channel', 'user_ref', 'station')


class RawVote(models.Model):
    """
    Stores raw user votes with normalization for grouping.
    No Spotify verification - just stores what the user typed.
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    station = models.CharField(max_length=32, choices=Station.choices, default=Station.RADIO_ZIMBABWE, db_index=True)
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
        # Removed unique_together constraint to allow unlimited votes for same song per day
        ordering = ['-vote_date', '-created_at']  # Most recent first
        indexes = [
            models.Index(fields=['vote_date', 'match_key']),
            models.Index(fields=['match_key']),
            models.Index(fields=['user', 'vote_date']),
            models.Index(fields=['station', 'vote_date']),
        ]

    def __str__(self):
        return f"{self.user} -> {self.display_name} ({self.vote_date})"


class RawSongTally(models.Model):
    """
    Daily vote counts grouped by match_key.
    Updated whenever a RawVote is recorded.
    """
    date = models.DateField()
    station = models.CharField(max_length=32, choices=Station.choices, default=Station.RADIO_ZIMBABWE, db_index=True)
    match_key = models.CharField(max_length=512)
    display_name = models.CharField(max_length=512)  # Best display name for this match_key
    count = models.IntegerField(default=0)

    class Meta:
        unique_together = ('date', 'station', 'match_key')
        ordering = ['-date', '-count']  # Highest vote counts first
        indexes = [
            models.Index(fields=['date', '-count']),
            models.Index(fields=['station', 'date']),
        ]

    def __str__(self):
        return f"{self.display_name}: {self.count} votes ({self.date})"


# ============================================================
# Global Song Catalog & Station-Scoped Songs
# ============================================================

class SongCatalog(models.Model):
    """
    Global song catalog that any station can add to.
    Contains canonical song information and Spotify metadata.
    Songs can be shared across stations or kept station-specific.
    """
    # Canonical display info
    artist = models.CharField(max_length=256)
    title = models.CharField(max_length=256)
    canonical_name = models.CharField(max_length=512, unique=True)  # "Artist - Title"
    
    # Global verification status
    is_globally_verified = models.BooleanField(
        default=False, 
        help_text="True if this song has been verified by any station and can be trusted"
    )
    added_by_station = models.CharField(
        max_length=32, 
        choices=Station.choices, 
        default=Station.RADIO_ZIMBABWE,
        help_text="Station that first added this song to the catalog"
    )
    
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
        verbose_name = 'Song Catalog Entry'
        verbose_name_plural = 'Song Catalog'
        indexes = [
            models.Index(fields=['is_globally_verified']),
            models.Index(fields=['canonical_name']),
            models.Index(fields=['added_by_station']),
        ]
    
    def __str__(self):
        icon = '✅' if self.is_globally_verified else '📝'
        return f"{icon} {self.canonical_name}"
    
    def save(self, *args, **kwargs):
        # Auto-generate canonical_name
        if not self.canonical_name:
            self.canonical_name = f"{self.artist} - {self.title}"
        super().save(*args, **kwargs)


class StationSong(models.Model):
    """
    Station-scoped song entry that references the global catalog.
    Each station has its own copy with local status.
    When importing from a globally verified catalog song, status starts as 'verified'.
    """
    STATUS_CHOICES = (
        ('pending', 'Pending Review'),
        ('verified', 'Verified'),
        ('rejected', 'Rejected'),
    )
    
    station = models.CharField(max_length=32, choices=Station.choices, db_index=True)
    catalog_song = models.ForeignKey(
        SongCatalog, 
        on_delete=models.CASCADE, 
        related_name='station_songs'
    )
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default='pending')
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ('station', 'catalog_song')
        ordering = ['catalog_song__artist', 'catalog_song__title']
        verbose_name = 'Station Song'
        verbose_name_plural = 'Station Songs'
        indexes = [
            models.Index(fields=['station', 'status']),
        ]
    
    def __str__(self):
        status_icon = {'pending': '⏳', 'verified': '✅', 'rejected': '❌'}.get(self.status, '')
        return f"{status_icon} {self.catalog_song.canonical_name} ({self.get_station_display()})"
    
    # Convenience properties to access catalog song fields
    @property
    def artist(self):
        return self.catalog_song.artist
    
    @property
    def title(self):
        return self.catalog_song.title
    
    @property
    def canonical_name(self):
        return self.catalog_song.canonical_name
    
    @property
    def spotify_track_id(self):
        return self.catalog_song.spotify_track_id
    
    @property
    def album(self):
        return self.catalog_song.album
    
    @property
    def image_url(self):
        return self.catalog_song.image_url
    
    @property
    def preview_url(self):
        return self.catalog_song.preview_url


class CleanedSong(models.Model):
    """
    Canonical song entry after cleaning/verification.
    Multiple raw match_keys can map to one CleanedSong.
    
    NOTE: This model is being deprecated in favor of SongCatalog + StationSong.
    Kept for backward compatibility during migration.
    """
    STATUS_CHOICES = (
        ('pending', 'Pending Review'),
        ('verified', 'Verified'),
        ('rejected', 'Rejected'),
    )
    
    # Station scope
    station = models.CharField(max_length=32, choices=Station.choices, default=Station.RADIO_ZIMBABWE, db_index=True)
    
    # Canonical display info
    artist = models.CharField(max_length=256)
    title = models.CharField(max_length=256)
    canonical_name = models.CharField(max_length=512)  # "Artist - Title"
    
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
        unique_together = ('station', 'canonical_name')
        ordering = ['artist', 'title']
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['station', 'status']),
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
    Station-scoped: each station has its own mappings.
    """
    station = models.CharField(max_length=32, choices=Station.choices, default=Station.RADIO_ZIMBABWE, db_index=True)
    match_key = models.CharField(max_length=512, db_index=True)
    cleaned_song = models.ForeignKey(CleanedSong, on_delete=models.CASCADE, related_name='match_keys')
    
    # For tracking which raw display_name was most common
    sample_display_name = models.CharField(max_length=512)
    vote_count = models.IntegerField(default=0)  # Total votes with this match_key
    
    # Auto-mapped or manually reviewed
    is_auto_mapped = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ('station', 'match_key')
        indexes = [
            models.Index(fields=['station', 'match_key']),
        ]
    
    def __str__(self):
        return f"{self.match_key} → {self.cleaned_song.canonical_name}"


class CleanedSongTally(models.Model):
    """
    Daily vote counts for cleaned/verified songs.
    This is what the dashboard displays.
    Station-scoped: each station has its own daily tallies.
    """
    date = models.DateField()
    station = models.CharField(max_length=32, choices=Station.choices, default=Station.RADIO_ZIMBABWE, db_index=True)
    cleaned_song = models.ForeignKey(CleanedSong, on_delete=models.CASCADE)
    count = models.IntegerField(default=0)
    
    class Meta:
        unique_together = ('date', 'station', 'cleaned_song')
        indexes = [
            models.Index(fields=['date', '-count']),
            models.Index(fields=['station', 'date']),
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
    Station-scoped for tracking decisions per station.
    """
    ACTION_CHOICES = (
        ('match', 'Matched to Verified'),
        ('reject', 'Rejected as Spam'),
        ('new', 'Marked as New Song'),
        ('auto_merge', 'Auto-Merged'),
        ('auto_reject', 'Auto-Rejected'),
    )
    
    station = models.CharField(max_length=32, choices=Station.choices, default=Station.RADIO_ZIMBABWE, db_index=True)
    
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


# ============================================================
# Weekly Chart Archive
# ============================================================

class WeeklyChart(models.Model):
    """
    Stores finalized weekly Top 20/50 charts.
    Charts are finalized every Saturday, and Dec 31st is the Top 50.
    Station-scoped: each station has its own weekly charts.
    """
    station = models.CharField(max_length=32, choices=Station.choices, default=Station.RADIO_ZIMBABWE, db_index=True)
    
    # Week identification
    week_start = models.DateField(help_text="Monday of the chart week")
    week_end = models.DateField(help_text="Sunday of the chart week (chart published on Saturday)")
    week_number = models.IntegerField(help_text="ISO week number 1-52/53")
    year = models.IntegerField()
    
    # Chart metadata
    is_year_end = models.BooleanField(default=False, help_text="True for Dec 31st Top 50")
    chart_size = models.IntegerField(default=20, help_text="20 for regular, 50 for year-end")
    total_votes = models.IntegerField(default=0, help_text="Total votes for this week")
    unique_songs = models.IntegerField(default=0, help_text="Unique songs voted for")
    
    # Status
    is_finalized = models.BooleanField(default=False, help_text="Chart has been locked/published")
    finalized_at = models.DateTimeField(null=True, blank=True)
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ('station', 'year', 'week_number')
        ordering = ['-year', '-week_number']
        verbose_name = 'Weekly Chart'
        verbose_name_plural = 'Weekly Charts'
        indexes = [
            models.Index(fields=['-year', '-week_number']),
            models.Index(fields=['week_end']),
            models.Index(fields=['station', '-year', '-week_number']),
        ]
    
    def __str__(self):
        if self.is_year_end:
            return f"🏆 Year-End Top 50 - {self.year}"
        return f"Week {self.week_number}, {self.year} (Top {self.chart_size})"


class WeeklyChartEntry(models.Model):
    """
    Individual entries in a weekly chart.
    """
    chart = models.ForeignKey(WeeklyChart, on_delete=models.CASCADE, related_name='entries')
    rank = models.IntegerField()
    
    # Song details (denormalized for historical accuracy)
    cleaned_song = models.ForeignKey(
        CleanedSong, 
        on_delete=models.SET_NULL, 
        null=True, 
        related_name='chart_entries'
    )
    title = models.CharField(max_length=256)
    artist = models.CharField(max_length=256)
    canonical_name = models.CharField(max_length=512)
    
    # Vote data
    vote_count = models.IntegerField(default=0)
    
    # Movement tracking
    previous_rank = models.IntegerField(null=True, blank=True, help_text="Rank from previous week")
    weeks_on_chart = models.IntegerField(default=1, help_text="Consecutive weeks on chart")
    peak_rank = models.IntegerField(default=1, help_text="Highest position reached")
    
    # Spotify data (snapshot)
    spotify_track_id = models.CharField(max_length=64, blank=True)
    image_url = models.URLField(blank=True)
    album = models.CharField(max_length=256, blank=True)
    
    class Meta:
        unique_together = ('chart', 'rank')
        ordering = ['chart', 'rank']
        indexes = [
            models.Index(fields=['chart', 'rank']),
        ]
    
    def __str__(self):
        return f"#{self.rank} {self.canonical_name} ({self.chart})"
    
    @property
    def movement(self):
        """Calculate movement from previous week."""
        if self.previous_rank is None:
            return 'new'
        diff = self.previous_rank - self.rank
        if diff > 0:
            return f'+{diff}'
        elif diff < 0:
            return str(diff)
        return '='

