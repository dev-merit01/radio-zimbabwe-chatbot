import re
from django.conf import settings
from django.db import models
from django.utils import timezone
from apps.accounts.models import Station
from .presentation import music_name


def normalize_text(text: str) -> str:
    """
    Normalize text for matching:
    - Strip whitespace
    - Collapse multiple spaces to single space
    - Lowercase
    """
    text = text.strip()
    text = re.sub(r"\s+", " ", text)  # collapse multiple spaces
    return text.lower()


def create_match_key(artist: str, song: str) -> str:
    """Create a normalized key for matching duplicate votes."""
    artist_norm = normalize_text(artist)
    song_norm = normalize_text(song)
    return f"{artist_norm}::{song_norm}"


def make_display_name(artist: str, song: str) -> str:
    """Create a clean display name from raw input."""
    artist = re.sub(r"\s+", " ", artist.strip())
    song = re.sub(r"\s+", " ", song.strip())
    return f"{music_name(artist)} - {music_name(song)}"


class ChartShow(models.Model):
    """A permanent programme identity, independent from its editable display name/code."""

    KIND_CHOICES = [
        ("legacy", "Legacy station chart"),
        ("weekly", "Regular chart show"),
        ("year_end", "End-of-year campaign"),
    ]
    MODE_CHOICES = [
        ("weekly", "Weekly schedule"),
        ("manual", "Manual"),
        ("campaign", "Date range"),
    ]
    station = models.CharField(max_length=32, choices=Station.choices, db_index=True)
    name = models.CharField(max_length=120)
    # Broadcast/programme information must not be mistaken for voting hours.
    presenter_name = models.CharField(max_length=120, blank=True)
    broadcast_start = models.TimeField(null=True, blank=True)
    broadcast_end = models.TimeField(null=True, blank=True)
    code = models.CharField(max_length=16, blank=True)
    kind = models.CharField(max_length=16, choices=KIND_CHOICES, default="weekly")
    schedule_mode = models.CharField(max_length=16, choices=MODE_CHOICES, default="weekly")
    is_enabled = models.BooleanField(default=False)
    manual_open = models.BooleanField(default=False)
    broadcast_weekday = models.PositiveSmallIntegerField(default=5)
    opens_weekday = models.PositiveSmallIntegerField(default=0)
    closes_weekday = models.PositiveSmallIntegerField(default=5)
    opens_time = models.TimeField(default="08:00")
    closes_time = models.TimeField(default="18:00")
    campaign_start = models.DateTimeField(null=True, blank=True)
    campaign_end = models.DateTimeField(null=True, blank=True)
    chart_size = models.PositiveSmallIntegerField(default=20)
    daily_limit = models.PositiveSmallIntegerField(default=5)
    is_archived = models.BooleanField(default=False)
    # Activated after a presenter publishes the first eligible song list.
    eligibility_enforced = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["station", "code"],
                condition=~models.Q(code=""),
                name="unique_station_show_code",
            ),
            models.UniqueConstraint(
                fields=["station"],
                condition=models.Q(kind="legacy"),
                name="unique_legacy_show_per_station",
            ),
        ]
        ordering = ["station", "kind", "name", "id"]
        indexes = [models.Index(fields=["station", "is_enabled"], name="av_show_enabled_idx")]

    def __str__(self):
        return f"{self.get_station_display()}: {self.name}"

    def accepts_at(self, instant):
        """Always use the receipt time, never the eventual worker processing time."""
        if not self.is_enabled or self.is_archived or self.kind == "legacy":
            return False
        from zoneinfo import ZoneInfo
        from django.conf import settings
        from django.utils import timezone

        when = timezone.localtime(instant, ZoneInfo(settings.TIME_ZONE))
        if self.schedule_mode == "manual":
            return self.manual_open
        if self.schedule_mode == "campaign":
            return bool(
                self.campaign_start and self.campaign_end
                and self.campaign_start <= instant < self.campaign_end
            )
        if self.schedule_mode != "weekly":
            return False
        # Compare within a repeating seven-day interval, including a window
        # spanning midnight/week boundaries (e.g. Friday to next Monday).
        start_minutes = self.opens_weekday * 1440 + self.opens_time.hour * 60 + self.opens_time.minute
        end_minutes = self.closes_weekday * 1440 + self.closes_time.hour * 60 + self.closes_time.minute
        if end_minutes <= start_minutes:
            end_minutes += 7 * 1440
        current_minutes = when.weekday() * 1440 + when.hour * 60 + when.minute
        return (start_minutes <= current_minutes < end_minutes
                or start_minutes <= current_minutes + 7 * 1440 < end_minutes)


class EligibleEdition(models.Model):
    """Presenter-published shortlist for one immutable show voting period."""

    # Preserve the 'locked' persisted value for existing ranking/audit queries.
    # Historical submitted/returned rows are migrated back to editable drafts.
    STATUS = [
        ("draft", "Draft"), ("locked", "Published"),
    ]
    station = models.CharField(max_length=32, choices=Station.choices, db_index=True)
    chart_show = models.ForeignKey(
        ChartShow, on_delete=models.PROTECT, related_name="eligible_editions",
    )
    chart_date = models.DateField()
    voting_start = models.DateField()
    voting_end = models.DateField()
    status = models.CharField(max_length=16, choices=STATUS, default="draft")
    prepared_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL,
        related_name="prepared_eligible_editions",
    )
    approved_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True,
        on_delete=models.SET_NULL, related_name="approved_eligible_editions",
    )
    approved_at = models.DateTimeField(null=True, blank=True)
    note = models.CharField(max_length=500, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["chart_show", "chart_date"],
                name="unique_eligible_chart_edition",
            ),
        ]
        ordering = ["-chart_date", "-id"]


class EditionCandidate(models.Model):
    edition = models.ForeignKey(
        EligibleEdition, on_delete=models.CASCADE, related_name="candidates",
    )
    song = models.ForeignKey(
        "CleanedSong", on_delete=models.PROTECT, related_name="candidate_editions",
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["edition", "song"], name="unique_edition_song",
            ),
        ]


class CandidateSongSuggestion(models.Model):
    """Presenter nominations of genuinely new songs, pending admin verification."""

    STATUS = [
        ("pending", "Pending review"), ("accepted", "Accepted"),
        ("rejected", "Rejected"),
    ]
    edition = models.ForeignKey(
        EligibleEdition, on_delete=models.PROTECT, related_name="suggestions",
    )
    artist = models.CharField(max_length=256)
    title = models.CharField(max_length=256)
    notes = models.CharField(max_length=500, blank=True)
    status = models.CharField(max_length=16, choices=STATUS, default="pending")
    suggested_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL,
        related_name="candidate_suggestions",
    )
    matched_song = models.ForeignKey(
        "CleanedSong", null=True, blank=True, on_delete=models.PROTECT,
        related_name="resolved_candidate_suggestions",
    )
    reviewed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True,
        on_delete=models.SET_NULL, related_name="reviewed_candidate_suggestions",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    reviewed_at = models.DateTimeField(null=True, blank=True)




class StationVotingConfig(models.Model):
    station = models.CharField(max_length=32, choices=Station.choices, unique=True)
    # Until explicitly activated, old code-free votes continue to the legacy chart.
    require_codes = models.BooleanField(default=False)
    # Applies to chart points only, never the number of authentic listener votes.
    vote_multiplier = models.PositiveSmallIntegerField(default=1)
    updated_at = models.DateTimeField(auto_now=True)


class UnassignedVote(models.Model):
    """Authentic inbound messages that cannot safely be assigned to a show."""

    station = models.CharField(max_length=32, choices=Station.choices)
    provider = models.CharField(max_length=16)
    message_id = models.CharField(max_length=200)
    sender = models.CharField(max_length=64)
    text = models.CharField(max_length=2048)
    reason = models.CharField(max_length=40)
    state = models.CharField(max_length=16, default="pending")
    received_at = models.DateTimeField()
    resolved_show = models.ForeignKey(
        ChartShow, null=True, blank=True, on_delete=models.PROTECT,
        related_name="resolved_unassigned_votes",
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["provider", "station", "message_id"],
                name="unique_unassigned_message",
            )
        ]
        indexes = [models.Index(fields=["station", "state", "-received_at"], name="av_unassigned_idx")]


class SuspectedSpamVote(models.Model):
    """Suspected automated/rapid inbound messages, held before vote creation.

    Human review is mandatory before a held message enters the AI voting
    pipeline. Provider IDs are unique so retries cannot bypass a flag.
    """

    station = models.CharField(max_length=32, choices=Station.choices)
    chart_show = models.ForeignKey(
        ChartShow, null=True, blank=True, on_delete=models.PROTECT,
        related_name="spam_reviews",
    )
    provider = models.CharField(max_length=16)
    message_id = models.CharField(max_length=200)
    sender = models.CharField(max_length=64)
    text = models.CharField(max_length=2048)
    vote_text = models.CharField(max_length=2048)
    submitted_code = models.CharField(max_length=16, blank=True)
    reason = models.CharField(max_length=32)
    state = models.CharField(
        max_length=16, default="pending",
        choices=[
            ("pending", "Awaiting review"),
            ("released", "Released by administrator"),
            ("dismissed", "Dismissed by administrator"),
        ],
    )
    received_at = models.DateTimeField()
    resolved_by = models.ForeignKey(
        "auth.User", null=True, blank=True, on_delete=models.SET_NULL,
    )
    resolved_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["provider", "station", "message_id"],
                name="unique_flagged_inbound_message",
            )
        ]
        indexes = [
            models.Index(
                fields=["station", "sender", "-received_at"],
                name="av_spam_sender_idx",
            ),
            models.Index(
                fields=["station", "state", "-received_at"],
                name="av_spam_queue_idx",
            ),
        ]



class User(models.Model):
    CHANNEL_CHOICES = (
        ("telegram", "Telegram"),
        ("whatsapp", "WhatsApp"),
        ("manual", "Manual entry"),
    )
    channel = models.CharField(max_length=16, choices=CHANNEL_CHOICES)
    user_ref = models.CharField(max_length=64)
    station = models.CharField(
        max_length=32, choices=Station.choices, default=Station.RADIO_ZIMBABWE
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("channel", "user_ref", "station")


class RawVote(models.Model):
    """
    Stores raw user votes with normalization for grouping.
    No Spotify verification - just stores what the user typed.
    """

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    station = models.CharField(
        max_length=32,
        choices=Station.choices,
        default=Station.RADIO_ZIMBABWE,
        db_index=True,
    )
    chart_show = models.ForeignKey(
        ChartShow, on_delete=models.PROTECT, null=True, blank=True,
        related_name="raw_votes", db_index=True,
    )
    submitted_code = models.CharField(max_length=16, blank=True)
    raw_input = models.CharField(max_length=512)  # exactly what user typed
    artist_raw = models.CharField(max_length=256)  # artist part before normalization
    song_raw = models.CharField(max_length=256)  # song part before normalization
    artist_normalized = models.CharField(max_length=256)  # lowercase, trimmed
    song_normalized = models.CharField(max_length=256)  # lowercase, trimmed
    match_key = models.CharField(
        max_length=512, db_index=True
    )  # "artist::song" for grouping
    display_name = models.CharField(
        max_length=512
    )  # cleaned "Artist - Song" for display
    vote_date = models.DateField()
    # Older votes remain 'legacy'. All newly received AI-first votes start
    # 'awaiting' and are excluded from verified tallies until AI/review approves.
    ai_status = models.CharField(
        max_length=16, default="legacy", db_index=True,
        choices=[
            ("legacy", "Legacy verified workflow"),
            ("awaiting", "Awaiting AI"),
            ("approved", "AI/reviewer approved"),
            ("review", "Needs human review"),
        ],
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        # Removed unique_together constraint to allow unlimited votes for same song per day
        ordering = ["-vote_date", "-created_at"]  # Most recent first
        indexes = [
            models.Index(fields=["vote_date", "match_key"]),
            models.Index(fields=["match_key"]),
            models.Index(fields=["user", "vote_date"]),
            models.Index(fields=["station", "vote_date"]),
            models.Index(fields=["station", "-created_at", "-id"]),
        ]

    def __str__(self):
        return f"{self.user} -> {self.display_name} ({self.vote_date})"


class RawSongTally(models.Model):
    """
    Daily vote counts grouped by match_key.
    Updated whenever a RawVote is recorded.
    """

    date = models.DateField()
    station = models.CharField(
        max_length=32,
        choices=Station.choices,
        default=Station.RADIO_ZIMBABWE,
        db_index=True,
    )
    match_key = models.CharField(max_length=512)
    display_name = models.CharField(
        max_length=512
    )  # Best display name for this match_key
    count = models.IntegerField(default=0)

    class Meta:
        unique_together = ("date", "station", "match_key")
        ordering = ["-date", "-count"]  # Highest vote counts first
        indexes = [
            models.Index(fields=["date", "-count"]),
            models.Index(fields=["station", "date"]),
            models.Index(fields=["station", "match_key", "date"]),
        ]

    def __str__(self):
        return f"{self.display_name}: {self.count} votes ({self.date})"


# ============================================================
# Legacy catalogue tables (not used by the station voting pipeline)
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
        help_text="True if this song has been verified by any station and can be trusted",
    )
    added_by_station = models.CharField(
        max_length=32,
        choices=Station.choices,
        default=Station.RADIO_ZIMBABWE,
        help_text="Station that first added this song to the catalog",
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
        ordering = ["artist", "title"]
        verbose_name = "Song Catalog Entry"
        verbose_name_plural = "Song Catalog"
        indexes = [
            models.Index(fields=["is_globally_verified"]),
            models.Index(fields=["canonical_name"]),
            models.Index(fields=["added_by_station"]),
        ]

    def __str__(self):
        icon = "✅" if self.is_globally_verified else "📝"
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
        ("pending", "Pending Review"),
        ("verified", "Verified"),
        ("rejected", "Rejected"),
    )

    station = models.CharField(max_length=32, choices=Station.choices, db_index=True)
    catalog_song = models.ForeignKey(
        SongCatalog, on_delete=models.CASCADE, related_name="station_songs"
    )
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default="pending")

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("station", "catalog_song")
        ordering = ["catalog_song__artist", "catalog_song__title"]
        verbose_name = "Station Song"
        verbose_name_plural = "Station Songs"
        indexes = [
            models.Index(fields=["station", "status"]),
        ]

    def __str__(self):
        status_icon = {"pending": "⏳", "verified": "✅", "rejected": "❌"}.get(
            self.status, ""
        )
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

    Authoritative station-owned catalogue. Names and verification are independent
    for each station. Legacy SongCatalog/StationSong tables are not used by AirVote.
    """

    STATUS_CHOICES = (
        ("pending", "Pending Review"),
        ("verified", "Verified"),
        ("rejected", "Rejected"),
    )

    # Station scope
    station = models.CharField(
        max_length=32,
        choices=Station.choices,
        default=Station.RADIO_ZIMBABWE,
        db_index=True,
    )

    # Canonical display info
    artist = models.CharField(max_length=256)
    title = models.CharField(max_length=256)
    canonical_name = models.CharField(max_length=512)  # "Artist - Title"

    # Status
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default="pending")

    # Optional Spotify enrichment
    spotify_track_id = models.CharField(max_length=64, blank=True, null=True)
    album = models.CharField(max_length=256, blank=True)
    image_url = models.URLField(blank=True)
    preview_url = models.URLField(blank=True)

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("station", "canonical_name")
        ordering = ["artist", "title"]
        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["station", "status"]),
            models.Index(fields=["canonical_name"]),
        ]

    def __str__(self):
        status_icon = {"pending": "⏳", "verified": "✅", "rejected": "❌"}.get(
            self.status, ""
        )
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


class SongAlias(models.Model):
    """Curated, station-scoped catalogue aliases; never created by an LLM alone.

    Aliases are intentionally not unique across songs. When an expression
    refers to multiple verified songs, retrieval returns competing candidates
    and the vote remains in review.
    """
    KIND_CHOICES = (
        ("full", "Full listener expression"),
        ("artist", "Artist/stage-name alias"),
        ("title", "Song title/short title"),
    )
    station = models.CharField(max_length=32, choices=Station.choices, db_index=True)
    song = models.ForeignKey(
        CleanedSong, on_delete=models.CASCADE, related_name="aliases"
    )
    kind = models.CharField(max_length=12, choices=KIND_CHOICES)
    value = models.CharField(max_length=512)
    normalised = models.CharField(max_length=512, db_index=True)
    is_approved = models.BooleanField(default=False, db_index=True)
    source = models.CharField(max_length=24, default="reviewer")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("station", "kind", "normalised", "song")
        indexes = [
            models.Index(fields=["station", "kind", "normalised"], name="voting_songa_station_512778_idx"),
            models.Index(fields=["station", "song", "is_approved"], name="voting_songa_station_76d1d8_idx"),
        ]

    def save(self, *args, **kwargs):
        from .catalogue_matching import normalise

        self.normalised = normalise(self.value)
        if not self.normalised:
            raise ValueError("An alias must contain searchable text.")
        if not self.song_id or self.song.station != self.station:
            raise ValueError("Aliases must belong to a song in the same station.")
        super().save(*args, **kwargs)


class MatchKeyMapping(models.Model):
    """
    Maps raw match_keys to CleanedSong entries.
    Allows multiple raw variations to point to one canonical song.
    Station-scoped: each station has its own mappings.
    """

    station = models.CharField(
        max_length=32,
        choices=Station.choices,
        default=Station.RADIO_ZIMBABWE,
        db_index=True,
    )
    match_key = models.CharField(max_length=512, db_index=True)
    cleaned_song = models.ForeignKey(
        CleanedSong, on_delete=models.CASCADE, related_name="match_keys"
    )

    # For tracking which raw display_name was most common
    sample_display_name = models.CharField(max_length=512)
    vote_count = models.IntegerField(default=0)  # Total votes with this match_key

    # Auto-mapped or manually reviewed
    is_auto_mapped = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("station", "match_key")
        indexes = [
            models.Index(fields=["station", "match_key"]),
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
    station = models.CharField(
        max_length=32,
        choices=Station.choices,
        default=Station.RADIO_ZIMBABWE,
        db_index=True,
    )
    cleaned_song = models.ForeignKey(CleanedSong, on_delete=models.CASCADE)
    count = models.IntegerField(default=0)

    class Meta:
        unique_together = ("date", "station", "cleaned_song")
        indexes = [
            models.Index(fields=["date", "-count"]),
            models.Index(fields=["station", "date"]),
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
        unique_together = ("user", "song", "vote_date")
        indexes = [
            models.Index(fields=["vote_date", "song"]),
        ]


class DailyTally(models.Model):
    """Legacy: Daily tallies for Spotify songs."""

    date = models.DateField()
    song = models.ForeignKey(Song, on_delete=models.CASCADE)
    count = models.IntegerField(default=0)

    class Meta:
        unique_together = ("date", "song")
        indexes = [
            models.Index(fields=["date", "count"]),
        ]


class DailyChart(models.Model):
    """Legacy: Computed daily chart."""

    date = models.DateField()
    rank = models.IntegerField()
    song = models.ForeignKey(Song, on_delete=models.CASCADE)
    count = models.IntegerField(default=0)
    computed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("date", "rank")
        indexes = [
            models.Index(fields=["date", "rank"]),
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
        ("match", "Matched to Verified"),
        ("reject", "Rejected as Spam"),
        ("new", "Marked as New Song"),
        ("auto_merge", "Auto-Merged"),
        ("auto_reject", "Auto-Rejected"),
    )

    station = models.CharField(
        max_length=32,
        choices=Station.choices,
        default=Station.RADIO_ZIMBABWE,
        db_index=True,
    )

    # What was being processed
    input_text = models.CharField(
        max_length=512, help_text="Original song name being processed"
    )
    input_type = models.CharField(
        max_length=32, default="pending_song", help_text="Type: pending_song, raw_vote"
    )

    # LLM decision
    action = models.CharField(max_length=32, choices=ACTION_CHOICES)
    confidence = models.CharField(max_length=16, help_text="high, medium, low, none")
    reasoning = models.TextField(blank=True, help_text="LLM's explanation")

    # What it was matched to (if applicable)
    matched_song = models.ForeignKey(
        "CleanedSong",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="llm_matches",
    )
    matched_song_name = models.CharField(
        max_length=512, blank=True, help_text="Snapshot of matched song name"
    )

    # Was the action applied?
    was_applied = models.BooleanField(
        default=False, help_text="Whether the action was actually applied"
    )

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "LLM Decision Log"
        verbose_name_plural = "LLM Decision Logs"
        indexes = [
            models.Index(fields=["-created_at"]),
            models.Index(fields=["action"]),
        ]

    def __str__(self):
        icon = {
            "match": "🔗",
            "reject": "🗑️",
            "new": "🆕",
            "auto_merge": "✅",
            "auto_reject": "❌",
        }.get(self.action, "❓")
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
        ("zimdancehall", "Zimdancehall"),
        ("sungura", "Sungura"),
        ("chimurenga", "Chimurenga"),
        ("gospel", "Gospel"),
        ("afropop", "Afropop"),
        ("rnb", "R&B"),
        ("hiphop", "Hip Hop"),
        ("jazz", "Jazz"),
        ("other", "Other"),
    )

    name = models.CharField(
        max_length=256, unique=True, help_text="Artist name as commonly known"
    )
    name_normalized = models.CharField(
        max_length=256, db_index=True, help_text="Lowercase for matching"
    )
    aliases = models.TextField(
        blank=True, help_text="Other names/spellings, one per line"
    )
    genre = models.CharField(max_length=32, choices=GENRE_CHOICES, default="other")
    is_active = models.BooleanField(
        default=True, help_text="Currently active in the industry"
    )
    spotify_artist_id = models.CharField(
        max_length=64, blank=True, help_text="Spotify Artist ID if available"
    )
    notes = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]
        verbose_name = "Verified Artist"
        verbose_name_plural = "Verified Artists"

    def save(self, *args, **kwargs):
        self.name_normalized = normalize_text(self.name)
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name

    def get_all_names(self) -> set:
        """Get all names including aliases (normalized)."""
        names = {self.name_normalized}
        if self.aliases:
            for alias in self.aliases.strip().split("\n"):
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
    Administrators save Saturday editions and a separate December year-end Top 50.
    Station-scoped: each station has its own weekly charts.
    """

    station = models.CharField(
        max_length=32,
        choices=Station.choices,
        default=Station.RADIO_ZIMBABWE,
        db_index=True,
    )

    chart_show = models.ForeignKey(
        ChartShow, null=True, blank=True, on_delete=models.PROTECT,
        related_name="published_charts",
    )
    # Freeze the actual edition used for position comparisons at publication.
    comparison_chart = models.ForeignKey(
        "self", null=True, blank=True, on_delete=models.PROTECT,
        related_name="compared_by",
    )
    chart_date = models.DateField(null=True, blank=True, help_text="Saturday broadcast or December year-end publication date; blank for legacy archives")

    # Week identification
    week_start = models.DateField(help_text="First voting day included in this snapshot")
    week_end = models.DateField(
        help_text="Last voting day included in this snapshot"
    )
    week_number = models.IntegerField(help_text="ISO week number 1-52/53")
    year = models.IntegerField()

    # Chart metadata
    is_year_end = models.BooleanField(
        default=False, help_text="True for Dec 31st Top 50"
    )
    chart_size = models.IntegerField(
        default=20, help_text="20 for regular, 50 for year-end"
    )
    total_votes = models.IntegerField(default=0, help_text="Total votes for this week")
    # Snapshot of the station multiplier at publication. Historical charts stay x1.
    vote_multiplier = models.PositiveSmallIntegerField(default=1)
    unique_songs = models.IntegerField(default=0, help_text="Unique songs voted for")

    # Status
    is_finalized = models.BooleanField(
        default=False, help_text="Chart has been locked/published"
    )
    finalized_at = models.DateTimeField(null=True, blank=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("chart_show", "year", "week_number", "is_year_end")
        ordering = ["-year", "-week_number"]
        verbose_name = "Weekly Chart"
        verbose_name_plural = "Weekly Charts"
        indexes = [
            models.Index(fields=["-year", "-week_number"]),
            models.Index(fields=["week_end"]),
            models.Index(fields=["station", "-year", "-week_number"]),
        ]

    def __str__(self):
        if self.is_year_end:
            return f"🏆 Year-End Top 50 - {self.year}"
        return f"Week {self.week_number}, {self.year} (Top {self.chart_size})"


class WeeklyChartEntry(models.Model):
    """
    Individual entries in a weekly chart.
    """

    chart = models.ForeignKey(
        WeeklyChart, on_delete=models.CASCADE, related_name="entries"
    )
    rank = models.IntegerField()

    # Song details (denormalized for historical accuracy)
    cleaned_song = models.ForeignKey(
        CleanedSong, on_delete=models.SET_NULL, null=True, related_name="chart_entries"
    )
    title = models.CharField(max_length=256)
    artist = models.CharField(max_length=256)
    canonical_name = models.CharField(max_length=512)

    # Vote data
    vote_count = models.IntegerField(default=0)

    # Movement tracking
    previous_rank = models.IntegerField(
        null=True, blank=True, help_text="Rank from previous week"
    )
    movement_status = models.CharField(
        max_length=16, blank=True, default="",
        choices=[
            ("up", "Climbing"),
            ("down", "Falling"),
            ("same", "Unchanged"),
            ("new", "New entry"),
            ("re_entry", "Re-entry"),
            ("no_previous", "No comparison available"),
        ],
        help_text="Frozen position movement when this chart was published.",
    )
    weeks_on_chart = models.IntegerField(
        default=1, help_text="Consecutive weeks on chart"
    )
    peak_rank = models.IntegerField(default=1, help_text="Highest position reached")

    # Spotify data (snapshot)
    spotify_track_id = models.CharField(max_length=64, blank=True)
    image_url = models.URLField(blank=True)
    album = models.CharField(max_length=256, blank=True)

    class Meta:
        unique_together = ("chart", "rank")
        ordering = ["chart", "rank"]
        indexes = [
            models.Index(fields=["chart", "rank"]),
        ]

    def __str__(self):
        return f"#{self.rank} {self.canonical_name} ({self.chart})"

    @property
    def movement(self):
        """Legacy display code. Movement status is snapshotted on publication."""
        if self.previous_rank is None:
            return {
                "re_entry": "re-entry",
                "no_previous": "—",
            }.get(self.movement_status, "new")
        diff = self.previous_rank - self.rank
        return f"+{diff}" if diff > 0 else str(diff) if diff < 0 else "="


class WorkItem(models.Model):
    """Durable work with expiring leases; PostgreSQL is required for multiple workers."""

    state = models.CharField(max_length=16, default="queued", db_index=True)
    attempts = models.PositiveIntegerField(default=0)
    available_at = models.DateTimeField(default=timezone.now)
    lease_until = models.DateTimeField(null=True, blank=True)
    lease_token = models.CharField(max_length=36, blank=True)
    error = models.CharField(max_length=240, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class InboundEvent(WorkItem):
    provider = models.CharField(max_length=16)
    station = models.CharField(max_length=32, choices=Station.choices)
    message_id = models.CharField(max_length=200)
    chart_show = models.ForeignKey(
        ChartShow, null=True, blank=True, on_delete=models.PROTECT,
        related_name="inbound_events",
    )
    vote_text = models.CharField(max_length=2048, blank=True)
    submitted_code = models.CharField(max_length=16, blank=True)
    sender = models.CharField(max_length=64)
    text = models.CharField(max_length=2048, blank=True)
    media_type = models.CharField(max_length=32, blank=True)
    reply = models.TextField(blank=True)
    # Receipt time, not eventual worker time, determines the voting day.
    received_at = models.DateTimeField(default=timezone.now)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["provider", "station", "message_id"],
                name="unique_inbound_delivery",
            )
        ]
        indexes = [models.Index(fields=["state", "available_at"])]


class VoteJob(WorkItem):
    class Meta:
        indexes = [models.Index(fields=["state", "available_at"])]

    vote = models.OneToOneField(
        RawVote, on_delete=models.CASCADE, related_name="processing_job"
    )


class OutboundMessage(WorkItem):
    class Meta:
        indexes = [models.Index(fields=["state", "available_at"])]

    event = models.OneToOneField(
        InboundEvent, on_delete=models.CASCADE, related_name="outbound"
    )
    text = models.TextField()
    provider_message_id = models.CharField(max_length=200, blank=True)


class StationState(models.Model):
    station = models.CharField(max_length=32, choices=Station.choices, unique=True)
    # A row lock serialises short review/recount/publication transactions per station.
    updated_at = models.DateTimeField(auto_now=True)


class ReviewAudit(models.Model):
    class Meta:
        indexes = [models.Index(fields=["station", "-created_at", "-id"])]

    station = models.CharField(max_length=32, choices=Station.choices, db_index=True)
    actor = models.ForeignKey("auth.User", on_delete=models.SET_NULL, null=True)
    action = models.CharField(max_length=32)
    details = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)


class WorkerHeartbeat(models.Model):
    queue = models.CharField(max_length=16, default="all")
    name = models.CharField(max_length=100, unique=True)
    last_seen = models.DateTimeField(auto_now=True)
