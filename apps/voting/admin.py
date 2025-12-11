from django.contrib import admin
from django.contrib import messages
from django.shortcuts import redirect
from django.urls import path
from django.utils.html import format_html
from django.utils import timezone
from django.db.models import Sum
from .models import (
    User,
    RawVote,
    RawSongTally,
    CleanedSong,
    MatchKeyMapping,
    CleanedSongTally,
    VerifiedArtist,
)
from .cleaning import CleaningService


@admin.register(User)
class UserAdmin(admin.ModelAdmin):
    list_display = ('id', 'channel', 'user_ref', 'created_at')
    list_filter = ('channel', 'created_at')
    search_fields = ('user_ref',)


@admin.register(RawVote)
class RawVoteAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'display_name', 'vote_date', 'created_at')
    list_filter = ('vote_date', 'created_at')
    search_fields = ('raw_input', 'display_name', 'match_key')
    date_hierarchy = 'vote_date'
    
    fieldsets = (
        ('Vote Details', {
            'fields': ('user', 'vote_date', 'raw_input')
        }),
        ('Parsed Data (Editable)', {
            'fields': ('artist_raw', 'song_raw', 'display_name'),
            'description': 'Edit these to correct the user\'s vote'
        }),
        ('Normalized Data (Auto-generated)', {
            'fields': ('artist_normalized', 'song_normalized', 'match_key'),
            'classes': ('collapse',),
            'description': 'These are auto-generated but can be edited if needed'
        }),
        ('Metadata', {
            'fields': ('created_at',),
            'classes': ('collapse',),
        }),
    )
    readonly_fields = ('created_at',)


@admin.register(RawSongTally)
class RawSongTallyAdmin(admin.ModelAdmin):
    list_display = ('id', 'display_name', 'count', 'date', 'match_key')
    list_filter = ('date',)
    search_fields = ('display_name', 'match_key')
    ordering = ('-date', '-count')
    change_list_template = 'admin/voting/rawsongtally/change_list.html'
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                'process-votes/',
                self.admin_site.admin_view(self.process_votes_view),
                name='voting_rawsongtally_process_votes',
            ),
        ]
        return custom_urls + urls
    
    def process_votes_view(self, request):
        """Admin view to process votes for today."""
        service = CleaningService()
        date = timezone.localdate()
        
        try:
            result = service.process_new_votes(date)
            messages.success(
                request,
                f"✅ Votes processed for {date}: "
                f"{result['new']} new songs, {result['auto_merged']} auto-merged, "
                f"{result.get('spotify_matched', 0)} Spotify matched"
            )
            
            # Check for pending songs
            pending = service.get_pending_review()
            if pending:
                messages.warning(
                    request,
                    f"⏳ {len(pending)} songs pending review. Check Cleaned Songs."
                )
        except Exception as e:
            messages.error(request, f"❌ Error processing votes: {e}")
        
        return redirect('admin:voting_rawsongtally_changelist')


class MatchKeyMappingInline(admin.TabularInline):
    model = MatchKeyMapping
    extra = 0
    readonly_fields = ('match_key', 'sample_display_name', 'vote_count', 'is_auto_mapped', 'created_at')
    can_delete = True


@admin.register(CleanedSong)
class CleanedSongAdmin(admin.ModelAdmin):
    list_display = ('id', 'status_badge', 'canonical_name', 'total_votes', 'has_spotify', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('artist', 'title', 'canonical_name')
    list_editable = ('status',) if False else ()  # Enable for bulk status changes
    actions = ['verify_songs', 'reject_songs', 'mark_pending']
    inlines = [MatchKeyMappingInline]
    change_list_template = 'admin/voting/cleanedsong/change_list.html'
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                'process-votes/',
                self.admin_site.admin_view(self.process_votes_view),
                name='voting_cleanedsong_process_votes',
            ),
        ]
        return custom_urls + urls
    
    def process_votes_view(self, request):
        """Admin view to process votes for today."""
        service = CleaningService()
        date = timezone.localdate()
        
        try:
            result = service.process_new_votes(date)
            messages.success(
                request,
                f"✅ Votes processed for {date}: "
                f"{result['new']} new songs, {result['auto_merged']} auto-merged, "
                f"{result.get('spotify_matched', 0)} Spotify matched"
            )
            
            # Check for pending songs
            pending = service.get_pending_review()
            if pending:
                messages.warning(
                    request,
                    f"⏳ {len(pending)} songs pending review."
                )
        except Exception as e:
            messages.error(request, f"❌ Error processing votes: {e}")
        
        return redirect('admin:voting_cleanedsong_changelist')
    
    fieldsets = (
        ('Song Info', {
            'fields': ('artist', 'title', 'canonical_name', 'status')
        }),
        ('Spotify Data', {
            'fields': ('spotify_track_id', 'album', 'image_url', 'preview_url'),
            'classes': ('collapse',),
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )
    readonly_fields = ('created_at', 'updated_at')
    
    def status_badge(self, obj):
        colors = {
            'pending': '#f0ad4e',
            'verified': '#5cb85c',
            'rejected': '#d9534f',
        }
        icons = {
            'pending': '⏳',
            'verified': '✅',
            'rejected': '❌',
        }
        color = colors.get(obj.status, '#000')
        icon = icons.get(obj.status, '')
        status_text = obj.status.title() if obj.status else 'Unknown'
        return format_html(
            '<span style="color: {color};">{icon} {status}</span>',
            color=color,
            icon=icon,
            status=status_text
        )
    status_badge.short_description = 'Status'
    status_badge.admin_order_field = 'status'
    
    def total_votes(self, obj):
        total = MatchKeyMapping.objects.filter(cleaned_song=obj).aggregate(
            total=Sum('vote_count')
        )['total'] or 0
        return total
    total_votes.short_description = 'Total Votes'
    
    def has_spotify(self, obj):
        if obj.spotify_track_id:
            return format_html('<span style="color: green;">{check}</span>', check='✓')
        return format_html('<span style="color: gray;">{dash}</span>', dash='-')
    has_spotify.short_description = 'Spotify'
    
    @admin.action(description='✅ Verify selected songs')
    def verify_songs(self, request, queryset):
        count = queryset.update(status='verified')
        # Update tallies for all verified songs
        for song in queryset:
            self._update_tallies(song)
        self.message_user(request, f'{count} songs verified and added to dashboard.')
    
    @admin.action(description='❌ Reject selected songs')
    def reject_songs(self, request, queryset):
        count = queryset.update(status='rejected')
        self.message_user(request, f'{count} songs rejected.')
    
    @admin.action(description='⏳ Mark as pending')
    def mark_pending(self, request, queryset):
        count = queryset.update(status='pending')
        self.message_user(request, f'{count} songs marked as pending.')
    
    def save_model(self, request, obj, form, change):
        """Auto-verify pending songs when edited, search Spotify, and update dashboard tallies."""
        was_pending = False
        if change and obj.pk:
            # Check if it was previously pending
            old_obj = CleanedSong.objects.filter(pk=obj.pk).first()
            was_pending = old_obj and old_obj.status == 'pending'
        
        # If editing a pending song and status wasn't explicitly changed to rejected
        if was_pending and obj.status == 'pending':
            # Search Spotify for this song
            try:
                from apps.spotify.search import resolve_with_confidence, is_high_confidence
                match, confidence = resolve_with_confidence(obj.artist, obj.title)
                
                if match and confidence > 0.4:  # Accept if reasonable match
                    obj.spotify_track_id = match['id']
                    obj.album = match.get('album', '')
                    obj.image_url = match.get('image_url', '')
                    obj.preview_url = match.get('preview_url', '')
                    # Use Spotify's artist/title if high confidence
                    if is_high_confidence(confidence):
                        obj.artist = ', '.join(match['artists'])
                        obj.title = match['title']
                    self.message_user(
                        request, 
                        f"✅ Found on Spotify: \"{match['title']}\" by {', '.join(match['artists'])} (confidence: {confidence:.0%})"
                    )
                else:
                    self.message_user(
                        request, 
                        f"⚠️ No good Spotify match found for \"{obj.artist} - {obj.title}\". Song verified without Spotify data.",
                        level='warning'
                    )
            except Exception as e:
                self.message_user(
                    request, 
                    f"⚠️ Spotify search failed: {e}. Song verified without Spotify data.",
                    level='warning'
                )
            
            # Mark as verified
            obj.status = 'verified'
        
        # Update canonical_name based on artist and title
        obj.canonical_name = f"{obj.artist} - {obj.title}"
        
        super().save_model(request, obj, form, change)
        
        # If song is now verified, ensure it has tallies in the dashboard
        if obj.status == 'verified':
            self._update_tallies(obj)
    
    def _update_tallies(self, cleaned_song):
        """Update CleanedSongTally based on MatchKeyMappings."""
        from django.db.models import Sum
        
        # Get all mappings for this cleaned song
        mappings = MatchKeyMapping.objects.filter(cleaned_song=cleaned_song)
        if not mappings.exists():
            return
        
        # Get match_keys
        match_keys = list(mappings.values_list('match_key', flat=True))
        
        # Get raw tallies by date for these match_keys
        raw_tallies = RawSongTally.objects.filter(
            match_key__in=match_keys
        ).values('date').annotate(total=Sum('count'))
        
        # Update or create CleanedSongTally for each date
        for tally_data in raw_tallies:
            CleanedSongTally.objects.update_or_create(
                date=tally_data['date'],
                cleaned_song=cleaned_song,
                defaults={'count': tally_data['total']}
            )


@admin.register(MatchKeyMapping)
class MatchKeyMappingAdmin(admin.ModelAdmin):
    list_display = ('id', 'match_key', 'cleaned_song', 'vote_count', 'is_auto_mapped')
    list_filter = ('is_auto_mapped', 'created_at')
    search_fields = ('match_key', 'sample_display_name', 'cleaned_song__canonical_name')
    raw_id_fields = ('cleaned_song',)


@admin.register(CleanedSongTally)
class CleanedSongTallyAdmin(admin.ModelAdmin):
    list_display = ('id', 'cleaned_song', 'count', 'date')
    list_filter = ('date',)
    search_fields = ('cleaned_song__canonical_name',)
    ordering = ('-date', '-count')
    date_hierarchy = 'date'


@admin.register(VerifiedArtist)
class VerifiedArtistAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'genre', 'is_active', 'has_spotify', 'created_at')
    list_filter = ('genre', 'is_active', 'created_at')
    search_fields = ('name', 'aliases', 'name_normalized')
    ordering = ('name',)
    
    fieldsets = (
        ('Artist Info', {
            'fields': ('name', 'genre', 'is_active')
        }),
        ('Aliases', {
            'fields': ('aliases',),
            'description': 'Other names or spellings for this artist (one per line). E.g., "Tuku" for Oliver Mtukudzi.'
        }),
        ('Spotify', {
            'fields': ('spotify_artist_id',),
            'classes': ('collapse',),
        }),
        ('Notes', {
            'fields': ('notes',),
            'classes': ('collapse',),
        }),
    )
    readonly_fields = ('name_normalized', 'created_at', 'updated_at')
    
    def has_spotify(self, obj):
        if obj.spotify_artist_id:
            return format_html('<span style="color: green;">{check}</span>', check='✓')
        return format_html('<span style="color: gray;">{dash}</span>', dash='-')
    has_spotify.short_description = 'Spotify'
