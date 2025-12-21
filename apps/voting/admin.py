from django.contrib import admin
from django.contrib import messages
from django.shortcuts import redirect
from django.urls import path
from django.utils.html import format_html
from django.utils import timezone
from django.db.models import Sum
from django import forms
from .models import (
    User,
    RawVote,
    RawSongTally,
    CleanedSong,
    MatchKeyMapping,
    CleanedSongTally,
    VerifiedArtist,
    LLMDecisionLog,
)
from .cleaning import CleaningService


class CleanedSongForm(forms.ModelForm):
    """Custom form that allows duplicate canonical_name for merging."""
    
    class Meta:
        model = CleanedSong
        fields = '__all__'
    
    def validate_unique(self):
        """Skip unique validation - we handle duplicates via merge in save_model."""
        try:
            self.instance.validate_unique(exclude=['canonical_name'])
        except forms.ValidationError as e:
            self._update_errors(e)


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

    def save_model(self, request, obj, form, change):
        """
        When editing a RawVote, update the RawSongTally counts:
        - Decrement old song's tally (if match_key changed)
        - Increment new song's tally
        """
        from .models import normalize_text, create_match_key, make_display_name
        
        old_match_key = None
        old_vote_date = None
        
        # If editing an existing vote, get the old match_key
        if change and obj.pk:
            try:
                old_obj = RawVote.objects.get(pk=obj.pk)
                old_match_key = old_obj.match_key
                old_vote_date = old_obj.vote_date
            except RawVote.DoesNotExist:
                pass
        
        # Auto-update normalized fields and match_key based on edited artist/song
        obj.artist_normalized = normalize_text(obj.artist_raw)
        obj.song_normalized = normalize_text(obj.song_raw)
        obj.match_key = create_match_key(obj.artist_raw, obj.song_raw)
        obj.display_name = make_display_name(obj.artist_raw, obj.song_raw)
        
        # Save the vote
        super().save_model(request, obj, form, change)
        
        # Update tallies if match_key or date changed
        new_match_key = obj.match_key
        new_vote_date = obj.vote_date
        
        if change and (old_match_key != new_match_key or old_vote_date != new_vote_date):
            # Decrement old tally
            if old_match_key and old_vote_date:
                old_tally = RawSongTally.objects.filter(
                    match_key=old_match_key,
                    date=old_vote_date
                ).first()
                if old_tally:
                    old_tally.count = max(0, old_tally.count - 1)
                    if old_tally.count == 0:
                        old_tally.delete()
                    else:
                        old_tally.save()
            
            # Increment new tally
            new_tally, created = RawSongTally.objects.get_or_create(
                match_key=new_match_key,
                date=new_vote_date,
                defaults={'display_name': obj.display_name, 'count': 0}
            )
            new_tally.count += 1
            new_tally.display_name = obj.display_name
            new_tally.save()
            
            messages.success(
                request,
                f"✅ Vote updated and tally adjusted for '{obj.display_name}'"
            )
        elif not change:
            # New vote - increment tally
            new_tally, created = RawSongTally.objects.get_or_create(
                match_key=new_match_key,
                date=new_vote_date,
                defaults={'display_name': obj.display_name, 'count': 0}
            )
            new_tally.count += 1
            new_tally.display_name = obj.display_name
            new_tally.save()

    def delete_model(self, request, obj):
        """When deleting a vote, decrement the tally."""
        tally = RawSongTally.objects.filter(
            match_key=obj.match_key,
            date=obj.vote_date
        ).first()
        
        super().delete_model(request, obj)
        
        if tally:
            tally.count = max(0, tally.count - 1)
            if tally.count == 0:
                tally.delete()
            else:
                tally.save()

    def delete_queryset(self, request, queryset):
        """When bulk deleting votes, decrement all tallies."""
        # Collect all match_key/date pairs before deletion
        vote_data = list(queryset.values('match_key', 'vote_date'))
        
        super().delete_queryset(request, queryset)
        
        # Decrement tallies
        for data in vote_data:
            tally = RawSongTally.objects.filter(
                match_key=data['match_key'],
                date=data['vote_date']
            ).first()
            if tally:
                tally.count = max(0, tally.count - 1)
                if tally.count == 0:
                    tally.delete()
                else:
                    tally.save()


@admin.register(RawSongTally)
class RawSongTallyAdmin(admin.ModelAdmin):
    list_display = ('id', 'display_name', 'count', 'date', 'match_key', 'is_matched')
    list_filter = ('date',)
    search_fields = ('display_name', 'match_key')
    ordering = ('-date', '-count')
    change_list_template = 'admin/voting/rawsongtally/change_list.html'
    actions = ['llm_match_selected', 'llm_match_all_unmatched']
    
    def is_matched(self, obj):
        """Show if this tally is linked to a CleanedSong."""
        mapping = MatchKeyMapping.objects.filter(match_key=obj.match_key).first()
        if mapping:
            return format_html(
                '<span style="color: green;" title="{}">✓ {}</span>',
                mapping.cleaned_song.canonical_name,
                mapping.cleaned_song.canonical_name[:30] + '...' if len(mapping.cleaned_song.canonical_name) > 30 else mapping.cleaned_song.canonical_name
            )
        return format_html('<span style="color: orange;">⏳ Pending</span>')
    is_matched.short_description = 'Matched To'
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                'process-votes/',
                self.admin_site.admin_view(self.process_votes_view),
                name='voting_rawsongtally_process_votes',
            ),
            path(
                'llm-match/',
                self.admin_site.admin_view(self.llm_match_view),
                name='voting_rawsongtally_llm_match',
            ),
        ]
        return custom_urls + urls
    
    @admin.action(description='🤖 LLM Match selected votes')
    def llm_match_selected(self, request, queryset):
        """Use LLM to match selected vote tallies."""
        from .llm_matcher import match_votes_with_llm, get_verified_songs_list, create_match_mapping
        
        songs = get_verified_songs_list()
        if not songs:
            messages.error(request, '❌ No verified songs in database. Add some songs first!')
            return
        
        votes_data = [
            {
                'display_name': t.display_name,
                'match_key': t.match_key,
                'count': t.count,
            }
            for t in queryset
        ]
        
        try:
            results = match_votes_with_llm(votes_data, songs)
            
            auto_linked = 0
            for result in results:
                if result.should_auto_link:
                    tally = queryset.filter(match_key=result.match_key).first()
                    create_match_mapping(
                        match_key=result.match_key,
                        cleaned_song_id=result.matched_song_id,
                        sample_display_name=result.raw_input,
                        vote_count=tally.count if tally else 0,
                        is_auto_mapped=True,
                    )
                    auto_linked += 1
            
            high = sum(1 for r in results if r.confidence == 'high')
            medium = sum(1 for r in results if r.confidence == 'medium')
            
            messages.success(
                request,
                f'🤖 LLM Matching complete: {high} high confidence, {medium} medium confidence. '
                f'Auto-linked {auto_linked} votes.'
            )
        except Exception as e:
            messages.error(request, f'❌ LLM matching error: {e}')
    
    @admin.action(description='🤖 LLM Match ALL unmatched (up to 100)')
    def llm_match_all_unmatched(self, request, queryset):
        """Use LLM to match all unmatched vote tallies."""
        from .llm_matcher import process_unmatched_votes
        
        try:
            result = process_unmatched_votes(limit=100, auto_link_high_confidence=True)
            
            if 'error' in result:
                messages.error(request, f"❌ {result['error']}")
                return
            
            stats = result.get('stats', {})
            messages.success(
                request,
                f"🤖 LLM Matching complete: "
                f"{stats.get('high_confidence', 0)} high, "
                f"{stats.get('medium_confidence', 0)} medium, "
                f"{stats.get('low_confidence', 0)} low confidence. "
                f"Auto-linked {stats.get('auto_linked', 0)} votes."
            )
        except Exception as e:
            messages.error(request, f'❌ LLM matching error: {e}')
    
    def llm_match_view(self, request):
        """Admin view to run LLM matching on all unmatched votes."""
        from .llm_matcher import process_unmatched_votes
        
        try:
            result = process_unmatched_votes(limit=100, auto_link_high_confidence=True)
            
            if 'error' in result:
                messages.error(request, f"❌ {result['error']}")
            else:
                stats = result.get('stats', {})
                messages.success(
                    request,
                    f"🤖 LLM Matching complete: "
                    f"{stats.get('high_confidence', 0)} high, "
                    f"{stats.get('medium_confidence', 0)} medium, "
                    f"{stats.get('low_confidence', 0)} low confidence. "
                    f"Auto-linked {stats.get('auto_linked', 0)} votes."
                )
        except Exception as e:
            messages.error(request, f'❌ LLM matching error: {e}')
        
        return redirect('admin:voting_rawsongtally_changelist')
    
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
    form = CleanedSongForm  # Custom form that allows duplicate canonical_name for merging
    list_display = ('id', 'status_badge', 'canonical_name', 'total_votes', 'has_spotify', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('artist', 'title', 'canonical_name')
    list_editable = ('status',) if False else ()  # Enable for bulk status changes
    actions = ['verify_songs', 'reject_songs', 'mark_pending', 'llm_review_selected']
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
            path(
                'llm-review-pending/',
                self.admin_site.admin_view(self.llm_review_pending_view),
                name='voting_cleanedsong_llm_review',
            ),
            path(
                'llm-process-raw-votes/',
                self.admin_site.admin_view(self.llm_process_raw_votes_view),
                name='voting_cleanedsong_llm_raw_votes',
            ),
            path(
                'diagnose-votes/',
                self.admin_site.admin_view(self.diagnose_votes_view),
                name='voting_cleanedsong_diagnose',
            ),
            path(
                'recalculate-tallies/',
                self.admin_site.admin_view(self.recalculate_tallies_view),
                name='voting_cleanedsong_recalculate',
            ),
            path(
                'verify-all-pending/',
                self.admin_site.admin_view(self.verify_all_pending_view),
                name='voting_cleanedsong_verify_all_pending',
            ),
        ]
        return custom_urls + urls
    
    def verify_all_pending_view(self, request):
        """Verify all pending songs at once."""
        from .models import CleanedSong
        
        count = CleanedSong.objects.filter(status='pending').update(status='verified')
        messages.success(request, f"✅ Verified {count} pending songs! Now click 'Recalculate Tallies' to update dashboard.")
        
        return redirect('admin:voting_cleanedsong_changelist')
    
    def diagnose_votes_view(self, request):
        """Diagnose why votes aren't showing on dashboard."""
        from .models import RawVote, RawSongTally, CleanedSong, MatchKeyMapping, CleanedSongTally
        from django.db.models import Sum
        
        # Raw data
        raw_vote_count = RawVote.objects.count()
        raw_tally_count = RawSongTally.objects.count()
        raw_tally_votes = RawSongTally.objects.aggregate(total=Sum('count'))['total'] or 0
        
        # Cleaned songs by status
        cleaned_total = CleanedSong.objects.count()
        verified_count = CleanedSong.objects.filter(status='verified').count()
        pending_count = CleanedSong.objects.filter(status='pending').count()
        rejected_count = CleanedSong.objects.filter(status='rejected').count()
        
        # Mappings
        mapping_count = MatchKeyMapping.objects.count()
        
        # Mappings to verified songs
        verified_song_ids = set(CleanedSong.objects.filter(status='verified').values_list('id', flat=True))
        mappings_to_verified = MatchKeyMapping.objects.filter(cleaned_song_id__in=verified_song_ids).count()
        mappings_to_pending = MatchKeyMapping.objects.filter(cleaned_song__status='pending').count()
        mappings_to_rejected = MatchKeyMapping.objects.filter(cleaned_song__status='rejected').count()
        
        # Votes mapped to verified songs
        verified_mapped_votes = MatchKeyMapping.objects.filter(
            cleaned_song_id__in=verified_song_ids
        ).aggregate(total=Sum('vote_count'))['total'] or 0
        
        # Dashboard tallies
        dashboard_tally_count = CleanedSongTally.objects.count()
        dashboard_total_votes = CleanedSongTally.objects.aggregate(total=Sum('count'))['total'] or 0
        
        # Unmapped raw tallies
        mapped_keys = set(MatchKeyMapping.objects.values_list('match_key', flat=True))
        all_raw_keys = set(RawSongTally.objects.values_list('match_key', flat=True))
        unmapped_keys = all_raw_keys - mapped_keys
        unmapped_votes = RawSongTally.objects.filter(match_key__in=unmapped_keys).aggregate(total=Sum('count'))['total'] or 0
        
        messages.info(request, f"📊 RAW DATA: {raw_vote_count} RawVotes, {raw_tally_count} RawSongTally entries, {raw_tally_votes} total raw votes")
        messages.info(request, f"📁 CLEANED SONGS: {cleaned_total} total ({verified_count} verified, {pending_count} pending, {rejected_count} rejected)")
        messages.info(request, f"🔗 MAPPINGS: {mapping_count} total → {mappings_to_verified} to verified, {mappings_to_pending} to pending, {mappings_to_rejected} to rejected")
        messages.info(request, f"📈 DASHBOARD: {dashboard_tally_count} tally entries, {dashboard_total_votes} votes showing")
        
        if len(unmapped_keys) > 0:
            messages.warning(request, f"⚠️ UNMAPPED: {len(unmapped_keys)} raw entries ({unmapped_votes} votes) have no mapping!")
        
        if mappings_to_pending > 0:
            messages.warning(request, f"⚠️ {mappings_to_pending} mappings point to PENDING songs (not counting on dashboard)")
        
        if verified_mapped_votes > dashboard_total_votes:
            messages.warning(request, f"⚠️ Dashboard shows {dashboard_total_votes} but {verified_mapped_votes} are mapped to verified songs. Click 'Recalculate Tallies'!")
        
        return redirect('admin:voting_cleanedsong_changelist')
    
    def recalculate_tallies_view(self, request):
        """Recalculate CleanedSongTally from mappings."""
        from .models import RawSongTally, CleanedSong, MatchKeyMapping, CleanedSongTally
        from django.db.models import Sum
        from collections import defaultdict
        
        # Get all mappings to verified songs
        verified_song_ids = set(CleanedSong.objects.filter(status='verified').values_list('id', flat=True))
        mappings = MatchKeyMapping.objects.filter(cleaned_song_id__in=verified_song_ids)
        mapping_dict = {m.match_key: m.cleaned_song_id for m in mappings}
        
        # Aggregate votes by cleaned_song and date
        song_date_counts = defaultdict(lambda: defaultdict(int))
        
        for tally in RawSongTally.objects.all():
            cleaned_song_id = mapping_dict.get(tally.match_key)
            if cleaned_song_id:
                song_date_counts[cleaned_song_id][tally.date] += tally.count
        
        # Update CleanedSongTally
        created = 0
        updated = 0
        total_votes = 0
        
        for song_id, date_counts in song_date_counts.items():
            for date, count in date_counts.items():
                obj, was_created = CleanedSongTally.objects.update_or_create(
                    cleaned_song_id=song_id,
                    date=date,
                    defaults={'count': count}
                )
                if was_created:
                    created += 1
                else:
                    updated += 1
                total_votes += count
        
        messages.success(
            request,
            f"✅ Recalculated tallies: {created} created, {updated} updated. "
            f"Dashboard now shows {total_votes} total votes across all dates."
        )
        
        return redirect('admin:voting_cleanedsong_changelist')
    
    def llm_process_raw_votes_view(self, request):
        """Admin view to process raw votes with LLM and update dashboard."""
        from .llm_matcher import process_all_raw_votes
        from .models import RawSongTally, CleanedSong, MatchKeyMapping, CleanedSongTally
        from django.db.models import Sum
        from collections import defaultdict
        
        # Count total unmapped
        all_raw_keys = list(RawSongTally.objects.values_list('match_key', flat=True))
        mapped_keys = set(MatchKeyMapping.objects.values_list('match_key', flat=True))
        unmapped_keys = [k for k in all_raw_keys if k not in mapped_keys]
        total_unmapped = len(set(unmapped_keys))
        
        if total_unmapped > 0:
            # Process unmapped votes with LLM
            try:
                result = process_all_raw_votes(
                    limit=100,
                    batch_size=20,
                    dry_run=False,
                )
                
                if 'error' in result:
                    messages.error(request, f"❌ LLM Error: {result['error']}")
                else:
                    stats = result.get('stats', {})
                    remaining = result.get('remaining', 0)
                    
                    messages.success(
                        request,
                        f"🤖 Processed {stats.get('processed', 0)} votes: "
                        f"{stats.get('matched', 0)} matched, "
                        f"{stats.get('new_songs', 0)} new, "
                        f"{stats.get('rejected', 0)} rejected"
                    )
                    
                    if remaining > 0:
                        messages.warning(request, f"⏳ {remaining} more to process. Click again!")
                        
            except Exception as e:
                messages.error(request, f"❌ Error: {e}")
        else:
            messages.info(request, "✅ All votes already mapped!")
        
        # Always recalculate dashboard tallies
        verified_song_ids = set(CleanedSong.objects.filter(status='verified').values_list('id', flat=True))
        mappings = MatchKeyMapping.objects.filter(cleaned_song_id__in=verified_song_ids)
        mapping_dict = {m.match_key: m.cleaned_song_id for m in mappings}
        
        song_date_counts = defaultdict(lambda: defaultdict(int))
        for tally in RawSongTally.objects.all():
            cleaned_song_id = mapping_dict.get(tally.match_key)
            if cleaned_song_id:
                song_date_counts[cleaned_song_id][tally.date] += tally.count
        
        total_votes = 0
        for song_id, date_counts in song_date_counts.items():
            for date, count in date_counts.items():
                CleanedSongTally.objects.update_or_create(
                    cleaned_song_id=song_id,
                    date=date,
                    defaults={'count': count}
                )
                total_votes += count
        
        messages.success(request, f"📊 Dashboard updated: {total_votes} total votes")
        
        return redirect('admin:voting_cleanedsong_changelist')
    
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
    
    def llm_review_pending_view(self, request):
        """Admin view to process ALL pending songs with LLM."""
        from .llm_matcher import process_pending_songs, get_pending_songs
        from .models import CleanedSong
        
        # Get count of ALL pending songs
        total_pending = CleanedSong.objects.filter(status='pending').count()
        
        if total_pending == 0:
            messages.info(request, "✅ No pending songs to review!")
            return redirect('admin:voting_cleanedsong_changelist')
        
        try:
            # Process pending songs in smaller batches to avoid Render timeout
            result = process_pending_songs(
                limit=20,  # Smaller batch to avoid timeout
                auto_merge=True,
                auto_reject=False,  # Don't auto-reject, keep for manual review
                dry_run=False,
            )
            
            if 'error' in result:
                messages.error(request, f"❌ LLM Error: {result['error']}")
            else:
                stats = result.get('stats', {})
                results = result.get('results', [])
                remaining = total_pending - stats.get('total_processed', 0)
                
                # Show detailed merge results
                merge_details = [r for r in results if r['action'] == 'match' and r['matched_to']]
                reject_suggestions = [r for r in results if r['action'] == 'reject']
                
                messages.success(
                    request,
                    f"🤖 LLM Review Complete: Processed {stats.get('total_processed', 0)} of {total_pending} pending songs. "
                    f"{stats.get('auto_merged', 0)} auto-merged, "
                    f"{stats.get('rejected', 0)} suggested for rejection, "
                    f"{stats.get('new_songs', 0)} new songs. "
                )
                
                if remaining > 0:
                    messages.warning(
                        request,
                        f"⏳ {remaining} more pending songs to process. Click 'LLM Review Pending' again to continue."
                    )
                
                # Show what was merged
                for r in merge_details[:5]:  # Show up to 5 examples
                    messages.info(
                        request,
                        f"🔗 \"{r['pending_name']}\" → \"{r['matched_to']}\" ({r['confidence']})"
                    )
                
                # Show rejection suggestions
                for r in reject_suggestions[:3]:
                    messages.warning(
                        request,
                        f"⚠️ Review: \"{r['pending_name']}\" - {r['reasoning']}"
                    )
                    
        except Exception as e:
            messages.error(request, f"❌ LLM processing error: {e}")
        
        return redirect('admin:voting_cleanedsong_changelist')
    
    @admin.action(description='🤖 LLM Review Selected')
    def llm_review_selected(self, request, queryset):
        """Use LLM to review selected pending songs."""
        from .llm_matcher import match_pending_songs_with_llm, get_verified_songs_list, merge_pending_to_verified
        
        # Only process pending songs
        pending_songs = queryset.filter(status='pending')
        if not pending_songs.exists():
            self.message_user(request, "⚠️ No pending songs in selection.", level='warning')
            return
        
        verified_songs = get_verified_songs_list()
        if not verified_songs:
            self.message_user(request, "❌ No verified songs to match against.", level='error')
            return
        
        try:
            results = match_pending_songs_with_llm(list(pending_songs), verified_songs)
            
            merged = 0
            rejected = 0
            new_songs = 0
            
            for r in results:
                if r.action == 'match' and r.confidence == 'high':
                    if merge_pending_to_verified(r.pending_song, r.matched_song_name):
                        merged += 1
                elif r.action == 'reject':
                    r.pending_song.status = 'rejected'
                    r.pending_song.save()
                    rejected += 1
                else:
                    new_songs += 1
            
            self.message_user(
                request,
                f"🤖 LLM Review: {merged} merged, {rejected} rejected, {new_songs} need manual review"
            )
        except Exception as e:
            self.message_user(request, f"❌ LLM Error: {e}", level='error')
        
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
        """
        Auto-verify pending songs when edited, search Spotify, and update dashboard tallies.
        If the song matches an existing one, merge them instead of blocking.
        """
        was_pending = False
        old_canonical_name = None
        
        if change and obj.pk:
            # Check if it was previously pending and get old canonical name
            old_obj = CleanedSong.objects.filter(pk=obj.pk).first()
            if old_obj:
                was_pending = old_obj.status == 'pending'
                old_canonical_name = old_obj.canonical_name
        
        # Update canonical_name based on artist and title
        new_canonical_name = f"{obj.artist} - {obj.title}"
        obj.canonical_name = new_canonical_name
        
        # Check if this matches an existing song (case-insensitive)
        existing = CleanedSong.objects.filter(
            canonical_name__iexact=new_canonical_name
        ).exclude(pk=obj.pk).first()
        
        if existing:
            # MERGE: Transfer all mappings and tallies to the existing song
            self._merge_into_existing(request, obj, existing)
            return  # Don't save the current object, it will be deleted
        
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
                        obj.canonical_name = f"{obj.artist} - {obj.title}"
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
        
        super().save_model(request, obj, form, change)
        
        # If song is now verified, ensure it has tallies in the dashboard
        if obj.status == 'verified':
            self._update_tallies(obj)

    def _merge_into_existing(self, request, source_song, target_song):
        """
        Merge source_song into target_song:
        - Transfer all MatchKeyMappings
        - Merge CleanedSongTally counts
        - Delete source_song
        """
        from django.db import transaction
        from django.db.models import F
        
        with transaction.atomic():
            # Transfer MatchKeyMappings from source to target
            mappings_transferred = MatchKeyMapping.objects.filter(
                cleaned_song=source_song
            ).update(cleaned_song=target_song)
            
            # Merge CleanedSongTally - add source counts to target
            source_tallies = CleanedSongTally.objects.filter(cleaned_song=source_song)
            for source_tally in source_tallies:
                target_tally, created = CleanedSongTally.objects.get_or_create(
                    date=source_tally.date,
                    cleaned_song=target_song,
                    defaults={'count': 0}
                )
                target_tally.count += source_tally.count
                target_tally.save()
            
            # Delete source tallies
            source_tallies.delete()
            
            # Delete the source song (if it exists in DB)
            if source_song.pk:
                source_song.delete()
            
            # Update tallies on target
            self._update_tallies(target_song)
        
        messages.success(
            request,
            f"✅ Merged into existing song \"{target_song.canonical_name}\" (ID: {target_song.pk}). "
            f"{mappings_transferred} vote mapping(s) transferred."
        )
    
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


@admin.register(LLMDecisionLog)
class LLMDecisionLogAdmin(admin.ModelAdmin):
    """Admin for reviewing LLM matching decisions."""
    list_display = ('id', 'action_icon', 'input_text_short', 'action', 'confidence', 'matched_to', 'was_applied', 'created_at')
    list_filter = ('action', 'confidence', 'was_applied', 'input_type', 'created_at')
    search_fields = ('input_text', 'matched_song_name', 'reasoning')
    ordering = ('-created_at',)
    date_hierarchy = 'created_at'
    readonly_fields = ('input_text', 'input_type', 'action', 'confidence', 'reasoning', 'matched_song', 'matched_song_name', 'was_applied', 'created_at')
    
    list_per_page = 50
    
    def action_icon(self, obj):
        icons = {
            'match': '🔗',
            'reject': '🗑️',
            'new': '🆕',
            'auto_merge': '✅',
            'auto_reject': '❌',
        }
        return icons.get(obj.action, '❓')
    action_icon.short_description = ''
    
    def input_text_short(self, obj):
        text = obj.input_text
        if len(text) > 50:
            return text[:50] + '...'
        return text
    input_text_short.short_description = 'Input'
    
    def matched_to(self, obj):
        if obj.matched_song_name:
            return obj.matched_song_name[:40] + '...' if len(obj.matched_song_name) > 40 else obj.matched_song_name
        return '-'
    matched_to.short_description = 'Matched To'
    
    def has_add_permission(self, request):
        return False  # Logs are auto-created
    
    def has_change_permission(self, request, obj=None):
        return False  # Read-only
