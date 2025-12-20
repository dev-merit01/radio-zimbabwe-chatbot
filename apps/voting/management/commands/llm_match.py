"""
Management command to run LLM matching on unmatched votes and pending songs.

Usage:
    # Process pending songs (songs awaiting review)
    python manage.py llm_match --pending
    
    # Dry run - see what would be matched without saving
    python manage.py llm_match --pending --dry-run
    
    # Process up to 100 pending songs
    python manage.py llm_match --pending --limit 100
    
    # Process unmatched raw vote tallies (original behavior)
    python manage.py llm_match --raw --limit 50
"""
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from datetime import datetime

from apps.voting.llm_matcher import (
    process_unmatched_votes,
    process_pending_songs,
    get_unmatched_tallies,
    get_pending_songs,
    get_verified_songs_list,
)


class Command(BaseCommand):
    help = 'Use LLM to match pending songs or unmatched votes to verified songs'

    def add_arguments(self, parser):
        parser.add_argument(
            '--pending',
            action='store_true',
            help='Process pending CleanedSong entries (recommended)',
        )
        parser.add_argument(
            '--raw',
            action='store_true',
            help='Process raw vote tallies without mappings',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=50,
            help='Maximum number of items to process (default: 50)',
        )
        parser.add_argument(
            '--date',
            type=str,
            help='Filter by date (YYYY-MM-DD format) - only for --raw mode',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show results without saving to database',
        )
        parser.add_argument(
            '--no-auto-merge',
            action='store_true',
            help='Do not auto-merge high confidence matches',
        )
        parser.add_argument(
            '--no-auto-reject',
            action='store_true',
            help='Do not auto-reject spam entries',
        )

    def handle(self, *args, **options):
        # Default to --pending if neither specified
        if not options['pending'] and not options['raw']:
            options['pending'] = True
        
        if options['pending']:
            self.handle_pending(options)
        else:
            self.handle_raw(options)
    
    def handle_pending(self, options):
        """Process pending CleanedSong entries."""
        limit = options['limit']
        dry_run = options['dry_run']
        auto_merge = not options['no_auto_merge']
        auto_reject = not options['no_auto_reject']
        
        # Check prerequisites
        songs = get_verified_songs_list()
        if not songs:
            self.stdout.write(self.style.ERROR(
                '❌ No verified songs in database. Add some songs first!'
            ))
            return
        
        self.stdout.write(f"📚 Found {len(songs)} verified songs in database")
        
        # Check pending songs
        pending = get_pending_songs(limit=limit)
        if not pending:
            self.stdout.write(self.style.SUCCESS('✅ No pending songs to review!'))
            return
        
        self.stdout.write(f"📋 Found {len(pending)} pending songs to review")
        
        if dry_run:
            self.stdout.write(self.style.WARNING('🔍 DRY RUN - no changes will be saved'))
        
        self.stdout.write('')
        self.stdout.write('🤖 Processing pending songs with LLM...')
        self.stdout.write('')
        
        # Process
        result = process_pending_songs(
            limit=limit,
            auto_merge=auto_merge,
            auto_reject=auto_reject,
            dry_run=dry_run,
        )
        
        if 'error' in result:
            self.stdout.write(self.style.ERROR(f"❌ {result['error']}"))
            return
        
        # Display results
        stats = result.get('stats', {})
        results = result.get('results', [])
        
        self.stdout.write('=' * 80)
        self.stdout.write('MATCHING RESULTS')
        self.stdout.write('=' * 80)
        
        for r in results:
            if r['action'] == 'match':
                style = self.style.SUCCESS
                icon = '✅'
                action_text = f"→ MATCH to: {r['matched_to']}"
            elif r['action'] == 'reject':
                style = self.style.ERROR
                icon = '🗑️'
                action_text = "→ REJECT (spam/invalid)"
            else:
                style = self.style.WARNING
                icon = '🆕'
                action_text = "→ NEW (keep as pending)"
            
            self.stdout.write('')
            self.stdout.write(f"{icon} \"{r['pending_name']}\"")
            self.stdout.write(style(f"   {action_text} ({r['confidence']})"))
            if r['reasoning']:
                self.stdout.write(f"   💭 {r['reasoning']}")
        
        # Summary
        self.stdout.write('')
        self.stdout.write('=' * 80)
        self.stdout.write('SUMMARY')
        self.stdout.write('=' * 80)
        self.stdout.write(f"Total processed: {stats.get('total_processed', 0)}")
        self.stdout.write(self.style.SUCCESS(f"Matched to verified: {stats.get('matched', 0)}"))
        self.stdout.write(self.style.ERROR(f"Rejected (spam): {stats.get('rejected', 0)}"))
        self.stdout.write(self.style.WARNING(f"New songs (keep pending): {stats.get('new_songs', 0)}"))
        
        if not dry_run:
            self.stdout.write('')
            self.stdout.write(self.style.SUCCESS(
                f"🔗 Auto-merged: {stats.get('auto_merged', 0)}"
            ))
            self.stdout.write(self.style.ERROR(
                f"🗑️ Auto-rejected: {stats.get('auto_rejected', 0)}"
            ))
        
        if stats.get('errors', 0) > 0:
            self.stdout.write(self.style.ERROR(f"Errors: {stats.get('errors', 0)}"))
    
    def handle_raw(self, options):
        """Process raw vote tallies (original behavior)."""
        limit = options['limit']
        dry_run = options['dry_run']
        auto_link = not options['no_auto_merge']
        min_confidence = 'low'  # Always show all results
        
        # Parse date if provided
        date = None
        if options['date']:
            try:
                date = datetime.strptime(options['date'], '%Y-%m-%d').date()
            except ValueError:
                raise CommandError('Invalid date format. Use YYYY-MM-DD')
        
        # Check prerequisites
        songs = get_verified_songs_list()
        if not songs:
            self.stdout.write(self.style.ERROR(
                '❌ No verified songs in database. Add some songs first!'
            ))
            return
        
        self.stdout.write(f"📚 Found {len(songs)} verified songs in database")
        
        # Check unmatched votes
        unmatched = get_unmatched_tallies(date=date, limit=limit)
        if not unmatched:
            self.stdout.write(self.style.SUCCESS('✅ No unmatched votes found!'))
            return
        
        self.stdout.write(f"📋 Found {len(unmatched)} unmatched vote tallies")
        
        if dry_run:
            self.stdout.write(self.style.WARNING('🔍 DRY RUN - no changes will be saved'))
        
        self.stdout.write('')
        self.stdout.write('🤖 Processing with LLM...')
        self.stdout.write('')
        
        # Process votes
        result = process_unmatched_votes(
            date=date,
            limit=limit,
            auto_link_high_confidence=auto_link,
            dry_run=dry_run,
        )
        
        if 'error' in result:
            self.stdout.write(self.style.ERROR(f"❌ {result['error']}"))
            return
        
        # Display results
        stats = result.get('stats', {})
        results = result.get('results', [])
        
        # Filter by confidence
        confidence_order = {'high': 3, 'medium': 2, 'low': 1, 'none': 0}
        min_conf_level = confidence_order.get(min_confidence, 0)
        
        self.stdout.write('=' * 80)
        self.stdout.write('MATCHING RESULTS')
        self.stdout.write('=' * 80)
        
        for r in results:
            conf_level = confidence_order.get(r['confidence'], 0)
            if conf_level < min_conf_level:
                continue
            
            # Color based on confidence
            if r['confidence'] == 'high':
                style = self.style.SUCCESS
                icon = '✅'
            elif r['confidence'] == 'medium':
                style = self.style.WARNING
                icon = '🟡'
            elif r['confidence'] == 'low':
                style = self.style.NOTICE if hasattr(self.style, 'NOTICE') else str
                icon = '🟠'
            else:
                style = self.style.ERROR
                icon = '❌'
            
            self.stdout.write('')
            self.stdout.write(f"{icon} \"{r['raw_input']}\"")
            
            if r['matched_song']:
                self.stdout.write(f"   → {r['matched_song']} ({r['confidence']})")
            else:
                self.stdout.write(f"   → No match ({r['confidence']})")
            
            if r['reasoning']:
                self.stdout.write(f"   💭 {r['reasoning']}")
            
            if r.get('auto_linked') and not dry_run:
                self.stdout.write(style('   🔗 Auto-linked!'))
        
        # Summary
        self.stdout.write('')
        self.stdout.write('=' * 80)
        self.stdout.write('SUMMARY')
        self.stdout.write('=' * 80)
        self.stdout.write(f"Total processed: {stats.get('total_processed', 0)}")
        self.stdout.write(self.style.SUCCESS(f"High confidence: {stats.get('high_confidence', 0)}"))
        self.stdout.write(self.style.WARNING(f"Medium confidence: {stats.get('medium_confidence', 0)}"))
        self.stdout.write(f"Low confidence: {stats.get('low_confidence', 0)}")
        self.stdout.write(f"No match: {stats.get('no_match', 0)}")
        
        if not dry_run and auto_link:
            self.stdout.write('')
            self.stdout.write(self.style.SUCCESS(
                f"🔗 Auto-linked {stats.get('auto_linked', 0)} high-confidence matches"
            ))
        
        if stats.get('errors', 0) > 0:
            self.stdout.write(self.style.ERROR(f"Errors: {stats.get('errors', 0)}"))
        
        self.stdout.write('')
