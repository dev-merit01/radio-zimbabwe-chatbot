"""
Management command to run LLM matching on unmatched votes.

Usage:
    # Dry run - see what would be matched without saving
    python manage.py llm_match --dry-run
    
    # Process up to 50 unmatched votes
    python manage.py llm_match --limit 50
    
    # Process all unmatched votes (careful with API costs!)
    python manage.py llm_match --limit 500
    
    # Process votes from a specific date
    python manage.py llm_match --date 2025-12-20
    
    # Only show high and medium confidence matches
    python manage.py llm_match --min-confidence medium
"""
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from datetime import datetime

from apps.voting.llm_matcher import (
    process_unmatched_votes,
    get_unmatched_tallies,
    get_verified_songs_list,
)


class Command(BaseCommand):
    help = 'Use LLM to match unmatched votes to verified songs'

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit',
            type=int,
            default=50,
            help='Maximum number of votes to process (default: 50)',
        )
        parser.add_argument(
            '--date',
            type=str,
            help='Filter by date (YYYY-MM-DD format)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show results without saving to database',
        )
        parser.add_argument(
            '--no-auto-link',
            action='store_true',
            help='Do not auto-link high confidence matches',
        )
        parser.add_argument(
            '--min-confidence',
            type=str,
            choices=['high', 'medium', 'low'],
            default='low',
            help='Minimum confidence level to display (default: low)',
        )

    def handle(self, *args, **options):
        limit = options['limit']
        dry_run = options['dry_run']
        auto_link = not options['no_auto_link']
        min_confidence = options['min_confidence']
        
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
