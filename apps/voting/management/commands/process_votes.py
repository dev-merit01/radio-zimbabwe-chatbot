"""
Management command to process and clean raw votes.

Usage:
    python manage.py process_votes           # Process today's votes
    python manage.py process_votes --date 2025-12-05
    python manage.py process_votes --show-pending
    python manage.py process_votes --show-suggestions
"""
from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import datetime

from apps.voting.cleaning import CleaningService


class Command(BaseCommand):
    help = 'Process raw votes and create cleaned song entries'

    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            type=str,
            help='Date to process (YYYY-MM-DD format). Default: today',
        )
        parser.add_argument(
            '--show-pending',
            action='store_true',
            help='Show songs pending review',
        )
        parser.add_argument(
            '--show-suggestions',
            action='store_true',
            help='Show merge suggestions for similar songs',
        )

    def handle(self, *args, **options):
        service = CleaningService()
        
        if options['show_pending']:
            self._show_pending(service)
            return
        
        if options['show_suggestions']:
            self._show_suggestions(service)
            return
        
        # Process votes
        if options['date']:
            date = datetime.strptime(options['date'], '%Y-%m-%d').date()
        else:
            date = timezone.localdate()
        
        self.stdout.write(f"\n🔄 Processing votes for {date}...\n")
        
        result = service.process_new_votes(date)
        
        self.stdout.write(self.style.SUCCESS(
            f"\n✅ Done!\n"
            f"   - New songs created: {result['new']}\n"
            f"   - Auto-merged: {result['auto_merged']}\n"
        ))
        
        # Show pending count
        pending = service.get_pending_review()
        if pending:
            self.stdout.write(self.style.WARNING(
                f"\n⏳ {len(pending)} songs pending review. "
                f"Run with --show-pending to see them.\n"
            ))

    def _show_pending(self, service):
        pending = service.get_pending_review()
        
        if not pending:
            self.stdout.write(self.style.SUCCESS("\n✅ No songs pending review!\n"))
            return
        
        self.stdout.write(f"\n⏳ Songs Pending Review ({len(pending)}):\n")
        self.stdout.write("-" * 60 + "\n")
        
        for i, song in enumerate(pending, 1):
            # Get vote count
            from apps.voting.models import MatchKeyMapping
            mappings = MatchKeyMapping.objects.filter(cleaned_song=song)
            total_votes = sum(m.vote_count for m in mappings)
            
            self.stdout.write(
                f"{i:3}. {song.canonical_name}\n"
                f"     Votes: {total_votes} | ID: {song.id}\n"
            )
        
        self.stdout.write("\n")

    def _show_suggestions(self, service):
        suggestions = service.get_merge_suggestions()
        
        if not suggestions:
            self.stdout.write(self.style.SUCCESS("\n✅ No merge suggestions!\n"))
            return
        
        self.stdout.write(f"\n🔀 Merge Suggestions ({len(suggestions)}):\n")
        self.stdout.write("-" * 70 + "\n")
        
        for i, sug in enumerate(suggestions, 1):
            self.stdout.write(
                f"{i}. Similarity: {sug['similarity']:.0%}\n"
                f"   Song A: {sug['song1'].canonical_name} (ID: {sug['song1'].id})\n"
                f"   Song B: {sug['song2'].canonical_name} (ID: {sug['song2'].id})\n"
                f"   Artist sim: {sug['artist_similarity']:.0%}, Title sim: {sug['title_similarity']:.0%}\n\n"
            )
