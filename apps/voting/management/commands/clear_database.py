"""
Management command to safely clear voting data while preserving user phone numbers.
Usage: python manage.py clear_database
"""
from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from apps.voting.models import RawVote, CleanedSong, RawSongTally, VerifiedArtist, LLMDecisionLog
from django.contrib.auth.models import User


class Command(BaseCommand):
    help = 'Clear all voting data tables except user phone numbers. Preserves user data.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--confirm',
            action='store_true',
            help='Confirm deletion without prompting',
        )

    def handle(self, *args, **options):
        confirm = options.get('confirm', False)

        # Display what will be deleted
        self.stdout.write(self.style.WARNING('\n⚠️  Data Deletion Summary:'))
        self.stdout.write(f'  • RawVote records: {RawVote.objects.count()}')
        self.stdout.write(f'  • CleanedSong records: {CleanedSong.objects.count()}')
        self.stdout.write(f'  • RawSongTally records: {RawSongTally.objects.count()}')
        self.stdout.write(f'  • VerifiedArtist records: {VerifiedArtist.objects.count()}')
        self.stdout.write(f'  • LLMDecisionLog records: {LLMDecisionLog.objects.count()}')
        self.stdout.write(self.style.WARNING('  • User table: PRESERVED (phone numbers kept)\n'))

        if not confirm:
            response = input('Are you sure you want to delete all these records? (yes/no): ')
            if response.lower() != 'yes':
                self.stdout.write(self.style.ERROR('Deletion cancelled.'))
                return

        try:
            # Delete in order of dependencies
            RawVote.objects.all().delete()
            self.stdout.write(self.style.SUCCESS('✓ Deleted RawVote records'))

            CleanedSong.objects.all().delete()
            self.stdout.write(self.style.SUCCESS('✓ Deleted CleanedSong records'))

            RawSongTally.objects.all().delete()
            self.stdout.write(self.style.SUCCESS('✓ Deleted RawSongTally records'))

            VerifiedArtist.objects.all().delete()
            self.stdout.write(self.style.SUCCESS('✓ Deleted VerifiedArtist records'))

            LLMDecisionLog.objects.all().delete()
            self.stdout.write(self.style.SUCCESS('✓ Deleted LLMDecisionLog records'))

            # Reset sequences (for PostgreSQL)
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT setval(pg_get_serial_sequence('voting_rawvote', 'id'), 1)
                    UNION ALL
                    SELECT setval(pg_get_serial_sequence('voting_cleanedsong', 'id'), 1)
                    UNION ALL
                    SELECT setval(pg_get_serial_sequence('voting_rawsongtally', 'id'), 1)
                    UNION ALL
                    SELECT setval(pg_get_serial_sequence('voting_verifiedartist', 'id'), 1)
                    UNION ALL
                    SELECT setval(pg_get_serial_sequence('voting_llmdecisionlog', 'id'), 1);
                """)
            self.stdout.write(self.style.SUCCESS('✓ Reset sequences'))

            self.stdout.write(self.style.SUCCESS('\n✅ All voting data cleared successfully!'))
            self.stdout.write(self.style.SUCCESS('   User phone numbers preserved.\n'))

        except Exception as e:
            raise CommandError(f'Error deleting data: {str(e)}')
