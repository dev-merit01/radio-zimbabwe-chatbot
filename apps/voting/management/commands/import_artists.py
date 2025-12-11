from django.core.management.base import BaseCommand
from apps.voting.models import VerifiedArtist


# Artist data with genre classification
ARTISTS_DATA = [
    # Zimdancehall / Urban Grooves
    ('Winky D', 'zimdancehall', ['Vigilance', 'Dancehall Doctor']),
    ('Tocky Vibes', 'zimdancehall', []),
    ('Killer T', 'zimdancehall', []),
    ('Freeman', 'zimdancehall', ['Freeman HKD']),
    ('Takura', 'zimdancehall', []),
    ('ExQ', 'zimdancehall', ['Mr Putiti']),
    ('Nutty O', 'zimdancehall', []),
    ('Holy Ten', 'hiphop', []),
    ('Ti Gonzi', 'hiphop', []),
    ('Voltz JT', 'zimdancehall', []),
    ('Enzo Ishall', 'zimdancehall', []),
    ('Jah Signal', 'zimdancehall', []),
    ('Soul Jah Love', 'zimdancehall', ['Chibaba', 'Sauro Musinamato']),
    ('Seh Calaz', 'zimdancehall', []),
    ('Dobba Don', 'zimdancehall', []),
    ('Pumacol', 'zimdancehall', []),
    ('Silent Killer', 'zimdancehall', []),
    ('Hwindi President', 'zimdancehall', []),
    ('Djembe Monk', 'zimdancehall', []),
    ('Poptain', 'zimdancehall', []),
    ('Shinsoman', 'zimdancehall', []),
    ('Ricky Fire', 'zimdancehall', []),
    ('Dhadza D', 'zimdancehall', []),
    ('Blot', 'zimdancehall', ['Grenade']),
    ('Guspy Warrior', 'zimdancehall', []),
    ('Caption', 'zimdancehall', []),
    ('Boom Beto', 'zimdancehall', []),
    ('Bagga Don', 'zimdancehall', ['Bagga']),
    ('Larry Lovah', 'zimdancehall', []),
    
    # Sungura / Jiti
    ('Alick Macheso', 'sungura', ['Macheso', 'Macheso and Orchestra Mberikwazvo']),
    ('Suluman Chimbetu', 'sungura', ['Sulu']),
    ('Simon Chimbetu', 'sungura', []),
    ('Dendera Kings', 'sungura', []),
    ('Leonard Dembo', 'sungura', []),
    ('System Tazvida', 'sungura', []),
    ('Somandla Ndebele', 'sungura', []),
    ('Tongai Moyo', 'sungura', []),
    ('Nicholas Zakaria', 'sungura', ['Zakaria']),
    ('Leonard Zhakata', 'sungura', []),
    ('Mark Ngwazi', 'sungura', []),
    ('Tryson Chimbetu', 'sungura', []),
    ('Peter Moyo', 'sungura', []),
    ('Betserai', 'sungura', []),
    ('Mukoma Panga', 'sungura', []),
    ('Blessing Gupa', 'sungura', ['Blessing Gupa Marezva']),
    
    # Chimurenga / Afro-Jazz / Legends
    ('Oliver Mtukudzi', 'chimurenga', ['Tuku', 'Dr Tuku']),
    ('Thomas Mapfumo', 'chimurenga', ['Mapfumo', 'Mukanya']),
    ('Stella Chiweshe', 'chimurenga', []),
    ('Chiwoniso Maraire', 'chimurenga', ['Chi']),
    ('Hope Masike', 'afropop', []),
    ('Mokoomba', 'afropop', []),
    ('Mbira DzeBongo', 'chimurenga', []),
    ('Zimpraise', 'gospel', []),
    ('Mechanic Manyeruke', 'gospel', []),
    ('Sabastian Magacha', 'gospel', []),
    
    # Contemporary / Afropop / R&B
    ('Jah Prayzah', 'afropop', ['Musoja', 'JP']),
    ('Ammara Brown', 'afropop', []),
    ('Gemma Griffiths', 'afropop', []),
    ('Shashl', 'afropop', []),
    ('Sha Sha', 'afropop', []),
    ('Cindy Munyavi', 'afropop', ['Cindy']),
    ('Hillzy', 'hiphop', []),
    ('Novuyo Seagirl', 'rnb', []),
    ('Feli Nandi', 'afropop', []),
    ('Janet Manyowa', 'gospel', []),
    ('Tammy Moyo', 'afropop', ['Tamy Moyo']),
    ('Kae Chaps', 'afropop', []),
    ('Zirree', 'afropop', []),
    ('Simba Tagz', 'hiphop', []),
    ('Asaph', 'hiphop', []),
    ('Tehn Diamond', 'hiphop', []),
    ('Roki', 'afropop', []),
    ('Buffalo Souljah', 'zimdancehall', []),
    ('Jaydee Taurus', 'afropop', []),
    ('Noluntu J', 'afropop', []),
    ('Ishan', 'afropop', []),
    ('Xtra Large', 'sungura', ['Xtra Large Maroja']),
    ('Crooger', 'hiphop', []),
    ('SaintFloew', 'hiphop', []),
    ('Uncle Epatan', 'hiphop', []),
    ('Andrea The Vocalist', 'afropop', []),
    
    # Gospel
    ('Minister Michael Mahendere', 'gospel', ['Michael Mahendere']),
    ('Mathias Mhere', 'gospel', []),
    ('Blessing Shumba', 'gospel', []),
    ('Olinda Zimuto', 'gospel', ['Olinda']),
    ('Charles Charamba', 'gospel', []),
    ('Fungisai Zvakavapano', 'gospel', ['Fungisai']),
    ('Prudence Katomeni', 'gospel', []),
    ('Bethany Pasinawako', 'gospel', []),
    ('Mkhululi Bhebhe', 'gospel', []),
    
    # Hip Hop / Urban
    ('Stunner', 'hiphop', []),
    ('Platinum Prince', 'hiphop', []),
    ('MC Chita', 'hiphop', []),
    ('DJ Tamuka', 'other', []),
    ('GZE', 'hiphop', []),
    ('Nobuntu', 'afropop', []),
]



class Command(BaseCommand):
    help = 'Import verified Zimbabwean artists into the database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear existing artists before importing',
        )

    def handle(self, *args, **options):
        if options['clear']:
            deleted_count = VerifiedArtist.objects.all().delete()[0]
            self.stdout.write(f'Cleared {deleted_count} existing artists')

        created = 0
        updated = 0
        
        for name, genre, aliases in ARTISTS_DATA:
            aliases_text = '\n'.join(aliases) if aliases else ''
            
            artist, was_created = VerifiedArtist.objects.update_or_create(
                name=name,
                defaults={
                    'genre': genre,
                    'aliases': aliases_text,
                    'is_active': True,
                }
            )
            
            if was_created:
                created += 1
            else:
                updated += 1

        self.stdout.write(
            self.style.SUCCESS(
                f'✅ Done! Created {created} new artists, updated {updated} existing.'
            )
        )
        self.stdout.write(
            f'Total verified artists: {VerifiedArtist.objects.count()}'
        )
