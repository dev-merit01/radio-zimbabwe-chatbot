"""
Management command to load pre-known songs into the database.

Usage:
    python manage.py load_songs

Songs are loaded as CleanedSong entries with status='verified'.
This improves vote matching accuracy.
"""
from django.core.management.base import BaseCommand
from apps.voting.models import CleanedSong


# ============================================================
# ADD YOUR SONGS HERE
# Format: ('Artist Name', 'Song Title')
# ============================================================
KNOWN_SONGS = [
    # Winky D
    ('Winky D', 'Kasong Kejecha'),
    ('Winky D', 'Ibotso'),
    ('Winky D', 'Dzika Ngirozi'),
    ('Winky D', 'Mugarden'),
    ('Winky D', 'Ijipita'),
    ('Winky D', 'Finhu Finhu'),
    ('Winky D', 'Disappear'),
    ('Winky D', 'Gafa President'),
    
    # Jah Prayzah
    ('Jah Prayzah', 'Mukwasha'),
    ('Jah Prayzah', 'Mdhara Vachauya'),
    ('Jah Prayzah', 'Hokoyo'),
    ('Jah Prayzah', 'Dzamutsana'),
    ('Jah Prayzah', 'Goto'),
    ('Jah Prayzah', 'Kutonga Kwaro'),
    ('Jah Prayzah', 'Munyaradzi'),
    
    # Holy Ten
    ('Holy Ten', 'Ndini Ndega'),
    ('Holy Ten', 'Ndakakutadzirei'),
    ('Holy Ten', 'Chigayo'),
    ('Holy Ten', 'Mabhawa'),
    ('Holy Ten', 'Simuka'),
    
    # Killer T
    ('Killer T', 'Takangodii'),
    ('Killer T', 'Zuva Guru'),
    ('Killer T', 'Ngoma Kurira'),
    
    # Freeman
    ('Freeman', 'Joina City'),
    ('Freeman', 'Handina Mhere'),
    
    # ExQ
    ('ExQ', 'Tsvigiri'),
    ('ExQ', 'Bhachura'),
    
    # Seh Calaz
    ('Seh Calaz', 'Mwana Angu'),
    ('Seh Calaz', 'Wenera'),
    
    # Tocky Vibes
    ('Tocky Vibes', 'Pinda Moto'),
    ('Tocky Vibes', 'Mhai'),
    
    # Ti Gonzi
    ('Ti Gonzi', 'Ndiwe Bae'),
    ('Ti Gonzi', 'Handichazive'),
    
    # Saintfloew
    ('Saintfloew', 'Ndipe Simba'),
    ('Saintfloew', 'Amen'),
    
    # Shinsoman
    ('Shinsoman', 'Tenda'),
    ('Shinsoman', 'Ndofamba Ndega'),
    
    # Oliver Mtukudzi
    ('Oliver Mtukudzi', 'Neria'),
    ('Oliver Mtukudzi', 'Todii'),
    ('Oliver Mtukudzi', 'Hear Me Lord'),
    
    # Alick Macheso
    ('Alick Macheso', 'Amai'),
    ('Alick Macheso', 'Mundikumbuke'),
    
    # Suluman Chimbetu
    ('Suluman Chimbetu', 'Changamire'),
    ('Suluman Chimbetu', 'Wandirasa'),
    
    # Soul Jah Love (Legend)
    ('Soul Jah Love', 'Pamamonya Ipapo'),
    ('Soul Jah Love', 'Gum Kum'),
    
    # ============================================================
    # Radio Zimbabwe Playlist (User Added)
    # ============================================================
    ('Blackdiva', 'Thilo Lilo'),
    ('Calvin Dowe', 'Sorry Sorry'),
    ('Chief Hwenje', 'Shumba Murambwi'),
    ('Chillmaster', 'Judas Iscariot'),
    ('Donator Calvins', 'Door Ratovharwa'),
    ('Dorcas Moyo', 'Mugeri Tsvatu'),
    ('DT Bio Mudhimba', 'Twabeyi Shuwa'),
    ('Fab G', 'Imali Yesigweja'),
    ('Freeman', 'Muchandinzwawo'),
    ('Jah Prayzah', 'Ruzhowa'),
    ('Jaycee ft Zinjaziyamluma', 'Ngixolele'),
    ('Killer T', 'Bhiya'),
    ('King Adiza', "Hamb'uyogeza"),
    ('King David', 'Chigaba Chinorira'),
    ('Leonard Zhakata', 'Tanyaradzwa'),
    ('Ma9nine ft Abigail Mabuza', 'Ngeke'),
    ('Mai Guvamombe', 'Wedzerai Mazuvha'),
    ('Michael Mahendere', 'Messiah'),
    ('Mkoma Panga', 'Zvikandwa'),
    ('Mlambos', 'Soft Life'),
    ('Nutty O', 'Too Much'),
    ('Nisha Ts', 'Admire Kadembo'),
    ('Obert Chari', 'Vatezvara'),
    ('Oriyano', 'Zampele'),
    ('Paddington Chiwashira', 'Manhanga Matete'),
    ('Paradzai Mesi', 'Takuziva'),
    ('Peter Moyo', 'Usandifendere'),
    ('Prince Chigwida', 'Ndiringe Mambo'),
    ('Psalmist Lamondy Dube', 'Achandipindura'),
    ('Roe Makawa', 'What Happened'),
    ('Simon Mutambi', 'Chimbomira'),
    ('Somandla Ndebele', 'Chembedzanai'),
    ('Sulumani', 'Timba'),
    ('Tamy Moyo ft Holy Ten & Kelvin Mangena', 'Bvunza'),
    ('Trymore Bande', 'Ngatigare Tichinamata'),
    ('Verutendo', 'Mudhindo Joshua'),
    ('Winky D', 'Drink Up'),
]


class Command(BaseCommand):
    help = 'Load pre-known songs into the database for better vote matching'

    def add_arguments(self, parser):
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear all existing CleanedSong entries before loading',
        )

    def handle(self, *args, **options):
        if options['clear']:
            count = CleanedSong.objects.count()
            CleanedSong.objects.all().delete()
            self.stdout.write(self.style.WARNING(f'Cleared {count} existing songs'))

        created_count = 0
        updated_count = 0
        skipped_count = 0

        for artist, title in KNOWN_SONGS:
            canonical_name = f"{artist} - {title}"
            
            # Try to find existing song (case-insensitive) - check both artist/title AND canonical_name
            existing = CleanedSong.objects.filter(
                canonical_name__iexact=canonical_name
            ).first()
            
            if not existing:
                existing = CleanedSong.objects.filter(
                    artist__iexact=artist,
                    title__iexact=title
                ).first()
            
            if existing:
                if existing.status != 'verified':
                    existing.status = 'verified'
                    existing.save()
                    updated_count += 1
                    self.stdout.write(f"✓ Verified: {canonical_name}")
                else:
                    skipped_count += 1
                    self.stdout.write(f"• Exists: {canonical_name}")
            else:
                CleanedSong.objects.create(
                    artist=artist,
                    title=title,
                    canonical_name=canonical_name,
                    status='verified'
                )
                created_count += 1
                self.stdout.write(self.style.SUCCESS(f"+ Added: {canonical_name}"))

        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Summary:'))
        self.stdout.write(f'  Created: {created_count}')
        self.stdout.write(f'  Updated: {updated_count}')
        self.stdout.write(f'  Skipped: {skipped_count}')
        self.stdout.write(f'  Total in DB: {CleanedSong.objects.count()}')
