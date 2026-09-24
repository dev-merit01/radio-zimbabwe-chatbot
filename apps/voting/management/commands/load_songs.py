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
    ("Winky D", "Kasong Kejecha"),
    ("Winky D", "Ibotso"),
    ("Winky D", "Dzika Ngirozi"),
    ("Winky D", "Mugarden"),
    ("Winky D", "Ijipita"),
    ("Winky D", "Finhu Finhu"),
    ("Winky D", "Disappear"),
    ("Winky D", "Gafa President"),
    # Jah Prayzah
    ("Jah Prayzah", "Mukwasha"),
    ("Jah Prayzah", "Mdhara Vachauya"),
    ("Jah Prayzah", "Hokoyo"),
    ("Jah Prayzah", "Dzamutsana"),
    ("Jah Prayzah", "Goto"),
    ("Jah Prayzah", "Kutonga Kwaro"),
    ("Jah Prayzah", "Munyaradzi"),
    # Holy Ten
    ("Holy Ten", "Ndini Ndega"),
    ("Holy Ten", "Ndakakutadzirei"),
    ("Holy Ten", "Chigayo"),
    ("Holy Ten", "Mabhawa"),
    ("Holy Ten", "Simuka"),
    # Killer T
    ("Killer T", "Takangodii"),
    ("Killer T", "Zuva Guru"),
    ("Killer T", "Ngoma Kurira"),
    # Freeman
    ("Freeman", "Joina City"),
    ("Freeman", "Handina Mhere"),
    # ExQ
    ("ExQ", "Tsvigiri"),
    ("ExQ", "Bhachura"),
    # Seh Calaz
    ("Seh Calaz", "Mwana Angu"),
    ("Seh Calaz", "Wenera"),
    # Tocky Vibes
    ("Tocky Vibes", "Pinda Moto"),
    ("Tocky Vibes", "Mhai"),
    # Ti Gonzi
    ("Ti Gonzi", "Ndiwe Bae"),
    ("Ti Gonzi", "Handichazive"),
    # Saintfloew
    ("Saintfloew", "Ndipe Simba"),
    ("Saintfloew", "Amen"),
    # Shinsoman
    ("Shinsoman", "Tenda"),
    ("Shinsoman", "Ndofamba Ndega"),
    # Oliver Mtukudzi
    ("Oliver Mtukudzi", "Neria"),
    ("Oliver Mtukudzi", "Todii"),
    ("Oliver Mtukudzi", "Hear Me Lord"),
    # Alick Macheso
    ("Alick Macheso", "Amai"),
    ("Alick Macheso", "Mundikumbuke"),
    # Suluman Chimbetu
    ("Suluman Chimbetu", "Changamire"),
    ("Suluman Chimbetu", "Wandirasa"),
    # Soul Jah Love (Legend)
    ("Soul Jah Love", "Pamamonya Ipapo"),
    ("Soul Jah Love", "Gum Kum"),
    # ============================================================
    # Radio Zimbabwe Playlist (User Added)
    # ============================================================
    ("Blackdiva", "Thilo Lilo"),
    ("Calvin Dowe", "Sorry Sorry"),
    ("Chief Hwenje", "Shumba Murambwi"),
    ("Chillmaster", "Judas Iscariot"),
    ("Donator Calvins", "Door Ratovharwa"),
    ("Dorcas Moyo", "Mugeri Tsvatu"),
    ("DT Bio Mudhimba", "Twabeyi Shuwa"),
    ("Fab G", "Imali Yesigweja"),
    ("Freeman", "Muchandinzwawo"),
    ("Jah Prayzah", "Ruzhowa"),
    ("Jaycee ft Zinjaziyamluma", "Ngixolele"),
    ("Killer T", "Bhiya"),
    ("King Adiza", "Hamb'uyogeza"),
    ("King David", "Chigaba Chinorira"),
    ("Leonard Zhakata", "Tanyaradzwa"),
    ("Ma9nine ft Abigail Mabuza", "Ngeke"),
    ("Mai Guvamombe", "Wedzerai Mazuvha"),
    ("Michael Mahendere", "Messiah"),
    ("Mkoma Panga", "Zvikandwa"),
    ("Mlambos", "Soft Life"),
    ("Nutty O", "Too Much"),
    ("Nisha Ts", "Admire Kadembo"),
    ("Obert Chari", "Vatezvara"),
    ("Oriyano", "Zampele"),
    ("Paddington Chiwashira", "Manhanga Matete"),
    ("Paradzai Mesi", "Takuziva"),
    ("Peter Moyo", "Usandifendere"),
    ("Prince Chigwida", "Ndiringe Mambo"),
    ("Psalmist Lamondy Dube", "Achandipindura"),
    ("Roe Makawa", "What Happened"),
    ("Simon Mutambi", "Chimbomira"),
    ("Somandla Ndebele", "Chembedzanai"),
    ("Sulumani", "Timba"),
    ("Tamy Moyo ft Holy Ten & Kelvin Mangena", "Bvunza"),
    ("Trymore Bande", "Ngatigare Tichinamata"),
    ("Verutendo", "Mudhindo Joshua"),
    ("Winky D", "Drink Up"),
]


class Command(BaseCommand):
    help = (
        "Seed the catalogue for an explicit station; existing decisions are preserved."
    )

    def add_arguments(self, parser):
        from apps.accounts.models import Station

        parser.add_argument("--station", required=True, choices=Station.values)

    def handle(self, *args, **options):
        from django.db import transaction
        from apps.voting.pipeline import lock_station
        from apps.voting.models import ReviewAudit

        station = options["station"]
        count = 0
        with transaction.atomic():
            lock_station(station)
            for artist, title in KNOWN_SONGS:
                name = f"{artist} - {title}"
                if not CleanedSong.objects.filter(
                    station=station, canonical_name__iexact=name
                ).exists():
                    CleanedSong.objects.create(
                        station=station,
                        artist=artist,
                        title=title,
                        canonical_name=name,
                        status="pending",
                    )
                    count += 1
            ReviewAudit.objects.create(
                station=station, action="seed_catalogue", details={"created": count}
            )
        self.stdout.write(
            f"Added {count} songs for review. Existing songs were unchanged."
        )
