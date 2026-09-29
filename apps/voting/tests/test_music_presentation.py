from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from apps.voting.presentation import music_name
from apps.voting.models import CleanedSong, RawVote, VoteJob
from apps.voting.services import VotingService
from apps.voting.worker import run_one


def test_music_names_use_consistent_case_and_spacing():
    for raw, expected in [("  WINKY   d ", "Winky D"), ("jah prayzah", "Jah Prayzah"),
                          ("DON'T STOP", "Don't Stop"), ("dj young MC", "DJ Young MC"),
                          ("r&b LOVE", "R&B Love"), ("long-WAY home", "Long-Way Home")]:
        assert music_name(raw) == expected


@override_settings(SECURE_SSL_REDIRECT=False, AUTO_AI_MATCH=False)
class MusicPresentationTests(TestCase):
    def test_new_votes_and_existing_catalogue_display_are_formatted(self):
        VotingService('telegram', 'case-listener').handle_incoming_text('WINKY D - IJIPITA')
        raw = RawVote.objects.get()
        self.assertEqual(raw.raw_input, 'WINKY D - IJIPITA')
        self.assertTrue(run_one(VoteJob))
        song = CleanedSong.objects.get()
        self.assertEqual((song.artist, song.title), ('Winky D', 'Ijipita'))
        # Existing records are formatted at the presentation boundary without migrating identity.
        CleanedSong.objects.filter(pk=song.pk).update(artist='WINKY D', title='IJIPITA')
        admin = get_user_model().objects.create_superuser('format-admin')
        self.client.force_login(admin)
        data = self.client.get('/api/workspace/songs?status=all').json()['items'][0]
        self.assertEqual((data['artist'], data['title']), ('Winky D', 'Ijipita'))
        response = self.client.post(f'/api/workspace/songs/{song.pk}/review',
                                   {'action':'edit', 'artist':'jah prayzah', 'title':'  NEW  SONG '}, content_type='application/json')
        self.assertEqual(response.status_code, 200)
        song.refresh_from_db()
        self.assertEqual(song.canonical_name, 'Jah Prayzah - New Song')
