"""Run via manage.py shell on an empty development database for browser checks."""
from django.conf import settings
from django.contrib.auth import get_user_model
from apps.voting.models import CleanedSong, VoteJob
from apps.voting.services import VotingService
from apps.voting.worker import run_one
from apps.voting.pipeline import review_song

if not settings.DEBUG or get_user_model().objects.exists():
    raise RuntimeError('Browser fixtures require DEBUG and a database with no existing accounts.')
user = get_user_model().objects.create_superuser('ui-operator', password='local-browser-test-only')
for i,(artist,title) in enumerate([('Example Artist','Morning Light'),('Studio Band','Together'),('River Voices','Home'),('Sunrise','New Day')]):
    for j in range(3+i):
        VotingService('telegram',f'demo-{i}-{j}').handle_incoming_text(f'{artist} - {title}')
while run_one(VoteJob):
    pass
for song in CleanedSong.objects.all()[:3]:
    review_song('radio_zimbabwe',user,song.id,'verified')
