"""Run via manage.py shell on an empty development database for browser checks."""
from django.conf import settings
from django.contrib.auth import get_user_model
from apps.voting.models import CleanedSong, VoteJob
from apps.voting.services import VotingService
from apps.voting.worker import run_one
from apps.voting.pipeline import review_song

if not settings.DEBUG or get_user_model().objects.exists():
    raise RuntimeError('Browser fixtures require DEBUG and a database with no existing accounts.')
# Legacy browser fixtures explicitly exercise code-free compatibility mode.
from apps.voting.models import StationVotingConfig
StationVotingConfig.objects.filter(station__in=("radio_zimbabwe", "national_fm")).update(require_codes=False)
# Chart-coded stations must be exercised through a real, open programme.
# Keep this isolated to CI data, never alter production voting windows.
from apps.voting.models import ChartShow
national_test_show = ChartShow.objects.get(station="national_fm", code="N20")
national_test_show.is_enabled = True
national_test_show.schedule_mode = "manual"
national_test_show.manual_open = True
national_test_show.eligibility_enforced = False
national_test_show.save(update_fields=[
    "is_enabled", "schedule_mode", "manual_open", "eligibility_enforced",
])
user = get_user_model().objects.create_superuser('ui-operator', password='local-browser-test-only')
for i,(artist,title) in enumerate([('Example Artist','Morning Light'),('Studio Band','Together'),('River Voices','Home'),('Sunrise','New Day')]):
    for j in range(3+i):
        VotingService('telegram',f'demo-{i}-{j}').handle_incoming_text(f'{artist} - {title}')
while run_one(VoteJob):
    pass
for song in CleanedSong.objects.all()[:3]:
    review_song('radio_zimbabwe',user,song.id,'verified')
from apps.accounts.models import AccountProfile
staff = get_user_model().objects.create_user('ui-staff', password='local-staff-test-only')
AccountProfile.objects.create(user=staff, station='radio_zimbabwe')
