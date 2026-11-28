from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from apps.accounts.models import StaffAccess
from apps.voting.models import (
    ChartShow,
    CleanedSong,
    InboundEvent,
    MatchKeyMapping,
    RawVote,
    StationVotingConfig,
    SuspectedSpamVote,
    UnassignedVote,
    User,
)


@override_settings(SECURE_SSL_REDIRECT=False, ALLOW_REPEAT_SONG=False)
class DirectorReportingTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.actor = get_user_model().objects.create_user("director-test")
        cls.access = StaffAccess.objects.create(
            user=cls.actor, role="director", privileges=["incoming"]
        )
        cls.show = ChartShow.objects.get(station="radio_zimbabwe", code="G20")
        cls.show.eligibility_enforced = False
        cls.show.save()
        cls.other = ChartShow.objects.get(station="national_fm", code="N20")
        cls.listener = User.objects.create(
            station=cls.show.station, channel="sms", user_ref="+263777000000"
        )
        song = CleanedSong.objects.create(
            station=cls.show.station,
            artist="Test",
            title="Music",
            canonical_name="Test - Music",
            status="verified",
        )
        MatchKeyMapping.objects.create(
            station=cls.show.station, match_key="test::music", cleaned_song=song
        )
        cls.home = reverse("accounts:staff_home")

    def setUp(self):
        self.client.force_login(self.actor)

    def vote(self, **changes):
        values = dict(
            station=self.show.station,
            chart_show=self.show,
            user=self.listener,
            ai_status="approved",
            artist_raw="Test",
            song_raw="Music",
            match_key="test::music",
            vote_date=timezone.localdate(),
        )
        return RawVote.objects.create(**(values | changes))

    def test_feed_deduplicates_receipts_and_matches_chart_rules(self):
        common = dict(
            station=self.show.station,
            chart_show=self.show,
            provider="sms",
            sender=self.listener.user_ref,
            text="private message",
        )
        linked = InboundEvent.objects.create(**common, message_id="1", state="done")
        self.vote(receipt=linked)
        self.vote()
        self.vote(ai_status="review", song_raw="+263777000000")
        InboundEvent.objects.create(
            **common,
            message_id="2",
            state="done",
            reply="✅ Submission received! Song verification",
        )
        InboundEvent.objects.create(**common, message_id="3")
        InboundEvent.objects.create(
            **common, message_id="4", state="done", reply="Limit reached"
        )
        SuspectedSpamVote.objects.create(
            **common, message_id="5", received_at=timezone.now()
        )
        UnassignedVote.objects.create(
            station=self.show.station,
            intended_show=self.show,
            sender="private",
            message_id="6",
            reason="show_closed",
            received_at=timezone.now(),
        )
        url = self.home + f"?show={self.show.pk}&tab=incoming"
        page = self.client.get(url)
        self.assertEqual(
            (
                page.context["received"],
                page.context["counted"],
                page.context["needs_review"],
            ),
            (7, 1, 3),
        )
        self.assertEqual(page.context["feed"].paginator.count, 7)
        self.assertContains(page, "Held for routing")
        self.assertNotContains(page, self.listener.user_ref)
        self.assertNotContains(page, "private message")
        filtered = self.client.get(url + "&counted=1")
        self.assertEqual(filtered.context["feed"].paginator.count, 1)
        self.assertContains(filtered, "Music")
        StationVotingConfig.objects.update_or_create(
            station=self.show.station, defaults={"repeat_song_policy": "allow"}
        )
        self.assertEqual(self.client.get(url).context["counted"], 2)
        self.show.eligibility_enforced = True
        self.show.save()
        self.assertEqual(self.client.get(url).context["counted"], 0)

    def test_dates_scope_and_revocation(self):
        self.vote(vote_date=timezone.localdate() - timedelta(days=2))
        self.vote(
            station=self.other.station,
            chart_show=self.other,
            song_raw="outside-assignment",
        )
        self.access.selected_shows_only = True
        self.access.save()
        self.access.shows.add(self.show)
        page = self.client.get(self.home)
        self.assertEqual(page.context["received"], 0)
        page = self.client.get(self.home + "?period=week&tab=incoming")
        self.assertEqual(page.context["received"], 1)
        self.assertEqual(page.context["trend"][4][1], 1)
        self.assertNotContains(page, "outside-assignment")
        self.assertEqual(
            self.client.get(self.home + f"?show={self.other.pk}").status_code, 404
        )
        self.access.privileges = []
        self.access.save()
        self.assertEqual(self.client.get(self.home + "?tab=incoming").status_code, 403)
        self.assertEqual(self.client.get(self.home + "?counted=1").status_code, 403)
        self.assertNotContains(
            self.client.get(self.home + f"?show={self.show.pk}"),
            "Latest incoming votes",
        )
