"""The live-routing audit cannot change a chart programme or any votes."""
from io import StringIO

from django.core.management import call_command
from django.test import TestCase

from apps.voting.models import ChartShow, StationVotingConfig, UnassignedVote


class RoutingAuditTests(TestCase):
    def test_reports_code_state_and_unassigned_count_without_mutation(self):
        show = ChartShow.objects.get(station="radio_zimbabwe", name="Gospel 20")
        show.is_enabled = False
        show.save(update_fields=["is_enabled"])
        config = StationVotingConfig.objects.get(station="radio_zimbabwe")
        config.require_codes = True
        config.save(update_fields=["require_codes"])
        UnassignedVote.objects.create(
            station="radio_zimbabwe", provider="bird", message_id="held-test",
            sender="test-listener", text="G20 Example - Song",
            reason="show_closed", received_at=show.created_at,
        )
        out = StringIO()
        call_command("audit_chart_routing", stdout=out)
        report = out.getvalue()
        self.assertIn("routing_audit station=radio_zimbabwe codes_required=1", report)
        self.assertIn("G20:CLOSED(enabled=0", report)
        self.assertIn("routing_holds station=radio_zimbabwe pending_by_reason=show_closed:1", report)
        self.assertNotIn("test-listener", report)
        self.assertNotIn("Example - Song", report)
        self.assertFalse(ChartShow.objects.get(pk=show.pk).is_enabled)
        self.assertEqual(UnassignedVote.objects.get().state, "pending")
