"""Read-only routing check for every station (no listener or message contents).

Intended for startup diagnostics and administrators. A registered code is
not the same as an OPEN voting period; never alter settings in this command.
"""
from django.core.management.base import BaseCommand
from django.db.models import Count
from django.utils import timezone

from apps.charts.routing import station_requires_codes
from apps.accounts.models import station_choices
from apps.voting.models import ChartShow, UnassignedVote


class Command(BaseCommand):
    help = "Report station voting-code readiness and held-vote reasons, without changing votes."

    def handle(self, *args, **kwargs):
        now = timezone.now()
        for station, _name in station_choices(active_only=True):
            required = station_requires_codes(station)
            shows = ChartShow.objects.filter(station=station).exclude(kind="legacy").order_by("code", "id")
            entries = []
            for show in shows:
                entries.append(
                    f"{show.code or 'NO_CODE'}:"
                    f"{'OPEN' if show.accepts_at(now) else 'CLOSED'}"
                    f"(enabled={int(show.is_enabled)},"
                    f"mode={show.schedule_mode},"
                    f"manual={int(show.manual_open)})"
                )
            self.stdout.write(
                f"routing_audit station={station} "
                f"codes_required={int(required)} "
                f"shows=[{','.join(entries)}]"
            )
            # Aggregate state only, never the original message or sender.
            reasons = (
                UnassignedVote.objects.filter(station=station, state="pending")
                .values("reason").annotate(n=Count("id"))
                .order_by("reason")
            )
            self.stdout.write(
                f"routing_holds station={station} pending_by_reason="
                + ",".join(f"{row['reason']}:{row['n']}" for row in reasons)
            )
            held_codes = (
                UnassignedVote.objects.filter(
                    station=station, state="pending", reason="show_closed",
                    intended_show__isnull=False,
                ).values("intended_show__code").annotate(n=Count("id"))
                .order_by("intended_show__code")
            )
            self.stdout.write(
                f"routing_held_programmes station={station} codes="
                + ",".join(
                    f"{item['intended_show__code']}:{item['n']}"
                    for item in held_codes
                )
            )
