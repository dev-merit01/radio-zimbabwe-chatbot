"""Read-only routing check for every station (no listener or message contents).

Intended for startup diagnostics and administrators. A registered code is
not the same as an OPEN voting period; never alter settings in this command.
"""
from django.core.management.base import BaseCommand
from django.db.models import Count
from django.utils import timezone

from apps.accounts.models import station_choices
from apps.voting.models import ChartShow, StationVotingConfig, UnassignedVote


class Command(BaseCommand):
    help = "Report station voting-code readiness and held-vote reasons, without changing votes."

    def handle(self, *args, **kwargs):
        now = timezone.now()
        for station, _name in station_choices(active_only=True):
            config = StationVotingConfig.objects.filter(station=station).first()
            required = bool(config and config.require_codes)
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
