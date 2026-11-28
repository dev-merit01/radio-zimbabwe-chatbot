import csv
import json
from datetime import date

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import IntegrityError, transaction
from django.db.models import Count
from django.http import Http404, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.http import require_http_methods

from apps.charts.publishing import publish_edition
from apps.dashboard.views import chart_today_for_show
from apps.voting.models import ReviewAudit, SuspectedSpamVote, WeeklyChart
from apps.voting.pipeline import lock_station
from .models import BroadcastStation, StaffAccess
from .staff_forms import StaffAccountForm
from .staff_policy import (
    PRIVILEGES,
    shows_for,
    can_delegate_scope,
    Role,
    access_for,
    grantable_roles,
    permitted,
    require_privilege,
    stations_for,
)


@login_required
@require_http_methods(["GET", "POST"])
@transaction.atomic
def staff_accounts(request):
    actor = request.user
    if not actor.is_superuser:
        # Serialize delegation against a concurrent privilege edit.
        locked = StaffAccess.objects.select_for_update().filter(user=actor).first()
        if not locked:
            raise PermissionDenied
        actor.staff_access = locked
    roles = grantable_roles(actor)
    if not roles:
        raise PermissionDenied
    scope = stations_for(actor)
    accounts = StaffAccess.objects.filter(
        role__in=roles, user__is_superuser=False, user__is_staff=False
    ).select_related("user")
    if not actor.is_superuser:
        accounts = (
            accounts.filter(stations__in=scope)
            .exclude(stations__in=BroadcastStation.objects.exclude(pk__in=scope))
            .distinct()
        )
    # Never let a delegate reset credentials or edit a more privileged account.
    visible = [
        row
        for row in accounts.order_by("user__username")
        if can_delegate_scope(actor, row)
        and (actor.is_superuser or all(permitted(actor, key) for key in row.privileges))
    ]
    target = None
    if "edit" in request.GET:
        target = next(
            (row for row in visible if str(row.pk) == request.GET["edit"]), None
        )
        if not target:
            raise Http404
        target = StaffAccess.objects.select_for_update().get(pk=target.pk)
        if not actor.is_superuser and (
            not can_delegate_scope(actor, target)
            or target.role not in roles
            or target.stations.exclude(pk__in=scope).exists()
            or any(not permitted(actor, key) for key in target.privileges)
        ):
            raise PermissionDenied
    role = target.role if target else request.GET.get("role", roles[0])
    if role not in roles:
        raise PermissionDenied
    form = StaffAccountForm(
        request.POST if request.method == "POST" else None,
        actor=actor,
        role=role,
        scope=scope,
        target=target,
    )
    if request.method == "POST" and form.is_valid():
        try:
            form.save()
        except IntegrityError:
            form.add_error(
                None, "Account details changed. Check the username and try again."
            )
        else:
            messages.success(request, "Staff account saved.")
            return redirect("accounts:staff_accounts")
    return render(
        request,
        "accounts/staff_accounts.html",
        {
            "form": form,
            "server_workspace": True,
            "creating_accounts": True,
            "show_form": bool(
                target or "role" in request.GET or request.method == "POST"
            ),
            "scope_stations": scope,
            "can_manage": True,
            "show_options": list(
                form.available_shows.values("pk", "name", "station", "is_archived")
            ),
            "roles": [(r, Role(r).label) for r in roles],
            "role": role,
            "role_name": Role(role).label,
            "target": target,
            "accounts": visible,
        },
    )


def chart_csv(rows):
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="chart-votes.csv"'
    writer = csv.writer(response)
    writer.writerow(("Rank", "Artist", "Title", "Votes"))
    for row in rows:
        values = [row[key] for key in ("rank", "artists", "title", "count")]
        writer.writerow(
            [
                "'" + value
                if isinstance(value, str)
                and value.lstrip().startswith(("=", "+", "-", "@"))
                else value
                for value in values
            ]
        )
    return response


@login_required
@require_http_methods(["GET", "POST"])
def staff_home(request):
    user = request.user
    grant = access_for(user)
    if not user.is_superuser and (not grant or grant.role == Role.PRESENTER):
        raise PermissionDenied
    stations = stations_for(user)
    shows = shows_for(user, stations)
    show = None
    if "show" in request.GET:
        key = request.GET["show"]
        if not key.isdecimal() or len(key) > 18:
            raise Http404
        show = get_object_or_404(shows, pk=int(key))
    context = {
        "server_workspace": True,
        "role_description": grant.role_description if grant else "",
        "role_name": "Super admin" if user.is_superuser else grant.get_role_display(),
        "can_manage": bool(grantable_roles(user)),
        "show": show,
        "rights": [key for key in PRIVILEGES if permitted(user, key)],
    }
    if show is None:
        if request.method != "GET":
            raise Http404
        grouped = {}
        for item in shows.annotate(received=Count("raw_votes")).order_by("name"):
            grouped.setdefault(item.station, []).append(item)
        context["directory"] = [
            (station.name, grouped.get(station.slug, [])) for station in stations
        ]
    else:
        if request.method == "POST":
            return show_action(request, show)
        charts = WeeklyChart.objects.filter(
            station=show.station, chart_show=show, is_finalized=True
        )
        if "archive" in request.GET:
            require_privilege(user, "archives")
            key = request.GET["archive"]
            if not key.isdecimal() or len(key) > 18:
                raise Http404
            edition = get_object_or_404(charts, pk=int(key))
            rows = [
                {
                    "rank": e.rank,
                    "artists": e.artist,
                    "title": e.title,
                    "count": e.vote_count,
                }
                for e in edition.entries.order_by("rank")
            ]
            context["edition"] = edition
        else:
            rows = json.loads(
                chart_today_for_show(show, show.chart_size, user).content
            )["top100"]
        if request.GET.get("export"):
            require_privilege(user, "export")
            return chart_csv(rows)
        context.update(
            rows=rows,
            archives=charts if permitted(user, "archives") else (),
            station_name=stations.get(pk=show.station).name,
        )
        if permitted(user, "review"):
            context["flags"] = SuspectedSpamVote.objects.filter(
                station=show.station, chart_show=show, state="pending"
            ).order_by("received_at")[:100]
    return render(request, "accounts/staff_home.html", context)


@transaction.atomic
def show_action(request, show):
    action = request.POST.get("action")
    if action not in ("publish", "shows", "review"):
        raise PermissionDenied
    if not request.user.is_superuser:
        grant = get_object_or_404(
            StaffAccess.objects.select_for_update(), user=request.user
        )
        request.user.staff_access = grant
        if (
            not shows_for(request.user, stations_for(request.user))
            .filter(pk=show.pk)
            .exists()
        ):
            raise PermissionDenied
    require_privilege(request.user, action)
    lock_station(show.station)
    show.refresh_from_db()
    if show.is_archived:
        raise PermissionDenied
    try:
        if action == "publish":
            publish_edition(
                show.station,
                date.fromisoformat(request.POST.get("date", "")),
                request.user,
                show.chart_size,
                show.kind,
                show,
            )
        elif action == "review":
            from apps.dashboard.show_management import resolve_flagged_vote

            key = request.POST.get("flag", "")
            if not key.isdecimal() or len(key) > 18:
                raise Http404
            flag = get_object_or_404(
                SuspectedSpamVote, pk=int(key), chart_show=show, station=show.station
            )
            resolve_flagged_vote(
                show.station, flag.pk, request.POST.get("decision"), request.user
            )
        else:
            presenter = request.POST.get("presenter", "").strip()
            limit = int(request.POST.get("daily_limit", ""))
            if len(presenter) > 120 or not 1 <= limit <= 100:
                raise ValueError(
                    "Use a presenter name up to 120 characters and a daily limit from 1 to 100."
                )
            show.presenter_name, show.daily_limit = presenter, limit
            show.save(update_fields=["presenter_name", "daily_limit", "updated_at"])
            ReviewAudit.objects.create(
                station=show.station,
                actor=request.user,
                action="show_updated",
                details={"chart_show_id": show.pk, "daily_limit": limit},
            )
    except SuspectedSpamVote.DoesNotExist:
        messages.error(request, "This vote has already been reviewed.")
    except ValueError as exc:
        messages.error(request, str(exc))
    else:
        messages.success(request, "Changes saved.")
    return redirect(reverse("accounts:staff_home") + "?show=" + str(show.pk))
