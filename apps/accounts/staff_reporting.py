"""Read-only reporting over the programmes authorised by the staff workspace."""

from collections import Counter, defaultdict
from datetime import datetime, time, timedelta
import re
from zoneinfo import ZoneInfo

from django.conf import settings
from django.core.paginator import Paginator
from django.db.models import (
    Case,
    Count,
    Exists,
    F,
    IntegerField,
    Max,
    OuterRef,
    Q,
    Subquery,
    Value,
    When,
    Window,
)
from django.db.models.functions import Coalesce, RowNumber, TruncDate, TruncHour
from django.http import Http404
from django.shortcuts import get_object_or_404
from django.utils import timezone

from apps.voting.models import (
    EditionCandidate,
    InboundEvent,
    MatchKeyMapping,
    RawVote,
    StationVotingConfig,
    SuspectedSpamVote,
    UnassignedVote,
)
from apps.voting.services import parse_vote_input
from .context_processors import local_station_logo
from .staff_policy import access_for, require_privilege

HARARE = ZoneInfo("Africa/Harare")
PHONE = re.compile(r"\+?\d(?:[\s().-]*\d){6,}")


def counted_votes(raw):
    """One eligible, verified vote per chart/song/listener/day unless repeats are enabled."""
    mapping = MatchKeyMapping.objects.filter(
        station=OuterRef("station"),
        cleaned_song__station=OuterRef("station"),
        match_key=OuterRef("match_key"),
        cleaned_song__status="verified",
    ).order_by()
    eligible = (
        raw.filter(ai_status__in=("approved", "legacy"))
        .annotate(song_key=Subquery(mapping.values("cleaned_song_id")[:1]))
        .exclude(song_key=None)
    )
    shortlist = EditionCandidate.objects.filter(
        edition__chart_show_id=OuterRef("chart_show_id"),
        edition__station=OuterRef("station"),
        edition__status="locked",
        edition__voting_start__lte=OuterRef("vote_date"),
        edition__voting_end__gte=OuterRef("vote_date"),
        song_id=OuterRef("song_key"),
    )
    eligible = eligible.filter(
        Q(chart_show__eligibility_enforced=False) | Exists(shortlist)
    )
    overrides = StationVotingConfig.objects.exclude(repeat_song_policy="inherit")
    allow = Q(
        station__in=overrides.filter(repeat_song_policy="allow").values("station")
    )
    if settings.ALLOW_REPEAT_SONG:
        allow |= ~Q(station__in=overrides.values("station"))
    eligible = eligible.annotate(
        position=Window(
            RowNumber(),
            partition_by=[
                F("chart_show_id"),
                F("song_key"),
                F("user_id"),
                F("vote_date"),
                Case(
                    When(allow, then=F("pk")),
                    default=Value(0),
                    output_field=IntegerField(),
                ),
            ],
            order_by=F("pk").asc(),
        )
    )
    return raw.filter(pk__in=eligible.filter(position=1).order_by().values("pk"))


def report_sources(show_ids, first, last, unrouted_stations):
    start = datetime.combine(first, time.min, HARARE)
    stop = datetime.combine(last + timedelta(days=1), time.min, HARARE)
    raw = (
        RawVote.objects.filter(
            chart_show_id__in=show_ids,
            station=F("chart_show__station"),
            vote_date__range=(first, last),
        )
        .order_by()
        .annotate(moment=Coalesce("receipt__received_at", "created_at"))
    )
    counted = counted_votes(raw)
    # Older accepted receipts have no FK. Their persisted acceptance reply and RawVote
    # describe the same submission. Keep the vote, which carries its current AI state.
    receipts = (
        InboundEvent.objects.filter(
            chart_show_id__in=show_ids,
            station=F("chart_show__station"),
            received_at__gte=start,
            received_at__lt=stop,
            recorded_vote__isnull=True,
        )
        .exclude(reply__startswith="✅ Submission received")
        .order_by()
    )
    flags = (
        SuspectedSpamVote.objects.filter(
            chart_show_id__in=show_ids,
            station=F("chart_show__station"),
            received_at__gte=start,
            received_at__lt=stop,
        )
        .exclude(state="released")
        .order_by()
    )
    held = (
        UnassignedVote.objects.filter(
            Q(intended_show_id__in=show_ids, station=F("intended_show__station"))
            | Q(intended_show__isnull=True, station__in=unrouted_stations),
            received_at__gte=start,
            received_at__lt=stop,
        )
        .exclude(state="assigned")
        .order_by()
    )
    sources = [
        (raw, "chart_show_id", Q(ai_status="review")),
        (
            receipts.annotate(moment=F("received_at")),
            "chart_show_id",
            Q(pk__isnull=True),
        ),
        (flags.annotate(moment=F("received_at")), "chart_show_id", Q(state="pending")),
        (
            held.annotate(moment=F("received_at")),
            "intended_show_id",
            Q(state="pending"),
        ),
    ]
    return sources, counted


def feed_page(sources, counted, counted_only, number, stations):
    raw, events, flags, held = [source for source, _, _ in sources]
    votes = (counted if counted_only else raw).annotate(
        result=Value("Counted")
        if counted_only
        else Case(
            When(pk__in=counted.values("pk"), then=Value("Counted")),
            When(ai_status="review", then=Value("Needs review")),
            When(ai_status="awaiting", then=Value("Verifying")),
            default=Value("Outside chart count"),
        ),
        programme=F("chart_show__name"),
        title=F("song_raw"),
        artist=F("artist_raw"),
        origin=Value("vote"),
    )
    columns = (
        "moment",
        "programme",
        "result",
        "title",
        "artist",
        "origin",
        "pk",
        "station",
    )
    rows = votes.values(*columns)
    if not counted_only:
        more = []
        for qs, show_name, origin, status in [
            (
                events,
                "chart_show__name",
                "receipt",
                Case(
                    When(state="done", then=Value("Not accepted")),
                    When(state="failed", then=Value("Processing failed")),
                    default=Value("Processing"),
                ),
            ),
            (
                flags,
                "chart_show__name",
                "flag",
                Case(
                    When(state="pending", then=Value("Needs review")),
                    default=Value("Dismissed"),
                ),
            ),
            (
                held,
                "intended_show__name",
                "held",
                Case(
                    When(state="pending", then=Value("Held for routing")),
                    default=Value("Dismissed"),
                ),
            ),
        ]:
            more.append(
                qs.annotate(
                    programme=F(show_name),
                    result=status,
                    title=F("vote_text"),
                    artist=Value(""),
                    origin=Value(origin),
                ).values(*columns)
            )
        rows = rows.union(*more, all=True)
    page = Paginator(rows.order_by("-moment", "origin", "-pk"), 25).get_page(number)
    for row in page:
        row["station_name"] = stations.get(row["station"], "")
        if row["origin"] != "vote":
            # Parse only the routed song field. Sender, raw message and worker replies stay private.
            parsed = (
                parse_vote_input(PHONE.sub("••••", row["title"]))
                if row["title"]
                else None
            )
            row["artist"], row["title"] = parsed or ("", "")
        row["title"] = PHONE.sub("••••", row["title"])
        row["artist"] = PHONE.sub("••••", row["artist"] or "")
    return page


def dashboard_context(request, stations, shows, show, can_view_feed):
    now = timezone.localtime(timezone.now(), HARARE)
    period = "week" if request.GET.get("period") == "week" else "today"
    first = now.date() - timedelta(days=6 if period == "week" else 0)
    station = None
    if request.GET.get("station"):
        station = get_object_or_404(stations, pk=request.GET["station"])
        if show and show.station != station.pk:
            raise Http404
    elif show:
        station = get_object_or_404(stations, pk=show.station)
    archived = request.GET.get("archived") == "1" or bool(show and show.is_archived)
    options = shows if archived else shows.filter(is_archived=False)
    if station:
        options = options.filter(station=station.pk)
    options = list(options.order_by("name"))
    selected = [show] if show else options
    station_list = list(stations.order_by("name"))
    station_names = {item.pk: item.name for item in station_list}
    unrouted_stations = []
    grant = access_for(request.user)
    if not show and (request.user.is_superuser or not grant.selected_shows_only):
        unrouted_stations = [station.pk] if station else list(station_names)
    sources, counted = report_sources(
        [s.pk for s in selected], first, now.date(), unrouted_stations
    )
    unrouted = Counter()
    totals, trend, recent = Counter(), Counter(), {}
    review = 0
    for qs, show_field, pending in sources:
        bucket = TruncHour("moment", tzinfo=HARARE)
        if period == "week":
            bucket = (
                F("vote_date")
                if qs.model is RawVote
                else TruncDate("moment", tzinfo=HARARE)
            )
        groups = (
            qs.annotate(bucket=bucket)
            .values(show_field, "station", "bucket")
            .annotate(
                n=Count("pk"),
                latest=Max("moment"),
                pending=Count("pk", filter=pending),
            )
        )
        for group in groups:
            key, bucket_time = group[show_field], group["bucket"]
            totals[key] += group["n"]
            if key is None:
                unrouted[group["station"]] += group["n"]
            recent[key] = max(group["latest"], recent.get(key, group["latest"]))
            review += group["pending"]
            index = (
                (bucket_time - first).days
                if period == "week"
                else bucket_time.hour // 3
            )
            trend[index] += group["n"]
    labels = (
        [(first + timedelta(days=i)).strftime("%a") for i in range(7)]
        if period == "week"
        else [f"{i:02d}h" for i in range(0, 24, 3)]
    )
    trend_max = max(trend.values(), default=1)
    grouped = defaultdict(list)
    for item in selected:
        item.received = totals[item.pk]
        item.last_vote = recent.get(item.pk)
        item.voting_open = item.accepts_at(now)
        grouped[item.station].append(item)
    directory = []
    for item in station_list:
        if station and item.pk != station.pk:
            continue
        programmes = sorted(grouped[item.pk], key=lambda s: (-s.received, s.name))
        item.logo_fallback = local_station_logo(item.pk)
        item.logo = item.logo_url or item.logo_fallback
        item.programmes = programmes
        item.received = unrouted[item.pk] + sum(s.received for s in programmes)
        directory.append(item)
    directory.sort(key=lambda item: -item.received)
    tab = request.GET.get("tab", "overview")
    if tab == "incoming" or request.GET.get("counted") == "1":
        require_privilege(request.user, "incoming")
    if tab not in ("overview", "incoming", "chart") or (tab == "chart" and not show):
        tab = "overview"
    feed = None
    if can_view_feed and (tab == "incoming" or (show and tab == "overview")):
        feed = feed_page(
            sources,
            counted,
            request.GET.get("counted") == "1",
            request.GET.get("page"),
            station_names,
        )
    return dict(
        report=True,
        report_station_logo=directory[0].logo if station else "",
        report_station_logo_fallback=directory[0].logo_fallback if station else "",
        updated=now,
        period=period,
        station_options=station_list,
        selected_station=station,
        programmes=options,
        station_rows=directory,
        archived=archived,
        tab=tab,
        feed=feed,
        counted_only=request.GET.get("counted") == "1",
        received=sum(totals.values()),
        counted=counted.count(),
        needs_review=review,
        open_shows=sum(s.voting_open for s in selected),
        unrouted_count=sum(unrouted.values()),
        trend=[
            (label, trend[i], round(100 * trend[i] / trend_max))
            for i, label in enumerate(labels)
        ],
    )
