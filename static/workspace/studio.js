"use strict";
(() => {
  const $ = (id) => document.getElementById(id);
  const esc = (v) =>
    String(v ?? "").replace(
      /[&<>"']/g,
      (c) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[c],
    );
  const num = (v) => Number(v || 0).toLocaleString();
  let reader, toastTimer;
  let liveSearchTimer, eligibleSearchTimer, proposalSearchTimer;
  let eligibleSearchSequence = 0, proposalSearchSequence = 0;
  let chartSize = 20, chartPeriod = "weekly";
  let selectedShowId = null, showsCache = [], showPolicyEnabled = false, activeVoteMultiplier = 1;
  let lastChartShowId = null;
  let selectedEditionId = null, eligibilityResults = [], eligibilityQuery = "", eligibilitySearched = false;
  let eligibilityCreateDate = null, eligibilityBroadcastDay = null, eligibilityShowName = "";
  let page = "overview",
    pagination = 1,
    query = "",
    generation = 0,
    online = false,
    permissions = {},
    timer;
  const titles = {
    overview: "Voting overview",
    shows: "Chart shows",
    eligibility: "Eligible songs",
    presenters: "Presenter accounts",
    administration: "Admin control",
    stations: "Manage stations",
    incoming: "Incoming votes",
    review: "Review queue",
    catalogue: "Song catalogue",
    archives: "Chart archives",
    activity: "Activity log",
    connections: "Connections",
  };
  async function api(path, data, signal) {
    const response = await fetch("/api/" + path, {
      credentials: "same-origin",
      signal: signal
        ? AbortSignal.any([signal, AbortSignal.timeout(15000)])
        : AbortSignal.timeout(15000),
      ...(data
        ? {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-CSRFToken": $("csrf-token").value,
            },
            body: JSON.stringify(data),
          }
        : {}),
    });
    if (response.status === 401 || response.redirected) {
      location.assign("/accounts/login/");
      throw new Error("Please sign in again.");
    }
    let result;
    try {
      result = await response.json();
    } catch {
      throw new Error(
        "The server returned an unexpected response. Please try again.",
      );
    }
    if (!response.ok) {
      const error = new Error(
        result.error || "The request could not be completed.",
      );
      error.status = response.status;
      throw error;
    }
    return result;
  }
  const btn = (label, action, id = "") =>
    `<button class="button secondary" data-action="${action}" data-id="${esc(id)}">${esc(label)}</button>`;
  const table = (headers, rows) =>
    `<div class="table-wrap"><table><thead><tr>${headers.map((h) => `<th>${h}</th>`).join("")}</tr></thead><tbody>${rows.join("")}</tbody></table></div>`;
  const empty =
    '<div class="empty"><h3>Nothing to show here</h3><p>Records will appear as your station receives and reviews votes.</p></div>';
  function panel(title, body, actions = "") {
    return `<section class="panel"><div class="panel-head"><h2>${esc(title)}</h2>${actions}</div>${body}</section>`;
  }
  function chartMovement(row) {
    const type = row.movement_status || "no_previous";
    const previous = row.previous_rank;
    const delta = row.movement_delta;
    const number = Number.isInteger(delta) ? Math.abs(delta) : 0;
    const label = type === "up" ? "Up " + number + " position" + (number === 1 ? "" : "s")
      : type === "down" ? "Down " + number + " position" + (number === 1 ? "" : "s")
      : type === "same" ? "No change"
      : type === "new" ? "New entry"
      : type === "re_entry" ? "Re-entry"
      : "No previous official chart";
    const symbol = type === "up" ? "↑" : type === "down" ? "↓"
      : type === "same" ? "→" : "";
    const content = type === "up" || type === "down"
      ? symbol + " " + number
      : type === "same" ? symbol + " SAME"
      : type === "new" ? "NEW"
      : type === "re_entry" ? "RE-ENTRY"
      : "—";
    const description = previous != null
      ? label + "; previously position " + previous
      : label;
    return '<span class="chart-movement chart-movement--' +
      esc(type) + '" aria-label="' + esc(description) +
      '" title="' + esc(description) + '">' + esc(content) + '</span>';
  }
  function chartMovementSummary(rows, comparison) {
    const comparisonAvailable = Boolean(comparison?.available);
    const heading = comparisonAvailable
      ? "Compared with " + esc(comparison.label.toLowerCase()) +
        " · " + esc(comparison.chart_date)
      : "No directly preceding published chart available";
    if (!rows.length) return "";
    const counts = {};
    rows.forEach((row) => {
      counts[row.movement_status] = (counts[row.movement_status] || 0) + 1;
    });
    const figures = [
      ["up", "Moving up"], ["down", "Moving down"],
      ["same", "Unchanged"], ["new", "New entries"],
      ["re_entry", "Re-entries"],
    ];
    return '<div class="chart-movement-summary" aria-label="Chart position changes">' +
      '<p class="chart-comparison-caption">' + heading + '</p>' +
      (comparisonAvailable
        ? '<div class="chart-movement-totals">' +
          figures.filter(([status]) => counts[status]).map(([status, label]) =>
            '<span><strong>' + num(counts[status]) + '</strong> ' + esc(label) +
            '</span>'
          ).join("") + '</div>'
        : "") + '</div>';
  }
  function chart(rows, comparison = {}) {
    if (!rows.length) return empty;
    const showScore = rows.some((r) => r.score !== undefined && r.score !== r.count);
    const previousLabel = comparison.label === "Previous annual chart"
      ? "Last year" : "Last week";
    return table(
      ["Position", "Song", "Votes", ...(showScore ? ["Weighted points"] : []),
        previousLabel, "Movement"],
      rows.map((r) =>
        `<tr><td class="chart-rank">#${num(r.rank)}</td><td><strong>${esc(r.title)}</strong><br><small>${esc(r.artists)}</small></td><td class="vote-count">${num(r.count)}</td>` +
        (showScore ? `<td class="vote-count">${num(r.score ?? r.count)}</td>` : "") +
        '<td class="chart-previous-rank">' +
          (r.previous_rank == null ? '—' : '#' + num(r.previous_rank)) +
          '</td><td>' + chartMovement(r) + '</td></tr>'
      ),
    );
  }
  const pager = (d) =>
    `<div class="table-footer"><span>${num(d.total)} records</span><div class="pagination">${d.page > 1 ? btn("Previous", "previous") : ""}<span>${d.page} / ${d.pages}</span>${d.page < d.pages ? btn("Next", "next") : ""}</div></div>`;
  const search = () =>
    `<form id="search-form" class="toolbar" role="search"><label class="searchbox"><input id="live-record-search" name="q" aria-label="Search records" placeholder="Search as you type…" autocomplete="off" maxlength="100" value="${esc(query)}"></label><button class="button secondary" type="submit">Search</button></form>`;
  function status(ok) {
    online = ok;
    $("connection").classList.toggle("offline", !ok);
    $("connection").lastElementChild.textContent = ok ? "Connected" : "Offline";
    $("offline-banner").classList.toggle("hidden", ok);
  }
  async function render() {
    // Focus and caret survive the server-filtered list refreshing.
    const active = document.activeElement;
    const focusState = active && ["live-record-search", "eligible-search-input", "archive-year-search"].includes(active.id)
      ? { id: active.id, start: null, end: null } : null;
    if (focusState) {
      try { focusState.start = active.selectionStart; focusState.end = active.selectionEnd; }
      catch { /* Number inputs have no selection range. */ }
    }
    const current = ++generation;
    reader?.abort();
    reader = new AbortController();
    const read = (path) => api(path, undefined, reader.signal);
    $("page-content").setAttribute("aria-busy", "true");
    clearTimeout(timer);
    try {
      let html = "";
      const showsData = await read("workspace/shows");
      if (current !== generation) return;
      showsCache = showsData.shows;
      showPolicyEnabled = showsData.require_codes;
      activeVoteMultiplier = showsData.vote_multiplier || 1;
      if (!showsCache.some((show) => show.id === selectedShowId))
        selectedShowId = showsData.presenter_show_id ||
          showsCache.find((show) => show.kind === "legacy")?.id ||
          showsCache[0]?.id;
      if (page === "eligibility" && showsCache.find((show) => show.id === selectedShowId)?.kind === "legacy")
        selectedShowId = showsCache.find((show) => show.kind !== "legacy")?.id;
      if (page === "eligibility" && !selectedShowId)
        throw new Error("Create a programme first before preparing its candidate list.");
      const selectedShow = showsCache.find((show) => show.id === selectedShowId);
      if (lastChartShowId !== selectedShowId) {
        chartSize = selectedShow?.kind === "legacy" ? 20 : selectedShow?.chart_size || 20;
        chartPeriod = selectedShow?.kind === "year_end" ? "year_end" : "weekly";
        lastChartShowId = selectedShowId;
      }
      const overview = await read("workspace/overview?show_id=" + selectedShowId);
      if (current !== generation) return;
      permissions = overview;
      $("nav-pending").textContent = num(overview.pending_songs);
      if (page === "overview") {
        const data = await read(`chart/today?limit=${chartSize}&period=${selectedShow?.kind === "year_end" ? "year_end" : chartPeriod}&show_id=${selectedShowId}`);
        html = `<div class="metrics">${[
          ["Received this week", overview.received],
          ["Verified votes", overview.verified_votes],
          ...(overview.vote_multiplier > 1 ? [["Weighted chart points", overview.verified_score]] : []),
          ["Unique listeners", overview.listeners],
          ["Songs awaiting review", overview.pending_songs],
        ]
          .map(
            ([label, value]) =>
              `<div class="metric"><span class="metric-label">${label}</span><strong class="metric-value">${num(value)}</strong><span class="metric-note">${label === "Songs awaiting review" ? "Current station · all dates" : "Current station · this week"}</span></div>`,
          )
          .join("")}</div>`;
        html +=
          '<div class="content-grid"><div>' +
          panel(
            chartPeriod === "year_end" ? "Year-to-date Top " + chartSize : "Live weekly Top " + chartSize,
            `<form id="chart-options" class="toolbar"><label>Period <select name="period"><option value="weekly" ${chartPeriod === "weekly" ? "selected" : ""} ${selectedShow?.kind === "year_end" ? "disabled" : ""}>Weekly chart</option><option value="year_end" ${chartPeriod === "year_end" || selectedShow?.kind === "year_end" ? "selected" : ""}>Annual countdown</option></select></label><label>Chart size <select name="size">${(selectedShow?.kind === "legacy" ? [20, 50] : [selectedShow?.chart_size || 20]).map(size => `<option value="${size}" ${chartSize === size ? "selected" : ""}>Top ${size}</option>`).join("")}</select></label><button class="button secondary">Show chart</button></form><p class="hint chart-period-meta">${esc(data.week_start)} to ${esc(data.week_end)} · Verified votes only${permissions.is_admin ? " · " + (data.vote_multiplier > 1 ? "Weighted scoring ×" + data.vote_multiplier + " (votes remain unchanged)" : "1 vote = 1 point") : ""}</p>` +
              chartMovementSummary(data.top100, data.comparison) +
              chart(data.top100, data.comparison),
            `<a class="button secondary" href="/api/workspace/export?limit=${chartSize}&period=${chartPeriod}&show_id=${selectedShowId}">Export CSV</a>`,
          ) +
          '</div><aside class="side-stack"><section class="panel activity-card"><h2>Votes this week</h2><p>Received votes by day, including songs awaiting review.</p><canvas id="timeline" width="520" height="250" aria-label="Daily vote counts" role="img"></canvas><p id="timeline-summary" class="sr-only"></p></section><section class="review-callout"><h3>' +
          num(overview.pending_songs) +
          (overview.pending_songs === 1
            ? " song to review</h3>"
            : " songs to review</h3>") +
          '<p>Check new entries before they appear on the chart.</p><button class="button secondary" data-page="review">Open review queue</button></section></aside></div>';
      } else if (["review", "catalogue", "incoming"].includes(page)) {
        const incoming = page === "incoming";
        const d = await read(
          `workspace/${incoming ? "incoming" : "songs"}?status=${page === "review" ? "pending" : "all"}&page=${pagination}&q=${encodeURIComponent(query)}${incoming ? "&show_id=" + selectedShowId : ""}`,
        );
        let rows = d.items.map((r) =>
          incoming
            ? `<tr><td>${esc(r.text)}</td><td>${esc(r.channel)}</td><td>${esc(r.listener)}</td><td>${esc(new Date(r.received_at).toLocaleString())}</td></tr>`
            : `<tr><td><strong>${esc(r.title)}</strong><br>${esc(r.artist)}<br><small>Song ID: ${r.id}</small></td><td>${num(r.votes)}</td><td>${esc(r.status)}</td><td><div class="row-actions">${permissions.can_review ? btn("Edit", "edit", r.id) + btn("Verify", "verified", r.id) + btn("Reject", "rejected", r.id) + btn("Merge", "merge", r.id) : ""}</div></td></tr>`,
        );
        html = panel(
          incoming
            ? "Incoming votes"
            : page === "review"
              ? "Review queue"
              : "Song catalogue",
          search() +
            (rows.length
              ? table(
                  incoming
                    ? ["Vote", "Channel", "Listener", "Received"]
                    : ["Song", "All-time mapped votes", "Status", "Actions"],
                  rows,
                )
              : empty) +
            pager(d),
          incoming && permissions.can_record_vote ? btn("Record vote", "record-vote") :
          !incoming && permissions.can_add ? btn("Add song", "add") : "",
        );
        window.studioSongs = d.items;
        if (incoming && d.submissions?.length) {
          html += `<section class="panel"><div class="panel-heading"><h2>Recent submissions</h2></div><div class="table-wrap"><table><thead><tr><th>Vote</th><th>Processing</th><th>Result</th></tr></thead><tbody>${d.submissions.map(s => `<tr><td>${esc(s.text)}</td><td>${esc(s.state)}</td><td>${esc(s.reply || "Waiting for the voting worker")}</td></tr>`).join("")}</tbody></table></div></section>`;
        }
      } else if (page === "eligibility") {
        const show = showsCache.find((item) => item.id === selectedShowId);
        if (show?.kind === "legacy")
          throw new Error("Choose a chart show instead of the historical chart.");
        if (window.eligibilityLastShowId !== selectedShowId) {
          window.eligibilityLastShowId = selectedShowId;
          selectedEditionId = null;
          eligibilityResults = [];
          eligibilityQuery = "";
          eligibilitySearched = false;
        }
        const url = "workspace/eligibility?show_id=" + selectedShowId +
          (selectedEditionId ? "&edition_id=" + selectedEditionId : "");
        const d = await read(url);
        if (current !== generation) return;
        const selected = d.selected;
        selectedEditionId = selected?.id || null;
        eligibilityCreateDate = d.suggested_chart_date;
        eligibilityBroadcastDay = d.broadcast_weekday;
        eligibilityShowName = d.chart_show;
        const canEdit = d.can_prepare && selected && ["draft", "returned", "submitted"].includes(selected.status);
        const weekday = days[d.broadcast_weekday] || "Broadcast day";
        const count = selected?.candidates?.length || 0;
        const currentIds = new Set(selected?.candidates?.map((item) => item.id) || []);
        const editionLabel = (item) => item.status === "locked"
          ? (item.voting_open ? "Voting Open" : "Published") : "Draft · in progress";
        const statusClass = selected?.status === "locked" ? "approved"
          : selected?.status === "submitted" ? "submitted"
          : selected?.status === "returned" ? "returned" : "draft";
        const createButton = d.can_create
          ? '<button class="button primary" data-action="eligible-new">+ Create edition</button>'
          : "";
        const programmeSelect = d.is_admin
          ? '<label class="eligible-programme-field">Chart programme' +
            '<select id="eligibility-show-select" aria-label="Select chart programme">' +
            showsCache.filter((item) => item.kind !== "legacy" && !item.is_archived).map((item) =>
              '<option value="' + item.id + '"' +
              (item.id === selectedShowId ? " selected" : "") + '>' +
              esc(item.name) + '</option>'
            ).join("") + '</select></label>'
          : '<div class="eligible-assigned"><span>YOUR ASSIGNED PROGRAMME</span>' +
            '<strong>' + esc(d.chart_show) + '</strong></div>';
        const editionItems = d.editions.length
          ? d.editions.map((item) =>
            '<button class="eligible-edition-option' +
            (selectedEditionId === item.id ? " is-selected" : "") +
            '" data-action="eligible-select" data-id="' + item.id + '"' +
            ' aria-pressed="' + String(selectedEditionId === item.id) + '">' +
            '<span>' + esc(item.chart_date) + '</span>' +
            '<small>' + esc(editionLabel(item)) +
            ' · ' + num(item.candidate_count) + ' songs</small></button>'
          ).join("")
          : '<p class="eligible-none">No chart editions yet. Create the first one below.</p>';
        const intro = '<section class="eligible-intro" aria-label="Eligible songs workflow">' +
          '<div class="eligible-intro-heading"><span>CHART SHOW PREPARATION</span>' +
          '<h2>Choose the songs that can compete</h2>' +
          '<p>Select verified songs for an upcoming edition. You can include more candidates than ' +
          'the Top ' + num(d.chart_size) + ' chart positions. Listener votes determine the final rankings.</p></div>' +
          '<div class="eligible-steps" aria-label="Three-step publishing process">' +
          '<span><b>1</b> Create edition</span><span><b>2</b> Choose songs</span>' +
          '<span><b>3</b> Publish list</span></div></section>';
        const selectorPanel = panel("Programme and editions",
          '<div class="eligible-selector-body">' +
          '<div class="eligible-selector-top">' + programmeSelect +
          '<div class="eligible-overview">' +
          '<span>CHART SIZE<strong>Top ' + num(d.chart_size) + '</strong></span>' +
          '<span>BROADCAST<strong>' +
          esc(d.kind === "year_end" ? "Annual countdown" : weekday) + '</strong></span>' +
          '<span>ELIGIBILITY<strong class="' +
          (d.eligibility_enforced ? "eligible-enabled" : "eligible-inactive") + '">' +
          (d.eligibility_enforced ? "Enabled" : "Not yet published") +
          '</strong></span></div></div>' +
          '<div class="eligible-editions-title"><strong>Chart editions</strong>' +
          '<span>Select an edition to prepare or view its eligible songs</span></div>' +
          '<div class="eligible-edition-list">' + editionItems + '</div>' +
          (!d.can_create && d.kind === "year_end"
            ? '<p class="eligible-setup-notice">This annual programme needs a future voting campaign configured by the administrator before a new edition can be created.</p>'
            : "") +
          '</div>', createButton);
        const startPanel = '<section class="eligible-start">' +
          '<div class="eligible-start-symbol" aria-hidden="true">♪</div>' +
          '<div><h3>No shortlist prepared yet</h3>' +
          '<p>Start with a chart edition dated for the next ' +
          esc(d.kind === "year_end" ? "annual broadcast" : weekday + " broadcast") +
          '. Then search the verified music catalogue and choose competing songs.</p>' +
          (d.can_create
            ? '<p class="eligible-date-preview">Suggested chart date: <strong>' +
              esc(d.suggested_chart_date) + '</strong></p>' +
              '<button class="button primary" data-action="eligible-new">Create first edition</button>'
            : '<p class="eligible-date-preview">Ask the administrator to configure the programme schedule before preparing an edition.</p>') +
          '</div></section>';
        let candidatePanel = "";
        if (selected) {
          const details = '<div class="eligible-details">' +
            '<div class="eligible-statusline"><span class="eligible-status eligible-status--' +
            statusClass + '">' + esc(editionLabel(selected)) +
            '</span><span>Voting dates: <strong>' + esc(selected.voting_start) +
            '</strong> to <strong>' + esc(selected.voting_end) + '</strong></span></div>' +
            (selected.note ? '<p class="eligible-review-note"><strong>Review note:</strong> ' +
              esc(selected.note) + '</p>' : "") +
            (selected.status === "locked"
              ? '<p class="eligible-locked">Published by ' +
                esc(selected.published_by || selected.prepared_by) +
                '. This list is protected once published.</p>'
              : '') + '</div>';
          const previousNote = '<p class="eligible-previous-note">' +
            (selected.previous_chart?.available
              ? 'Last official edition: <strong>' + esc(selected.previous_chart.chart_date) +
                '</strong>. Previous positions are for reference only.'
              : 'No immediately preceding published chart is available for comparison.') +
            '</p>';
          const candidateContent = selected.candidates.length
            ? table(["Artist", "Song", "Previous position", "Prior votes", "Action"],
                selected.candidates.map((item) =>
                  '<tr><td>' + esc(item.artist) + '</td><td><strong>' +
                  esc(item.title) + '</strong></td><td class="chart-previous-rank">' +
                  (item.previous_rank == null ? '—' : '#' + num(item.previous_rank)) +
                  '</td><td>' + (item.previous_votes == null ? '—' : num(item.previous_votes)) +
                  '</td><td>' + (canEdit
                    ? btn("Remove", "eligible-remove", item.id)
                    : '<span class="eligible-row-state">Eligible song</span>') +
                  '</td></tr>'))
            : '<div class="eligible-candidate-empty">Your shortlist is empty. ' +
              'Search verified songs below and click <strong>Add to shortlist</strong>.</div>';
          const stats = '<div class="eligible-list-stats">' +
            '<div><strong>' + num(count) + '</strong><span>selected candidates</span></div>' +
            '<div><strong>Top ' + num(d.chart_size) + '</strong><span>published chart</span></div>' +
            '<p>The shortlist may contain more than ' + num(d.chart_size) +
            ' songs. Only the highest-ranked ' + num(d.chart_size) +
            ' appear in the official countdown.</p></div>';
          const searchPanel = canEdit
            ? '<div class="eligible-search">' +
              '<div class="eligible-search-heading"><h3>Add verified songs</h3>' +
              '<p>Search by artist or song title in this station’s verified catalogue.</p></div>' +
              '<form id="eligible-search-form" class="eligible-search-controls">' +
              '<label for="eligible-search-input" class="sr-only">Search verified songs</label>' +
              '<input id="eligible-search-input" name="q" type="search" maxlength="100" required' +
              ' placeholder="Enter an artist or song title" value="' + esc(eligibilityQuery) + '">' +
              '<button class="button primary" type="submit">Search catalogue</button></form>' +
              (eligibilitySearched
                ? '<div class="eligible-results-head"><strong>' +
                  (eligibilityResults.length
                    ? num(eligibilityResults.length) + ' verified songs found'
                    : 'No matching verified songs') + '</strong><span>' +
                  (eligibilityResults.length
                    ? 'Showing up to 30 results. Refine your search to find more.'
                    : 'Check the spelling or propose a new song below.') +
                  '</span></div>' +
                  (eligibilityResults.length
                    ? table(["Artist", "Song", "Select"],
                        eligibilityResults.map((item) =>
                          '<tr><td>' + esc(item.artist) + '</td><td>' +
                          '<strong>' + esc(item.title) + '</strong></td><td>' +
                          (currentIds.has(item.id)
                            ? '<span class="eligible-added">✓ Added</span>'
                            : btn("Add to shortlist", "eligible-add", item.id)) +
                          '</td></tr>'))
                    : '<p class="eligible-no-results">No matching verified songs in this station’s catalogue.</p>')
                : '<p class="eligible-search-tip">Start by searching for an artist or song. ' +
                  'Only verified songs can be added directly.</p>') +
              '<div class="eligible-search-footer">' +
              '<span>Song not listed or not verified yet?</span>' +
              btn("Propose a new song", "eligible-suggest") + '</div></div>'
            : "";
          const submitBar = canEdit
            ? '<div class="eligible-submit-bar"><div><strong>Ready to publish?</strong>' +
              '<span>Check your verified songs, then publish directly. No administrator approval is needed.</span></div>' +
              '<button class="button primary" data-action="eligible-submit"' +
              (count === 0 ? ' disabled title="Add at least one verified song first"' : "") +
              '>Publish eligible list</button></div>'
            : "";
          const suggestions = selected.suggestions.length
            ? '<div class="eligible-suggestions"><h3>Proposed songs awaiting verification</h3>' +
              table(["Artist", "Song", "Review status", "Action"],
                selected.suggestions.map((item) =>
                  '<tr><td>' + esc(item.primary_artist || item.artist) +
                  (item.featured_artists ? '<br><small>feat. ' +
                    esc(item.featured_artists) + '</small>' : '') + '</td><td>' +
                  esc(item.title) + '</td><td>' + esc(item.status) +
                  '</td><td>' +
                  (d.is_admin && item.status === "pending"
                    ? btn("Link verified song", "eligible-proposal-accept", item.id) +
                      btn("Reject", "eligible-proposal-reject", item.id)
                    : '<span class="eligible-row-state">—</span>') +
                  '</td></tr>')) + '</div>' : "";
          candidatePanel = panel("Shortlist · " + selected.chart_date,
            details + stats + previousNote + candidateContent +
            searchPanel + suggestions + submitBar);
        }
        const nominations = d.nomination_interest?.length
          ? panel("Unlisted song nominations",
            '<p class="eligible-note-inset">These requests are recorded as nomination interest, not as official chart votes.</p>' +
            table(["Song requested", "Submissions"], d.nomination_interest.map((item) =>
              '<tr><td>' + esc(item.display_name) + '</td><td>' + num(item.n) + '</td></tr>')))
          : "";
        html = '<div class="eligible-page">' + intro + selectorPanel +
          (selected ? candidatePanel + nominations : startPanel) + '</div>';
      } else if (page === "stations") {
        const d = await read("workspace/stations");
        if (current !== generation) return;
        window.airvoteNetworkStations = d.stations;
        const active = d.stations.filter((station) => station.is_active).length;
        const stationCards = d.stations.map((station) => {
          const selected = station.slug === overview.station;
          const number = station.effective_bird_number
            ? "+" + station.effective_bird_number
            : "Not connected";
          const connection = station.number_source === "database"
            ? "Managed in AirVote"
            : station.number_source === "environment"
              ? "Existing Railway connection"
              : "Needs Bird number";
          return '<article class="network-station-card">' +
            '<div class="network-station-header"><div><strong>' +
              esc(station.name) + '</strong><small>' + esc(station.slug) +
              '</small></div><span class="show-status ' +
              (station.is_active ? "show-status--live" : "show-status--disabled") + '">' +
              (station.is_active ? "Active" : "Setup required") +
              '</span></div>' +
            '<div class="network-station-details">' +
              '<p><span>WhatsApp</span><strong>' + esc(number) +
              '</strong><small>' + esc(connection) + '</small></p>' +
              '<p><span>Chart shows</span><strong>' + num(station.chart_shows) +
              '</strong></p>' +
              '</div>' +
            '<div class="network-station-actions">' +
              (selected ? '<span class="network-selected">Current workspace</span>' :
                btn("Open workspace", "admin-station", station.slug)) +
              btn("Configure station", "network-station-edit", station.slug) +
            '</div></article>';
        }).join("");
        html = '<div class="network-page">' +
          '<div class="network-intro"><div><h2>Station management</h2>' +
          '<p>Configure additional radio stations without installing another system. ' +
          'Each station has independent votes, programme codes and presenter permissions.</p>' +
          '</div>' + btn("+ Add station", "network-station-new") + '</div>' +
          '<div class="network-metrics">' +
          '<div><span>Registered stations</span><strong>' + num(d.stations.length) + '</strong></div>' +
          '<div><span>Active stations</span><strong>' + num(active) + '</strong></div>' +
          '<div><span>Awaiting setup</span><strong>' + num(d.stations.length - active) + '</strong></div>' +
          '</div><div class="network-grid">' + stationCards + '</div>' +
          '<section class="panel network-guidance"><h2>Set up a new station</h2>' +
          '<div class="network-steps">' +
          '<p><b>1.</b> Create a station. AirVote generates its permanent ID automatically.</p>' +
          '<p><b>2.</b> Open the station workspace and create its chart shows. Give each show a distinct voting code, voting schedule and presenter.</p>' +
          '<p><b>3.</b> Assign presenter accounts so presenters can select and publish eligible songs before voting opens.</p>' +
          '<p><b>4.</b> Connect the Bird receiving number, then activate the station and enable its shows.</p>' +
          '</div><p class="hint">A phone number must already be connected to Bird and its signed webhook configured. Adding a number here only routes incoming messages; it does not purchase or connect a WhatsApp number.</p></section>' +
          '</div>';
      } else if (page === "presenters") {
        const d = await read("workspace/presenters");
        if (current !== generation) return;
        window.presenterAccounts = d;
        const choices = showsCache.filter((item) =>
          item.kind !== "legacy" && !item.is_archived
        );
        html = '<div class="presenter-page">' +
          panel("Presenter accounts · " + esc(overview.station),
            '<p class="hint">Assign each presenter to one chart show in the selected station. They sign in with their own username and password; only the central administrator can change assignments.</p>' +
            (d.presenters.length ? table(
              ["Username", "Assigned chart show", "Login", "Action"],
              d.presenters.map((item) =>
                '<tr><td><strong>' + esc(item.username) + '</strong></td><td>' +
                esc(item.chart_show) + '</td><td>' +
                (item.active ? "Active" : "Inactive") + '</td><td>' +
                btn("Change assignment", "presenter-reassign", item.user_id) +
                '</td></tr>'
              )
            ) : '<p class="show-queue-empty">No presenters assigned to this station yet.</p>'),
            choices.length ? btn("Create presenter account", "presenter-create") : "") +
          panel("Existing station accounts",
            '<p class="hint">Existing station accounts continue to work. The administrator can assign an account to a specific programme without changing its password.</p>' +
            (d.unassigned_accounts.length ? table(
              ["Username", "Status", "Action"],
              d.unassigned_accounts.map((item) =>
                '<tr><td>' + esc(item.username) + '</td><td>' +
                (item.active ? "Active" : "Inactive") + '</td><td>' +
                (choices.length ? btn("Assign chart show", "presenter-assign", item.user_id) : "") +
                '</td></tr>'
              )
            ) : '<p class="show-queue-empty">All current station accounts have assignments.</p>')) +
          '</div>';
      } else if (page === "shows") {
        window.studioShows = showsCache;
        const programmeShows = showsCache.filter((show) => show.kind !== "legacy");
        const liveCount = programmeShows.filter((show) => show.accepting).length;
        const cardMarkup = showsCache.filter((show) => !show.is_archived).map((show) => {
          const historical = show.kind === "legacy";
          const archived = show.is_archived;
          const status = show.accepting ? "Accepting votes"
            : archived ? "Archived"
            : historical ? "Historical"
            : show.is_enabled ? "Outside voting hours" : "Disabled";
          const statusStyle = show.accepting ? "live"
            : historical ? "historical" : archived ? "archived"
            : show.is_enabled ? "scheduled" : "disabled";
          const typeLabel = historical ? "STATION ARCHIVE"
            : show.kind === "year_end" ? "ANNUAL COUNTDOWN" : "WEEKLY PROGRAMME";
          const code = historical ? '<span class="show-code legacy-code">Historical records only</span>'
            : '<span class="show-code">' + esc(show.code) + '</span>';
          const detail = historical ? "Existing station-wide votes and saved charts"
            : show.kind === "year_end" ? "Separate Top 50 campaign"
            : "Top " + show.chart_size + " · " + esc(days[show.broadcast_weekday] || "Saturday") + " broadcast";
          const broadcastTime = show.broadcast_start
            ? show.broadcast_start + (show.broadcast_end ? "–" + show.broadcast_end : " · end time to confirm")
            : "To be confirmed";
          const broadcast = historical ? "" :
            '<p class="show-broadcast"><strong>On air:</strong> ' +
            (show.kind === "year_end" ? "Date to be confirmed" : esc(days[show.broadcast_weekday])) +
            ' · ' + esc(broadcastTime) + '</p>' +
            '<p class="show-presenter"><strong>Presenter:</strong> ' +
            esc(show.presenter_name || "To be assigned") + '</p>';
          return '<article class="show-card' + (archived ? " is-archived" : "") + '">' +
            '<div class="show-card-top">' +
              '<span class="show-type">' + typeLabel + '</span>' +
              '<span class="show-status show-status--' + statusStyle + '">' + status + '</span>' +
            '</div>' +
            '<h3 class="show-card-title">' + esc(show.name) + '</h3>' +
            '<p class="show-card-description">' + detail + '</p>' +
            broadcast +
            '<div class="show-code-block"><span class="show-code-label">VOTING CODE</span>' +
              code + '</div>' +
            (historical ? "" : '<p class="show-vote-example">Send: <strong>' +
              esc(show.code) + ' Artist - Song</strong></p>') +
            '<div class="show-card-actions">' +
              btn("View results", "show-select", show.id) +
              (showsData.is_admin && !historical && !archived
                ? btn("Edit settings", "show-edit", show.id) +
                  btn("Duplicate", "show-duplicate", show.id) +
                  btn("Archive", "show-archive", show.id)
                : "") +
            '</div>' +
          '</article>';
        }).join("");
        html = '<div class="show-page">' +
          '<div class="show-summary" aria-label="Programme overview">' +
            '<div class="show-summary-item"><span>Chart shows</span><strong>' +
              num(programmeShows.length) + '</strong></div>' +
            '<div class="show-summary-item"><span>Accepting votes now</span><strong>' +
              num(liveCount) + '</strong></div>' +
            '<div class="show-summary-item"><span>Voting codes</span><strong>' +
              (showsData.require_codes ? "Required" : "Optional") + '</strong></div>' +
          '</div>' +
          (!liveCount
            ? '<section class="review-callout" role="status"><h3>No chart shows are accepting votes</h3>' +
              '<p>All programme voting windows at this station are currently closed. ' +
              'Messages containing programme codes are safely held rather than counted. ' +
              (showsData.is_admin
                ? 'Use Edit settings below to enable a programme and configure its voting window.'
                : 'Please contact the administrator to open the programme.') +
              '</p></section>'
            : '') +
          (showsData.unassigned_pending
            ? '<section class="review-callout" role="status"><h3>' +
              num(showsData.unassigned_pending) + ' votes need routing review</h3>' +
              '<p>These messages were not counted. The Unassigned votes list below shows the reason.</p></section>'
            : '') +
          '<section class="panel show-directory" aria-labelledby="show-directory-title">' +
            '<div class="show-directory-head">' +
              '<div><h2 id="show-directory-title">Programmes for this station</h2>' +
                '<p>Manage each programme independently. Previous station votes remain preserved.</p></div>' +
              (showsData.is_admin ? btn("+ Create chart show", "show-new") : "") +
            '</div>' +
            '<div class="show-grid">' + cardMarkup + '</div>' +
          '</section>';
        if (showsData.is_admin) {
          const waiting = await read("workspace/unassigned");
          if (current !== generation) return;
          window.studioUnassigned = waiting.items;
          const flagged = await read("workspace/spam");
          if (current !== generation) return;
          window.studioSpam = flagged.items;
          html += '<div class="show-settings-grid">' +
            '<section class="panel show-settings-panel" aria-labelledby="show-multiplier-title">' +
              '<div class="show-settings-head"><h2 id="show-multiplier-title">Station vote multiplier</h2>' +
                '<span class="show-multiplier-current">×' + activeVoteMultiplier + '</span></div>' +
              '<p>Applies to the selected station and all its chart shows. Listener votes remain unchanged; charts display weighted points separately. Published editions keep their saved multiplier.</p>' +
              btn("Change multiplier", "station-multiplier") +
            '</section>' +
            '<section class="panel show-settings-panel" aria-labelledby="show-code-policy-title">' +
              '<div class="show-settings-head"><h2 id="show-code-policy-title">Voting code settings</h2>' +
                '<span class="show-status show-status--' + (showsData.require_codes ? "live" : "scheduled") + '">' +
                (showsData.require_codes ? "Codes required" : "Compatibility mode") +
                '</span></div>' +
              '<p>' + (showsData.codes_enforced_by_shows
                ? "Programme codes are enforced for this multi-chart station. Missing and unknown codes are held for review, not counted under historical charts."
                : showsData.require_codes
                ? "Listeners must include a programme code. Unknown codes are held for review."
                : "No coded programmes have been configured. Enable codes before opening voting.") +
              '</p>' +
              (showsData.codes_enforced_by_shows ? "" :
                btn(showsData.require_codes ? "Make codes optional" : "Require programme codes", "show-policy")) +
            '</section>' +
            '<section class="panel show-settings-panel" aria-labelledby="show-unassigned-title">' +
              '<div class="show-settings-head"><h2 id="show-unassigned-title">Unassigned votes</h2>' +
                '<span class="show-queue-count">' + num(waiting.items.length) + ' awaiting review</span></div>' +
              '<p>Messages with missing, unknown or closed-show codes are never added to a chart automatically.</p>' +
              (waiting.items.length
                ? '<div class="show-queue-table">' + table(
                    ["Received", "Vote text", "Intended chart", "Reason", "Actions"],
                    waiting.items.map((item) =>
                      '<tr><td>' + esc(new Date(item.received_at).toLocaleString()) +
                      '</td><td class="show-queue-text">' + esc(item.text) +
                      '</td><td>' + esc(item.intended_show || "Unspecified") +
                      (item.code ? " (" + esc(item.code) + ")" : "") +
                      '</td><td>' + esc(item.reason.replaceAll("_", " ")) +
                      '</td><td><div class="show-queue-actions">' +
                      (item.reason === "show_closed"
                        ? '<span class="show-queue-count">Held · voting was closed</span>'
                        : btn("Assign", "show-assign", item.id)) +
                      btn("Dismiss", "show-dismiss", item.id) +
                      '</div></td></tr>'
                    )
                  ) + '</div>'
                : '<p class="show-queue-empty">No unassigned votes at this station.</p>') +
            '</section>' +
            '<section class="panel show-settings-panel show-spam-panel" aria-labelledby="show-spam-title">' +
              '<div class="show-settings-head"><h2 id="show-spam-title">Suspected spam votes</h2>' +
                '<span class="show-queue-count show-spam-count">' +
                num(flagged.pending) + ' flagged for review</span></div>' +
              '<p>Repeated submissions or unusually rapid votes from the same sender are held before AI processing. They are not counted unless the administrator releases them. Popular songs voted for by different listeners are unaffected.</p>' +
              (flagged.items.length
                ? '<div class="show-queue-table">' + table(
                    ["Received", "Listener", "Chart show", "Vote", "Flag", "Review"],
                    flagged.items.map((item) =>
                      '<tr><td>' + esc(new Date(item.received_at).toLocaleString()) +
                      '</td><td>' + esc(item.listener) +
                      '</td><td>' + esc(item.show) +
                      '</td><td class="show-queue-text">' + esc(item.text) +
                      '</td><td>' + esc(item.reason.replaceAll("_", " ")) +
                      '</td><td><div class="show-queue-actions">' +
                      btn("Release", "spam-release", item.id) +
                      btn("Dismiss", "spam-dismiss", item.id) +
                      '</div></td></tr>'
                    )
                  ) + '</div>'
                : '<p class="show-queue-empty">No suspicious votes are currently awaiting review.</p>') +
            '</section>' +
          '</div>';
        }
        html += '</div>';
      } else if (page === "archives") {
        const d = await read(
          "chart/archives?show_id=" + selectedShowId +
            (query ? "&year=" + encodeURIComponent(query) : ""),
        );
        html = panel(
          "Chart archives",
          `<form id="search-form" class="toolbar" role="search"><label>Year <input id="archive-year-search" name="q" type="number" min="1900" max="9999" aria-label="Filter archive year" value="${esc(query || d.year)}"></label><button class="button secondary" type="submit">Load</button></form>` +
            (d.charts.length
              ? table(
                  ["Edition", "Voting period", "Votes", ...(permissions.is_admin ? ["Weighted points"] : []), "Open"],
                  d.charts.map(
                    (c) =>
                      `<tr><td>${c.is_year_end ? "Year-end Top 50" : "Top " + c.chart_size + " · " + (c.chart_date || "Week " + c.week_number)}</td><td>${esc(c.week_start)} – ${esc(c.week_end)}</td><td>${num(c.total_votes)}</td>${permissions.is_admin ? `<td>${num(c.total_score)} (×${c.vote_multiplier})</td>` : ""}<td>${btn("View chart", "archive", c.id)}</td></tr>`,
                  ),
                )
              : empty),
          permissions.can_publish
            ? btn(selectedShow?.kind === "year_end" ? "Publish year-end Top 50" : selectedShow?.kind === "legacy" ? "Save Saturday chart" : "Publish this chart", "publish") + (selectedShow?.kind === "legacy" ? btn("Save December Top 50", "publish-year") : "")
            : "",
        );
      } else if (page === "administration") {
        const d = await read("workspace/administration");
        const selected = d.stations.find(s => s.id === overview.station);
        html = `<div class="admin-metrics">${[
          ["Listener votes", num(d.summary.votes), "All stations · all time"],
          ["Awaiting review", num(d.summary.pending_songs), "Songs across the network"],
          ["Access requests", num(d.summary.pending_accounts), "Accounts awaiting activation"],
          ["Active accounts", num(d.summary.active_accounts), "Approved station team members"],
        ].map(([label,value,note]) => `<div class="admin-metric"><span>${label}</span><strong>${value}</strong><small>${note}</small></div>`).join("")}</div>
        <div class="admin-grid"><div class="admin-main">
        ${panel("Network configuration", '<p>Expand AirVote to any number of radio stations, then configure shows and presenter access independently.</p>', btn("Manage stations", "network-manage"))}
        ${panel("Station directory", `<div class="station-directory">${d.stations.map(station => `<article class="station-tile ${station.id === overview.station ? "selected" : ""}"><div class="station-tile-heading"><span class="station-monogram">${esc(station.name.split(" ").map(w=>w[0]).join("").slice(0,2))}</span><div><h3>${esc(station.name)}</h3><small>${station.id === overview.station ? "Current workspace" : "Independent station workspace"}</small></div></div><div class="station-tile-stats"><span><strong>${num(station.votes)}</strong> received votes</span><span><strong>${num(station.pending)}</strong> to review</span><span><strong>${num(station.spam)}</strong> spam flags</span></div>${station.id === overview.station ? '<span class="access-badge">Selected</span>' : btn("Open station", "admin-station", station.id)}</article>`).join("")}</div>`)}
        ${panel("Accounts & access", `<div class="admin-section-intro"><p>Approve access for the station shown. Account management includes station assignments, password resets and deactivation.</p></div>` + table(["Team member", "Assigned station", "Access", "Action"], d.accounts.map(u => `<tr><td><strong>${esc(u.name || u.username)}</strong><br><small>@${esc(u.username)}</small></td><td>${esc(u.station)}</td><td><span class="access-badge ${u.active ? "" : "pending"}">${u.active ? "Active" : "Awaiting activation"}</span></td><td>${u.active ? '<a class="account-link" href="/admin/auth/user/' + u.id + '/change/">Manage account →</a>' : btn("Approve access", "approve-account", u.id)}</td></tr>`)), '<a class="button secondary" href="/admin/auth/user/">Manage accounts</a>')}
        </div><aside class="admin-aside">
        <section class="admin-station-focus"><span class="studio-kicker">CURRENT WORKSPACE</span><h2>${esc(selected?.name)}</h2><p>These controls apply to this station.</p></section>
        ${panel("Voting & publication", `<div class="admin-action-list"><button data-page="review"><strong>Review music <span>→</span></strong><small>Edit, verify, reject or merge songs.</small></button><button data-action="publish"><strong>Publish selected chart <span>→</span></strong><small>Publish a permanent Top 20 or Top 50.</small></button><button data-page="shows"><strong>Manage chart shows <span>→</span></strong><small>Choose the year-end chart show before publishing.</small></button><button data-page="shows"><strong>Review suspected spam <span>→</span></strong><small>Flagged votes across the selected station: ${num(selected?.spam || 0)}.</small></button><button data-action="station-multiplier"><strong>Station vote multiplier <span>→</span></strong><small>Configure weighted points for the selected station (×1–×10).</small></button><button data-page="archives"><strong>Chart archives <span>→</span></strong><small>Revisit and export saved editions.</small></button></div>`)}
        ${panel("Station operations", `<div class="admin-action-list"><button data-action="station-password"><strong>Station password <span>→</span></strong><small>Manage access when switching stations.</small></button><button data-page="connections"><strong>Connections & queues <span>→</span></strong><small>Check providers and retry failed jobs.</small></button><button data-page="activity"><strong>Activity history <span>→</span></strong><small>Review recorded administrative actions.</small></button></div>`)}
        </aside></div>`;
      } else if (page === "activity") {
        const d = await read("workspace/audit?page=" + pagination);
        html = panel(
          "Activity log",
          (d.items.length
            ? d.items
                .map(
                  (a) =>
                    `<div class="audit-item"><div><strong>${esc(a.action)}</strong><p>${esc(a.actor)}</p><p>${esc(a.details.after?.name || a.details.before?.name || (a.details.week_start ? "Week beginning " + a.details.week_start : a.details.count !== undefined ? a.details.count + " jobs" : "Station record updated"))}</p></div><time>${esc(new Date(a.created_at).toLocaleString())}</time></div>`,
                )
                .join("")
            : empty) + pager(d),
        );
      } else {
        const d = await read("workspace/health");
        html = panel(
          "Connections",
          `<div class="settings-section"><p>${esc(d.note)}</p><div class="settings-row"><strong>Background worker</strong><span>${d.worker_online ? "Online" : "No recent heartbeat"}</span></div>${d.providers.map((p) => `<div class="settings-row"><strong>${esc(p.name)}</strong><span>${p.configured ? "Configured" : "Not configured"}</span></div>`).join("")}<p>Daily limit: ${num(d.daily_limit)} · AI matching: ${d.ai_enabled ? "enabled" : "disabled"}</p></div><div class="settings-section"><h2>Processing queues</h2>${Object.entries(
            d.queues,
          )
            .map(
              ([name, counts]) =>
                `<p><strong>${esc(name)}</strong>: ${esc(
                  Object.entries(counts)
                    .map(([s, n]) => s + ": " + n)
                    .join(", ") || "Empty",
                )}</p>`,
            )
            .join(
              "",
            )}${d.can_retry ? '<a class="button secondary" href="/admin/auth/user/">Manage staff</a>' + btn("Set station password", "station-password") + btn("Retry failed jobs", "retry") : ""}</div>`,
        );
      }
      if (current !== generation) return;
      if (permissions.is_admin && ["activity", "connections"].includes(page)) {
        html = '<div class="workspace-tools"><button class="button secondary" data-page="administration">← Admin control</button></div>' + html;
      }
      if (["overview", "incoming", "archives"].includes(page) && !showsData.is_presenter) {
        html = '<div class="workspace-tools"><label>Chart show <select id="chart-show-select">' +
          showsCache.filter((show) => !show.is_archived || show.id === selectedShowId)
            .map((show) => '<option value="' + show.id + '"' + (show.id === selectedShowId ? ' selected' : '') + '>' + esc(show.name) + (show.is_archived ? " (archived)" : "") + '</option>').join("") +
          '</select></label></div>' + html;
      }
      $("page-content").innerHTML = html;
      if (focusState) {
        const nextField = document.getElementById(focusState.id);
        if (nextField) {
          nextField.focus({ preventScroll: true });
          if (focusState.start !== null) {
            try { nextField.setSelectionRange(focusState.start, focusState.end); }
            catch { /* Only text inputs expose selection. */ }
          }
        }
      }
      status(true);
      if (page === "overview") drawTimeline(overview.timeline);
      $("last-updated").textContent =
        "Updated " + new Date().toLocaleTimeString();
      $("today-label").textContent = overview.today;
    } catch (error) {
      if (current !== generation) return;
      if (error.name === "AbortError") return;
      status(false);
      $("offline-banner").textContent =
        error.message + " Previous results, if shown, have not been updated.";
      if ($("page-content").querySelector(".loading"))
        $("page-content").innerHTML =
          `<div class="empty">${esc(error.message)}<p>Reconnecting automatically…</p></div>`;
    } finally {
      if (current === generation) {
        $("page-content").setAttribute("aria-busy", "false");
        schedule();
      }
    }
  }
  function schedule() {
    clearTimeout(timer);
    timer = setTimeout(() => {
      const editing =
        document.activeElement?.matches("input, select, textarea") ||
        $("modal").open;
      if (
        !document.hidden &&
        !editing &&
        !$("station-dialog").open
      )
        render();
      else schedule();
    }, 15000);
  }
  function drawTimeline(days) {
    const canvas = $("timeline");
    if (!canvas) return;
    const ctx = canvas.getContext("2d"),
      max = Math.max(1, ...days.map((d) => d.count));
    days.forEach((d, i) => {
      const x = i * 72 + 16,
        h = (d.count / max) * 160;
      ctx.fillStyle = "#28634b";
      ctx.fillRect(x, 184 - h, 38, Math.max(2, h));
      ctx.fillStyle = "#52665a";
      ctx.font = "18px Segoe UI";
      ctx.fillText(
        new Date(d.date + "T12:00:00").toLocaleDateString("en", {
          weekday: "short",
        }),
        x,
        215,
      );
      ctx.fillText(String(d.count), x, 170 - h);
    });
    $("timeline-summary").textContent = days
      .map((d) => d.date + ": " + num(d.count))
      .join(" · ");
  }
  function toast(message) {
    clearTimeout(toastTimer);
    $("toast").textContent = message;
    $("toast").classList.remove("hidden");
    toastTimer = setTimeout(() => $("toast").classList.add("hidden"), 5000);
  }

  function setMobileNavigation(open, restoreFocus = false) {
    const toggle = $("menu-toggle");
    const sidebar = $("sidebar");
    const backdrop = $("sidebar-backdrop");
    const shouldOpen = open && window.matchMedia("(max-width: 700px)").matches;
    const wasOpen = sidebar.classList.contains("open");
    sidebar.classList.toggle("open", shouldOpen);
    backdrop.hidden = !shouldOpen;
    toggle.setAttribute("aria-expanded", String(shouldOpen));
    document.body.classList.toggle("nav-open", shouldOpen);
    document.documentElement.classList.toggle("nav-open", shouldOpen);
    if (!shouldOpen && wasOpen && restoreFocus && window.matchMedia("(max-width: 700px)").matches) {
      toggle.focus({ preventScroll: true });
    }
  }
  function navigate(next) {
    clearTimeout(liveSearchTimer);
    clearTimeout(eligibleSearchTimer);
    clearTimeout(proposalSearchTimer);
    eligibleSearchSequence++;
    proposalSearchSequence++;
    page = next;
    pagination = 1;
    query = "";
    generation++;
    $("page-title").textContent = titles[page];
    $("page-description").textContent = {
      overview:
        "Received votes, verified results and songs waiting for review.",
      incoming: "Listener submissions in the order they reached your station.",
      review: "Check artist names and titles before approving chart entries.",
      catalogue: "Search, edit and manage your station’s music.",
      archives: "Immutable published charts for the selected programme.",
      shows: "Programmes, voting codes, schedules and per-show settings.",
      stations: "Create stations, connect WhatsApp numbers and manage station settings.",
      eligibility: "Choose verified songs for each chart edition and publish eligible lists directly.",
      presenters: "Assign each presenter to exactly one station and chart show.",
      administration: "Manage accounts and oversee voting across every station.",
      activity: "A record of catalogue changes and chart publications.",
      connections: "Provider setup, background workers and processing queues.",
    }[page];
    document
      .querySelectorAll("[data-page]")
      .forEach((b) => {
        const active = b.dataset.page === page || (b.classList.contains("nav-item") && b.dataset.page === "administration" && ["activity", "connections"].includes(page));
        b.classList.toggle("active", active);
        if (b.classList.contains("nav-item")) {
          if (active) b.setAttribute("aria-current", "page");
          else b.removeAttribute("aria-current");
        }
      });
    setMobileNavigation(false);
    $("page-content").innerHTML = '<div class="loading">Loading…</div>';
    render();
  }
  function modal(title, fields, save) {
    $("modal-title").textContent = title;
    $("modal-content").innerHTML = fields;
    $("modal-error").textContent = "";
    $("modal-submit").hidden = !save;
    $("modal").showModal();
    $("modal-form").onsubmit = async (e) => {
      e.preventDefault();
      if (!online) return;
      $("modal-submit").disabled = true;
      try {
        await save(new FormData(e.target));
        $("modal").close();
        toast("Saved successfully.");
        await render();
      } catch (err) {
        $("modal-error").textContent = err.message;
      } finally {
        $("modal-submit").disabled = false;
      }
    };
  }
  const field = (label, name, value = "", type = "text") =>
    `<label class="field">${label}<input required maxlength="240" name="${name}" type="${type}" value="${esc(value)}"></label>`;

  // Use separate, repeatable fields for collaborators while preserving the
  // catalogue's backwards-compatible "Main Artist feat. Guest" representation.
  const featuredArtistRow = (value = "") =>
    '<div class="featured-artist-row">' +
    '<input name="featured_artists" aria-label="Featured artist" type="text" ' +
    'maxlength="240" autocomplete="off" placeholder="Featured artist name" value="' +
    esc(value) + '">' +
    '<button type="button" class="button secondary" data-action="featured-remove" ' +
    'aria-label="Remove featured artist">Remove</button></div>';
  const featuredArtistsFields = (value = "") => {
    const names = String(value || "").split(",").map((name) => name.trim()).filter(Boolean);
    return '<div class="field featured-artists-field"><span>Featured artists (optional)</span>' +
      '<div id="featured-artist-list" class="featured-artist-list">' +
      (names.length ? names.map(featuredArtistRow).join("") : featuredArtistRow()) +
      '</div><button type="button" class="button secondary featured-artist-add" ' +
      'data-action="featured-add">+ Add featured artist</button>' +
      '<small class="hint">Add each featured artist separately. AirVote combines ' +
      'the credits for display and AI vote matching.</small></div>';
  };
  const featuredArtistsPayload = (form) =>
    form.getAll("featured_artists").flatMap((value) => String(value).split(","))
      .map((value) => value.trim()).filter(Boolean).join(", ");
  function stationFormFields(station = {}) {
    const existing = Boolean(station.slug);
    const name = '<label class="field">Station name<input name="name" minlength="3" maxlength="100" required value="' +
      esc(station.name || "") + '" placeholder="e.g. Midlands Radio"></label>';
    const number = '<label class="field">Bird WhatsApp receiving number (optional during setup)' +
      '<input name="bird_number" inputmode="tel" maxlength="20" autocomplete="off" value="' +
      esc(station.bird_number || station.effective_bird_number || "") +
      '" placeholder="263790000000"></label>';
    const logo = '<label class="field">Station logo HTTPS image URL (optional)' +
      '<input name="logo_url" type="url" maxlength="500" value="' +
      esc(station.logo_url || "") + '" placeholder="https://example.org/station-logo.png"></label>';
    const status = existing
      ? showSelectField("Station status", "station_active",
        [["false", "Disabled — setup / maintenance"], ["true", "Active — accept configured votes"]],
        String(Boolean(station.is_active)))
      : '<p class="hint">New stations are initially disabled. AirVote will generate a permanent station ID. Configure its chart shows and Bird number before activation.</p>';
    return '<div class="network-station-form">' +
      (existing ? '<p class="network-permanent-id"><strong>Permanent ID:</strong> ' +
        esc(station.slug) + ' <small>(cannot be changed)</small></p>' : "") +
      name + number + logo + status +
      '<p class="hint">The Bird number identifies the station; voting codes then identify each chart show. A number already reserved for another station cannot be reassigned here. Existing Railway-configured numbers remain connected until that configuration is retired.</p>' +
      '</div>';
  }
  const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
  const showSelectField = (label, name, options, current) =>
    '<label class="field">' + esc(label) + '<select name="' + name + '">' +
    options.map((option) => '<option value="' + esc(option[0]) + '"' +
      (String(option[0]) === String(current) ? " selected" : "") + '>' +
      esc(option[1]) + '</option>').join("") +
    '</select></label>';
  function showFields(show = {}) {
    const weekdays = days.map((day, index) => [index, day]);
    const input = (label, name, value, type = "text", required = true) =>
      '<label class="field">' + esc(label) +
      '<input name="' + name + '" type="' + type + '" value="' + esc(value ?? "") + '"' +
      (name === "code" ? ' maxlength="16"' : name === "name" ? ' maxlength="120"' : "") +
      (type === "number" ? ' min="1" max="100"' : "") +
      (required ? " required" : "") + '></label>';
    return '<div class="show-form">' +
      '<p class="show-form-help">Programme codes must be unique within this station. Changes to a show do not move existing votes or rewrite saved charts.</p>' +
      '<fieldset class="show-form-section"><legend>Programme details</legend>' +
        '<div class="show-form-grid">' +
        input("Programme name", "name", show.name || "") +
        input("Voting code (2–16 letters or numbers)", "code", show.code || "") +
        input("Presenter (on-air name, optional)", "presenter_name", show.presenter_name || "", "text", false) +
        showSelectField("Chart type", "kind",
          [["weekly", "Regular weekly show"], ["year_end", "End-of-year Top 50"]], show.kind || "weekly") +
        showSelectField("Chart size", "chart_size",
          [[10, "Top 10"], [20, "Top 20"], [40, "Top 40"], [50, "Top 50"]], show.chart_size || 20) +
        input("Daily vote limit per listener", "daily_limit", show.daily_limit ?? 5, "number") +
        showSelectField("Voting schedule", "schedule_mode",
          [["weekly", "Repeating weekly"], ["manual", "Manual open / close"],
           ["campaign", "Fixed date range"]], show.schedule_mode || "weekly") +
        '</div>' +
      '</fieldset>' +
      '<fieldset class="show-form-section"><legend>Broadcast timetable (not voting hours)</legend>' +
        '<p class="show-form-caption">From the ZBC programme schedule. Do not use these times to open or close WhatsApp voting.</p>' +
        '<div class="show-form-grid">' +
        input("On-air start", "broadcast_start", show.broadcast_start || "", "time", false) +
        input("On-air end (optional)", "broadcast_end", show.broadcast_end || "", "time", false) +
        '</div></fieldset>' +
      '<fieldset class="show-form-section" data-show-mode="weekly">' +
        '<legend>Weekly voting hours</legend>' +
        '<p class="show-form-caption">Times use Zimbabwe local time.</p>' +
        '<div class="show-form-grid">' +
        showSelectField("Broadcast day", "broadcast_weekday", weekdays, show.broadcast_weekday ?? 5) +
        showSelectField("Opens on", "opens_weekday", weekdays, show.opens_weekday ?? 0) +
        input("Opening time", "opens_time", show.opens_time || "08:00", "time") +
        showSelectField("Closes on", "closes_weekday", weekdays, show.closes_weekday ?? 5) +
        input("Closing time", "closes_time", show.closes_time || "18:00", "time") +
        '</div>' +
      '</fieldset>' +
      '<fieldset class="show-form-section" data-show-mode="campaign">' +
        '<legend>Campaign voting dates</legend>' +
        '<p class="show-form-caption">Select the exact opening and closing dates, using Zimbabwe local time.</p>' +
        '<div class="show-form-grid">' +
        input("Campaign starts", "campaign_start",
          (show.campaign_start || "").slice(0, 16), "datetime-local", false) +
        input("Campaign ends", "campaign_end",
          (show.campaign_end || "").slice(0, 16), "datetime-local", false) +
        '</div>' +
      '</fieldset>' +
      '<fieldset class="show-form-section" data-show-mode="manual">' +
        '<legend>Manual voting</legend>' +
        '<label class="show-form-check"><input type="checkbox" name="manual_open" ' +
        (show.manual_open ? "checked" : "") + '> Allow votes while this programme is manually open</label>' +
      '</fieldset>' +
      '<fieldset class="show-form-section show-form-activation">' +
        '<legend>Programme availability</legend>' +
        (show.is_enabled ? '<label class="show-form-check">' +
          '<input type="checkbox" name="confirm_live_code_change">' +
          'I understand that changing the code while voting is open ' +
          'will require listeners to use the new code. Earlier votes will be preserved.' +
          '</label>' : "") +
        '<label class="show-form-check"><input type="checkbox" name="is_enabled" ' +
        (show.is_enabled ? "checked" : "") + '> Enable this programme to receive votes</label>' +
      '</fieldset>' +
      '</div>';
  }
  function syncShowFormVisibility() {
    const form = document.querySelector("#modal-content .show-form");
    if (!form) return;
    const kind = form.querySelector('[name="kind"]');
    const size = form.querySelector('[name="chart_size"]');
    const mode = form.querySelector('[name="schedule_mode"]');
    if (kind.value === "year_end") {
      size.value = "50";
      mode.value = "campaign";
    }
    Array.from(size.options).forEach((option) => {
      option.disabled = kind.value === "year_end" && option.value !== "50";
    });
    for (const option of mode.options) {
      option.disabled = kind.value === "year_end" && option.value !== "campaign";
    }
    form.querySelectorAll("[data-show-mode]").forEach((section) => {
      section.hidden = section.dataset.showMode !== mode.value;
    });
  }
  const showPayload = (fields, id = null) => ({
    id,
    name: fields.get("name"), code: fields.get("code").trim().toUpperCase(),
    presenter_name: fields.get("presenter_name") || "",
    broadcast_start: fields.get("broadcast_start") || "",
    broadcast_end: fields.get("broadcast_end") || "",
    kind: fields.get("kind"), schedule_mode: fields.get("schedule_mode"),
    chart_size: Number(fields.get("chart_size")),
    daily_limit: Number(fields.get("daily_limit")),
    broadcast_weekday: Number(fields.get("broadcast_weekday")),
    opens_weekday: Number(fields.get("opens_weekday")),
    closes_weekday: Number(fields.get("closes_weekday")),
    opens_time: fields.get("opens_time"), closes_time: fields.get("closes_time"),
    campaign_start: fields.get("campaign_start") || null,
    campaign_end: fields.get("campaign_end") || null,
    is_enabled: fields.has("is_enabled"), manual_open: fields.has("manual_open"),
    confirm_live_code_change: fields.has("confirm_live_code_change"),
  });
  document.addEventListener("click", async (e) => {
    const nav = e.target.closest("[data-page]");
    if (nav) {
      navigate(nav.dataset.page);
      return;
    }
    const b = e.target.closest("[data-action]");
    if (!b) return;
    const action = b.dataset.action,
      id = b.dataset.id;
    if (action === "featured-add") {
      const list = $("featured-artist-list");
      if (list) {
        list.insertAdjacentHTML("beforeend", featuredArtistRow());
        list.lastElementChild.querySelector("input")?.focus();
      }
      return;
    }
    if (action === "featured-remove") {
      b.closest(".featured-artist-row")?.remove();
      return;
    }
    if (action === "eligible-select") {
      selectedEditionId = Number(id);
      eligibilityResults = [];
      eligibilityQuery = "";
      eligibilitySearched = false;
      render();
      return;
    }
    if (action === "eligible-new") {
      const upcoming = eligibilityCreateDate || "";
      const weekday = days[eligibilityBroadcastDay] || "the broadcast day";
      modal("Create a chart edition",
        '<p class="eligible-modal-guide">Choose the broadcast date for <strong>' +
        esc(eligibilityShowName) + '</strong>. Weekly editions must fall on ' +
        esc(weekday) + '. You can prepare more eligible songs than the final chart positions.</p>' +
        field("Chart broadcast date", "chart_date", upcoming, "date"),
        async (fields) => {
          const result = await api("workspace/eligibility/create", {
            chart_show_id: selectedShowId, chart_date: fields.get("chart_date"),
          });
          selectedEditionId = result.edition.id;
          eligibilityQuery = "";
          eligibilityResults = [];
          eligibilitySearched = false;
        });
      return;
    }
    if (action === "eligible-find") {
      await findEligibleSongs();
      return;
    }
    if (action === "eligible-add" || action === "eligible-remove") {
      try {
        await api("workspace/eligibility/candidate", {
          edition_id: selectedEditionId,
          song_id: Number(id),
          action: action === "eligible-add" ? "add" : "remove",
        });
        await render();
      } catch (error) { toast(error.message); }
      return;
    }
    if (action === "eligible-suggest") {
      modal("Propose a new song",
        field("Main artist", "artist") + featuredArtistsFields() +
        field("Song title", "title") +
        '<label class="field">Release details (optional)<textarea name="notes" maxlength="500"></textarea></label>',
        (fields) => api("workspace/eligibility/suggest", {
          edition_id: selectedEditionId, artist: fields.get("artist"),
          featured_artists: featuredArtistsPayload(fields),
          title: fields.get("title"), notes: fields.get("notes") || "",
        }));
      return;
    }
    if (action === "eligible-submit") {
      modal("Publish eligible list",
        "<p>This makes the selected verified songs eligible for the programme. You can publish without administrator approval. Once published, the list cannot be changed during voting.</p>",
        () => api("workspace/eligibility/submit", {
          edition_id: selectedEditionId,
        }));
      return;
    }
    if (action === "eligible-proposal-accept" || action === "eligible-proposal-reject") {
      const accept = action === "eligible-proposal-accept";
      modal(accept ? "Link to a verified catalogue song" : "Reject proposed song",
        accept
          ? '<p class="eligible-modal-guide">Search for the artist or title, ' +
            'then choose the matching verified recording. If it is missing, ' +
            'verify it in Song catalogue first. Never link an unrelated recording.</p>' +
            '<label class="field">Search verified catalogue' +
            '<input type="search" id="eligible-proposal-query" autocomplete="off"' +
            ' maxlength="100" placeholder="Artist or song title"></label>' +
            '<button class="button secondary" type="button"' +
            ' data-action="eligible-proposal-find">Search verified songs</button>' +
            '<div class="eligible-proposal-matches" id="eligible-proposal-results">' +
            '<p>Search to choose a verified song.</p></div>' +
            '<p class="eligible-proposal-selected" id="eligible-proposal-selection">No song selected</p>' +
            '<input type="hidden" name="song_id" id="eligible-proposal-song-id">'
          : '<p class="eligible-modal-guide">The proposal will be marked rejected ' +
            'and retained in the review history.</p>',
        (fields) => {
          const songId = Number(fields.get("song_id"));
          if (accept && (!Number.isSafeInteger(songId) || songId <= 0))
            throw new Error("Search for and select a verified song before linking.");
          return api("workspace/eligibility/suggestion/review", {
            suggestion_id: Number(id), action: accept ? "accept" : "reject",
            ...(accept ? {song_id: songId} : {}),
          });
        });
      return;
    }
    if (action === "eligible-proposal-find") {
      clearTimeout(proposalSearchTimer);
      await findProposalMatches(++proposalSearchSequence);
      return;
    }
    if (action === "eligible-proposal-select") {
      const match = (window.eligibleProposalMatches || []).find(
        (item) => String(item.id) === String(id)
      );
      if (!match) return;
      document.getElementById("eligible-proposal-song-id").value = String(match.id);
      const selected = document.getElementById("eligible-proposal-selection");
      selected.textContent = "Selected: " + match.artist + " – " + match.title;
      selected.classList.add("is-selected");
      return;
    }
    if (action === "presenter-create") {
      const options = showsCache.filter((item) =>
        item.kind !== "legacy" && !item.is_archived
      ).map((item) => [item.id, item.name]);
      modal("Create presenter account",
        field("Presenter username", "username") +
        '<label class="field">Strong password<input name="password" type="password" required minlength="12" maxlength="128" autocomplete="new-password"></label>' +
        showSelectField("Assigned chart show", "chart_show_id", options, options[0]?.[0]) +
        '<p class="hint">The presenter will only access this station and programme. The password is saved securely as a hash.</p>',
        (fields) => api("workspace/presenters/create", {
          username: fields.get("username"), password: fields.get("password"),
          chart_show_id: Number(fields.get("chart_show_id")),
        }));
      return;
    }
    if (action === "presenter-assign" || action === "presenter-reassign") {
      const options = showsCache.filter((item) =>
        item.kind !== "legacy" && !item.is_archived
      ).map((item) => [item.id, item.name]);
      if (!options.length) { toast("Create a chart show first."); return; }
      modal("Assign presenter to programme",
        showSelectField("Chart show", "chart_show_id", options, options[0][0]) +
        '<p class="hint">Only the central administrator can change this assignment. The new programme takes effect on the next request.</p>',
        (fields) => api("workspace/presenters/assign", {
          user_id: Number(id), chart_show_id: Number(fields.get("chart_show_id")),
        }));
      return;
    }
    if (action === "show-select") {
      selectedShowId = Number(id);
      chartPeriod = "weekly";
      navigate("overview");
      return;
    }
    if (action === "show-new" || action === "show-edit" || action === "show-duplicate") {
      const source = showsCache.find((item) => item.id === Number(id));
      const edit = action === "show-edit";
      const initial = action === "show-new" ? {} :
        action === "show-duplicate"
          ? {...source, name: "Copy of " + source.name, code: "", is_enabled: false,
             manual_open: false}
          : source;
      if (edit && !source) return;
      modal(edit ? "Edit chart show" :
            action === "show-duplicate" ? "Duplicate chart show settings" : "Create chart show",
            showFields(initial),
            (fields) => api("workspace/shows/save",
              showPayload(fields, edit ? Number(id) : null)));
      syncShowFormVisibility();
      return;
    }
    if (action === "show-archive") {
      const show = showsCache.find((item) => item.id === Number(id));
      if (!show) return;
      modal("Archive " + show.name,
        "<p>This will stop new voting for the programme and hide it from the active list. All votes and published chart archives remain permanently intact.</p>",
        () => api("workspace/shows/archive", {id: Number(id)}));
      return;
    }
    if (action === "station-multiplier") {
      const options = Array.from({length: 10}, (_, i) =>
        '<option value="' + (i + 1) + '"' +
        (activeVoteMultiplier === i + 1 ? ' selected' : '') +
        '>×' + (i + 1) + (i === 0 ? ' — standard, no weighting' : '') +
        '</option>'
      ).join("");
      modal("Station vote multiplier",
        '<p class="hint">This applies to the station currently selected by the administrator, across all its chart shows. Actual received votes stay unchanged; chart scores are multiplied. Changing the setting updates live score displays, but not saved chart archives.</p>' +
        '<label class="field">Score multiplier<select name="multiplier" required>' + options + '</select></label>' +
        '<p class="hint">Example: 10 verified votes with ×2 produces 20 chart points, while the vote count remains 10.</p>',
        (fields) => api("workspace/shows/multiplier", {
          multiplier: Number(fields.get("multiplier")),
        })
      );
      return;
    }
    if (action === "show-policy") {
      modal(showPolicyEnabled ? "Disable mandatory voting codes" : "Enable mandatory voting codes",
        "<p>When codes are required, unrecognised messages will be held for review with no WhatsApp reply. Announce the voting codes before enabling this rule.</p>",
        () => api("workspace/shows/routing", {require_codes: !showPolicyEnabled}));
      return;
    }
    if (action === "show-dismiss") {
      const item = (window.studioUnassigned || []).find((row) => row.id === Number(id));
      if (!item) return;
      modal("Dismiss unassigned vote",
        "<p>This message will not count in any chart. The action will be recorded in the audit log.</p>",
        () => api("workspace/unassigned/dismiss", {id: Number(id)}));
      return;
    }
    if (action === "spam-release" || action === "spam-dismiss") {
      const item = (window.studioSpam || []).find((row) => row.id === Number(id));
      if (!item) return;
      const release = action === "spam-release";
      modal(release ? "Release flagged vote" : "Dismiss flagged vote",
        '<p class="hint"><strong>Reason:</strong> ' +
          esc(item.reason.replaceAll("_", " ")) +
          ' · <strong>Chart show:</strong> ' + esc(item.show) +
          '</p><p class="hint">' + esc(item.text) + '</p>' +
          '<p>' + (release
            ? "Confirm this submission is legitimate. It will then enter the normal duplicate-limit and AI verification pipeline. It is not automatically counted or approved."
            : "Dismiss this suspicious message without adding a vote. The decision will be recorded in the station audit log.") +
          '</p>',
        () => api("workspace/spam/review", {
          id: Number(id), action: release ? "release" : "dismiss",
        }));
      return;
    }
    if (action === "show-assign") {
      const item = (window.studioUnassigned || []).find((row) => row.id === Number(id));
      if (!item) return;
      const options = showsCache.filter((show) =>
        show.kind !== "legacy" && !show.is_archived
      ).map((show) => [show.id, show.name + " (" + show.code + ")"]);
      if (!options.length) { toast("Create a chart show first."); return; }
      modal("Assign unrecognised vote",
        '<p class="hint">Review the exact song before assigning. Only shows open when this message was received can accept it.</p>' +
        showSelectField("Chart show", "show_id", options, options[0][0]) +
        field("Song vote (remove incorrect code)", "vote_text", item.text),
        (fields) => api("workspace/unassigned/assign", {
          id: Number(id), show_id: Number(fields.get("show_id")),
          vote_text: fields.get("vote_text"),
        }));
      return;
    }
    if (action === "next" || action === "previous") {
      pagination += action === "next" ? 1 : -1;
      render();
      return;
    }
    if (!online) return;
    if (action === "archive") {
      try {
        const d = await api("chart/" + id);
        modal(
          (d.chart.is_year_end ? "Year-end Top 50" : "Top " + d.chart.chart_size) + " · " + (d.chart.chart_date || d.chart.week_end),
          `<p class="hint">Voting period: ${esc(d.chart.week_start)} to ${esc(d.chart.week_end)}. Saved ${esc(new Date(d.chart.finalized_at).toLocaleString())}.</p>` +
            chartMovementSummary(d.entries, d.chart.comparison) +
            chart(d.entries, d.chart.comparison) +
            `<a class="button secondary" href="/api/workspace/export?archive=${id}">Export CSV</a>`,
          null,
        );
      } catch (err) {
        toast(err.message);
      }
      return;
    }
    if (action === "network-manage") {
      navigate("stations");
      return;
    }
    if (action === "network-station-new") {
      modal("Create a radio station", stationFormFields(),
        async (fields) => {
          const result = await api("workspace/stations/create", {
            name: fields.get("name"),
            bird_number: fields.get("bird_number") || "",
            logo_url: fields.get("logo_url") || "",
          });
          const select = $("station-select");
          if (select && result.station && !Array.from(select.options).some(
            (option) => option.value === result.station.slug
          )) {
            select.add(new Option(result.station.name, result.station.slug));
          }
        });
      return;
    }
    if (action === "network-station-edit") {
      const station = (window.airvoteNetworkStations || []).find(
        (item) => item.slug === id
      );
      if (!station) { toast("Station is no longer available."); return; }
      modal("Configure " + station.name, stationFormFields(station),
        async (fields) => {
          const result = await api("workspace/stations/update", {
            slug: station.slug, name: fields.get("name"),
            bird_number: fields.get("bird_number") || "",
            logo_url: fields.get("logo_url") || "",
            is_active: fields.get("station_active") === "true",
          });
          const option = Array.from($("station-select")?.options || []).find(
            (item) => item.value === result.station.slug
          );
          if (option) option.textContent = result.station.name;
          if (result.station.slug === $("station-select")?.value) {
            const heading = document.querySelector(".station strong");
            if (heading) heading.textContent = result.station.name;
          }
        });
      return;
    }
    if (action === "admin-station") {
      $("station-select").value = id;
      $("station-select").dispatchEvent(new Event("change"));
      return;
    }
    if (action === "go-review") { navigate("review"); return; }
    if (action === "approve-account") {
      modal("Approve station access", "<p>Allow this account to sign in and view its assigned station? Song moderation remains administrator-only.</p>", () => api(`workspace/accounts/${id}/approve`, {}));
      return;
    }
    if (action === "publish" || action === "publish-year") {
      const show = showsCache.find((item) => item.id === selectedShowId);
      const annual = action === "publish-year" || show?.kind === "year_end";
      modal(
        annual ? "Save December Top 50" : show?.kind === "legacy" ? "Save Saturday chart" : "Publish chart show",
        field(annual ? "Publication date (December)" : "Chart broadcast date", "chart_date", "", "date") +
          (annual ? '<input type="hidden" name="size" value="50">' :
            show?.kind === "legacy"
              ? '<label class="field">Chart size<select name="size"><option>20</option><option>50</option></select></label>'
              : '<input type="hidden" name="size" value="' + show.chart_size + '"><p class="hint">Configured chart size: Top ' + show.chart_size + '</p>') +
          `<p class="hint">${annual ? "Counts verified votes received during the configured annual campaign, up to the selected publication date." : show?.kind === "legacy" ? "Counts verified votes from Monday through Sunday." : "Counts eligible verified votes in the seven days leading up to this show's broadcast date."} Finish reviewing and processing votes first. Published results cannot be rewritten.</p>`,
        (f) => api("workspace/publish", {...Object.fromEntries(f), show_id: selectedShowId, kind: annual ? "year_end" : "weekly"}),
      );
      return;
    }
    if (action === "record-vote") {
      const requestId = crypto.randomUUID();
      modal("Record a listener vote",
        field("Listener reference", "listener") + field("Artist - Song", "text") +
        '<p class="hint">Use the same reference for the same listener. Daily limits and duplicate rules apply. This creates a real manual vote for the selected station. New songs need review before charting.</p>',
        f => api("workspace/votes", {...Object.fromEntries(f), show_id: selectedShowId, request_id: requestId}));
      return;
    }
    if (action === "station-password") {
      modal("Set password for this station",
        field("New station password (12–128 characters)", "password", "", "password") +
        '<p class="hint">Share it only with authorised station staff. Changing it revokes their existing switches to this station. Administrators do not need this password.</p>',
        f => api("workspace/station-password", Object.fromEntries(f)));
      return;
    }
    if (action === "retry") {
      modal(
        "Retry failed jobs",
        "<p>Retry failed intake, matching and reply jobs. Replies with unknown delivery outcomes are excluded.</p>",
        () => api("workspace/retry", {}),
      );
      return;
    }
    const song = (window.studioSongs || []).find((s) => s.id === Number(id));
    if (action === "edit" || action === "add") {
      modal(
        action === "add" ? "Add song" : "Edit song",
        field("Main artist", "artist", song?.primary_artist ?? song?.artist) +
          featuredArtistsFields(song?.featured_artists) +
          field("Song title", "title", song?.title),
        (f) =>
          api(
            action === "add"
              ? "workspace/songs/add"
              : `workspace/songs/${id}/review`,
            { ...Object.fromEntries(f), featured_artists: featuredArtistsPayload(f),
              action: "edit" },
          ),
      );
      return;
    }
    if (action === "merge") {
      modal(
        "Merge into a verified song",
        '<p class="hint">Search for the correct song. Its votes will be combined with this entry.</p><label class="field">Search artist or title<input id="merge-search" autocomplete="off" placeholder="Start typing an artist or song"></label><label class="field">Verified song<select required name="target_id" id="merge-target"><option value="">Search to choose a song</option></select></label>',
        (f) =>
          api(`workspace/songs/${id}/review`, {
            action,
            target_id: Number(f.get("target_id")),
          }),
      );
      let sequence = 0,
        debounce;
      $("merge-search").oninput = () => {
        clearTimeout(debounce);
        const seq = ++sequence;
        const typed = $("merge-search").value.trim();
        if (!typed) {
          $("merge-target").innerHTML = '<option value="">Start typing to find a song</option>';
          return;
        }
        $("merge-target").innerHTML = '<option value="">Searching…</option>';
        debounce = setTimeout(async () => {
          try {
            const d = await api(
              "workspace/songs?status=verified&q=" +
                encodeURIComponent(typed),
            );
            if (seq !== sequence || !$("modal").open || !$("merge-search") ||
                $("merge-search").value.trim() !== typed) return;
            const options = d.items.filter((r) => r.id !== Number(id));
            $("merge-target").innerHTML =
              '<option value="">' +
              (options.length ? "Choose a song" : "No verified matches") +
              "</option>" +
              options
                .map(
                  (r) =>
                    `<option value="${r.id}">${esc(r.artist)} — ${esc(r.title)}</option>`,
                )
                .join("");
          } catch (err) {
            if ($("modal").open) $("modal-error").textContent = err.message;
          }
        }, 300);
      };
      return;
    }
    modal(
      action === "verified" ? "Verify song" : "Reject song",
      `<p>${esc(song?.artist)} – ${esc(song?.title)}</p><p>This updates the current totals and creates an audit record.</p>`,
      () => api(`workspace/songs/${id}/review`, { action }),
    );
  });
  document.addEventListener("change", (e) => {
    if (e.target.closest("#modal-content .show-form") &&
        ["kind", "schedule_mode"].includes(e.target.name)) {
      syncShowFormVisibility();
    }
    if (e.target.id === "eligibility-show-select") {
      selectedShowId = Number(e.target.value);
      selectedEditionId = null;
      eligibilityResults = [];
      eligibilityQuery = "";
      eligibilitySearched = false;
      render();
      return;
    }
    if (e.target.id === "chart-show-select") {
      selectedShowId = Number(e.target.value);
      chartPeriod = "weekly";
      pagination = 1;
      render();
    }
  });
  async function findEligibleSongs(searchSequence = ++eligibleSearchSequence) {
    const input = document.getElementById("eligible-search-input");
    if (!input || page !== "eligibility") return;
    const text = input.value;
    const q = text.trim();
    eligibilityQuery = text;
    if (!q) {
      eligibilityResults = [];
      eligibilitySearched = false;
      await render();
      return;
    }
    try {
      const result = await api("workspace/songs?status=verified&q=" +
        encodeURIComponent(q) + "&page=1");
      // An older request must never overwrite more recently typed text.
      if (searchSequence !== eligibleSearchSequence || !input.isConnected ||
          page !== "eligibility" || input.value !== text) return;
      eligibilityResults = result.items || [];
      eligibilitySearched = true;
      await render();
    } catch (error) {
      if (searchSequence === eligibleSearchSequence && input.isConnected)
        toast(error.message);
    }
  }

  async function findProposalMatches(searchSequence = ++proposalSearchSequence) {
    const input = document.getElementById("eligible-proposal-query");
    const results = document.getElementById("eligible-proposal-results");
    if (!input || !results || !$("modal").open) return;
    const text = input.value.trim();
    window.eligibleProposalMatches = [];
    if (!text) {
      results.innerHTML = "<p>Start typing to find a verified song.</p>";
      return;
    }
    results.innerHTML = "<p>Searching verified songs…</p>";
    try {
      const d = await api("workspace/songs?status=verified&q=" +
        encodeURIComponent(text) + "&page=1");
      if (searchSequence !== proposalSearchSequence || !input.isConnected ||
          !$("modal").open || input.value.trim() !== text) return;
      const songs = d.items || [];
      window.eligibleProposalMatches = songs;
      results.innerHTML = songs.length
        ? songs.map((item) =>
            '<div class="eligible-proposal-option"><span><strong>' +
            esc(item.title) + '</strong><small>' + esc(item.artist) +
            '</small></span><button class="button secondary" type="button"' +
            ' data-action="eligible-proposal-select" data-id="' +
            Number(item.id) + '">Choose</button></div>').join("")
        : "<p>No verified catalogue match. Verify the song in Song catalogue first.</p>";
    } catch (error) {
      if (searchSequence === proposalSearchSequence && input.isConnected &&
          $("modal").open)
        results.textContent = error.message;
    }
  }
  // Debounce requests to protect Railway during radio show peaks. All of
  // these inputs work automatically; the forms remain for keyboard fallback.
  document.addEventListener("input", (event) => {
    const input = event.target;
    if (input.id === "live-record-search" &&
        ["incoming", "catalogue", "review"].includes(page)) {
      query = input.value;
      pagination = 1;
      generation++;
      reader?.abort();
      clearTimeout(liveSearchTimer);
      liveSearchTimer = setTimeout(() => {
        if (input.isConnected && $("live-record-search") === input)
          render();
      }, 260);
      return;
    }
    if (input.id === "archive-year-search" && page === "archives") {
      query = input.value;
      generation++;
      reader?.abort();
      clearTimeout(liveSearchTimer);
      // An incomplete year is not a meaningful chart filter; wait for 4 digits.
      if (!query || /^\d{4}$/.test(query)) {
        liveSearchTimer = setTimeout(() => {
          if (input.isConnected && $("archive-year-search") === input)
            render();
        }, 260);
      }
      return;
    }
    if (input.id === "eligible-search-input" && page === "eligibility") {
      eligibilityQuery = input.value;
      generation++;
      reader?.abort();
      const seq = ++eligibleSearchSequence;
      clearTimeout(eligibleSearchTimer);
      eligibleSearchTimer = setTimeout(() => {
        if (input.isConnected && $("eligible-search-input") === input)
          findEligibleSongs(seq);
      }, 260);
      return;
    }
    if (input.id === "eligible-proposal-query") {
      const seq = ++proposalSearchSequence;
      clearTimeout(proposalSearchTimer);
      const chosen = $("eligible-proposal-song-id");
      if (chosen) chosen.value = "";
      const description = $("eligible-proposal-selection");
      if (description) {
        description.textContent = "No song selected";
        description.classList.remove("is-selected");
      }
      proposalSearchTimer = setTimeout(() => {
        if (input.isConnected && $("eligible-proposal-query") === input)
          findProposalMatches(seq);
      }, 260);
    }
  });

  document.addEventListener("submit", (e) => {
    if (e.target.id === "eligible-search-form") {
      e.preventDefault();
      clearTimeout(eligibleSearchTimer);
      findEligibleSongs(++eligibleSearchSequence);
      return;
    }
    if (e.target.id === "chart-options") {
      e.preventDefault();
      const values = new FormData(e.target);
      chartSize = Number(values.get("size"));
      chartPeriod = values.get("period");
      render();
    }
    if (e.target.id === "search-form") {
      e.preventDefault();
      clearTimeout(liveSearchTimer);
      query = new FormData(e.target).get("q");
      if (page === "archives" && query && !/^\d{4}$/.test(query)) {
        toast("Enter a complete four-digit year.");
        return;
      }
      pagination = 1;
      render();
    }
  });
  const stationForm = $("station-switch-form");
  if (stationForm) {
    $("station-select").addEventListener("change", () => {
      const selected = $("station-select").value;
      if (selected === stationForm.dataset.current) return;
      if (stationForm.dataset.admin === "true") { stationForm.requestSubmit(); return; }
      $("destination-station").value = selected;
      $("station-destination-label").textContent = "Enter the password for " + $("station-select").selectedOptions[0].textContent + ".";
      $("station-password").value = "";
      $("station-dialog").showModal();
      $("station-password").focus();
    });
    $("station-dialog").addEventListener("close", () => { $("station-select").value = stationForm.dataset.current; });
    $("cancel-station").onclick = () => $("station-dialog").close();
  }
  $("menu-toggle").onclick = () => {
    setMobileNavigation(!$("sidebar").classList.contains("open"));
    if ($("sidebar").classList.contains("open")) $("sidebar-close").focus({ preventScroll: true });
  };
  $("sidebar-close").onclick = () => setMobileNavigation(false, true);
  $("sidebar-backdrop").onclick = () => setMobileNavigation(false, true);
  $("close-modal").onclick = $("cancel-modal").onclick = () =>
    $("modal").close();
  document.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && e.target.id === "eligible-proposal-query") {
      e.preventDefault();
      clearTimeout(proposalSearchTimer);
      findProposalMatches(++proposalSearchSequence);
      return;
    }
    if (e.key === "Enter" && e.target.id === "merge-search") {
      // Enter must never submit an unselected song-merging decision.
      e.preventDefault();
      return;
    }
    if (e.key === "Escape" && $("sidebar").classList.contains("open")) {
      setMobileNavigation(false, true);
    }
  });
  window.addEventListener("resize", () => {
    if (window.innerWidth > 700 && $("sidebar").classList.contains("open")) {
      setMobileNavigation(false);
    }
  });
  window.addEventListener("online", render);
  window.addEventListener("offline", () => status(false));
  render();
})();

