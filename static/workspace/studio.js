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
  let chartSize = 20, chartPeriod = "weekly";
  let page = "overview",
    pagination = 1,
    query = "",
    generation = 0,
    online = false,
    permissions = {},
    timer;
  const titles = {
    overview: "Voting overview",
    administration: "Admin control",
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
  function chart(rows) {
    return rows.length
      ? table(
          ["Rank", "Song", "Votes", "Movement"],
          rows.map(
            (r) =>
              `<tr><td>${r.rank}</td><td><strong>${esc(r.title)}</strong><br><small>${esc(r.artists)}</small></td><td class="vote-count">${num(r.count)}</td><td>${esc(r.movement)}</td></tr>`,
          ),
        )
      : empty;
  }
  const pager = (d) =>
    `<div class="table-footer"><span>${num(d.total)} records</span><div class="pagination">${d.page > 1 ? btn("Previous", "previous") : ""}<span>${d.page} / ${d.pages}</span>${d.page < d.pages ? btn("Next", "next") : ""}</div></div>`;
  const search = () =>
    `<form id="search-form" class="toolbar"><label class="searchbox"><input name="q" aria-label="Search records" placeholder="Search…" value="${esc(query)}"></label><button class="button secondary">Search</button></form>`;
  function status(ok) {
    online = ok;
    $("connection").classList.toggle("offline", !ok);
    $("connection").lastElementChild.textContent = ok ? "Connected" : "Offline";
    $("offline-banner").classList.toggle("hidden", ok);
  }
  async function render() {
    const current = ++generation;
    reader?.abort();
    reader = new AbortController();
    const read = (path) => api(path, undefined, reader.signal);
    $("page-content").setAttribute("aria-busy", "true");
    clearTimeout(timer);
    try {
      let html = "";
      const overview = await read("workspace/overview");
      if (current !== generation) return;
      permissions = overview;
      $("nav-pending").textContent = num(overview.pending_songs);
      if (page === "overview") {
        const data = await read(`chart/today?limit=${chartSize}&period=${chartPeriod}`);
        html = `<div class="metrics">${[
          ["Received this week", overview.received],
          ["Verified votes", overview.verified_votes],
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
            `<form id="chart-options" class="toolbar"><label>Period <select name="period"><option value="weekly" ${chartPeriod === "weekly" ? "selected" : ""}>Sunday–Saturday</option><option value="year_end" ${chartPeriod === "year_end" ? "selected" : ""}>Year to date</option></select></label><label>Chart size <select name="size"><option value="20" ${chartSize === 20 ? "selected" : ""}>Top 20</option><option value="50" ${chartSize === 50 ? "selected" : ""}>Top 50</option></select></label><button class="button secondary">Show chart</button></form><p class="hint">${esc(data.week_start)} to ${esc(data.week_end)} · Verified votes only</p>` + chart(data.top100),
            `<a class="button secondary" href="/api/workspace/export?limit=${chartSize}&period=${chartPeriod}">Export CSV</a>`,
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
          `workspace/${incoming ? "incoming" : "songs"}?status=${page === "review" ? "pending" : "all"}&page=${pagination}&q=${encodeURIComponent(query)}`,
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
      } else if (page === "archives") {
        const d = await read(
          "chart/archives" +
            (query ? "?year=" + encodeURIComponent(query) : ""),
        );
        html = panel(
          "Chart archives",
          `<form id="search-form" class="toolbar"><label>Year <input name="q" type="number" min="1900" max="9999" value="${esc(d.year)}"></label><button class="button secondary">Load</button></form>` +
            (d.charts.length
              ? table(
                  ["Edition", "Voting period", "Votes", "Open"],
                  d.charts.map(
                    (c) =>
                      `<tr><td>${c.is_year_end ? "Year-end Top 50" : "Top " + c.chart_size + " · " + (c.chart_date || "Week " + c.week_number)}</td><td>${esc(c.week_start)} – ${esc(c.week_end)}</td><td>${num(c.total_votes)}</td><td>${btn("View chart", "archive", c.id)}</td></tr>`,
                  ),
                )
              : empty),
          permissions.can_publish
            ? btn("Save Saturday chart", "publish") + btn("Save December Top 50", "publish-year")
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
        ${panel("Station directory", `<div class="station-directory">${d.stations.map(station => `<article class="station-tile ${station.id === overview.station ? "selected" : ""}"><div class="station-tile-heading"><span class="station-monogram">${esc(station.name.split(" ").map(w=>w[0]).join("").slice(0,2))}</span><div><h3>${esc(station.name)}</h3><small>${station.id === overview.station ? "Current workspace" : "Independent station workspace"}</small></div></div><div class="station-tile-stats"><span><strong>${num(station.votes)}</strong> received votes</span><span><strong>${num(station.pending)}</strong> to review</span></div>${station.id === overview.station ? '<span class="access-badge">Selected</span>' : btn("Open station", "admin-station", station.id)}</article>`).join("")}</div>`)}
        ${panel("Accounts & access", `<div class="admin-section-intro"><p>Approve access for the station shown. Account management includes station assignments, password resets and deactivation.</p></div>` + table(["Team member", "Assigned station", "Access", "Action"], d.accounts.map(u => `<tr><td><strong>${esc(u.name || u.username)}</strong><br><small>@${esc(u.username)}</small></td><td>${esc(u.station)}</td><td><span class="access-badge ${u.active ? "" : "pending"}">${u.active ? "Active" : "Awaiting activation"}</span></td><td>${u.active ? '<a class="account-link" href="/admin/auth/user/' + u.id + '/change/">Manage account →</a>' : btn("Approve access", "approve-account", u.id)}</td></tr>`)), '<a class="button secondary" href="/admin/auth/user/">Manage accounts</a>')}
        </div><aside class="admin-aside">
        <section class="admin-station-focus"><span class="studio-kicker">CURRENT WORKSPACE</span><h2>${esc(selected?.name)}</h2><p>These controls apply to this station.</p></section>
        ${panel("Voting & publication", `<div class="admin-action-list"><button data-page="review"><strong>Review music <span>→</span></strong><small>Edit, verify, reject or merge songs.</small></button><button data-action="publish"><strong>Save Saturday chart <span>→</span></strong><small>Publish a permanent Top 20 or Top 50.</small></button><button data-action="publish-year"><strong>December Top 50 <span>→</span></strong><small>Save the year-end countdown.</small></button><button data-page="archives"><strong>Chart archives <span>→</span></strong><small>Revisit and export saved editions.</small></button></div>`)}
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
      $("page-content").innerHTML = html;
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
  function navigate(next) {
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
      archives: "Saturday and December charts, preserved as published.",
      administration: "Manage accounts and oversee voting across every station.",
      activity: "A record of catalogue changes and chart publications.",
      connections: "Provider setup, background workers and processing queues.",
    }[page];
    document
      .querySelectorAll("[data-page]")
      .forEach((b) => b.classList.toggle("active", b.dataset.page === page));
    $("sidebar").classList.remove("open");
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
          `<p class="hint">Voting period: ${esc(d.chart.week_start)} to ${esc(d.chart.week_end)}. Saved ${esc(new Date(d.chart.finalized_at).toLocaleString())}.</p>` + chart(d.entries) +
            `<a class="button secondary" href="/api/workspace/export?archive=${id}">Export CSV</a>`,
          null,
        );
      } catch (err) {
        toast(err.message);
      }
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
      const annual = action === "publish-year";
      modal(
        annual ? "Save December Top 50" : "Save Saturday chart",
        field(annual ? "Publication date (December)" : "Chart date (Saturday)", "chart_date", "", "date") +
          (annual ? '<input type="hidden" name="size" value="50">' : '<label class="field">Chart size<select name="size"><option>20</option><option>50</option></select></label>') +
          `<p class="hint">${annual ? "Counts verified votes from January 1 through the chosen December date." : "Counts verified votes from Sunday through the chosen Saturday."} Saving today includes votes processed so far. Finish reviewing and processing votes first. This permanent snapshot cannot be overwritten; later votes and edits will not change it.</p>`,
        (f) => api("workspace/publish", {...Object.fromEntries(f), kind: annual ? "year_end" : "weekly"}),
      );
      return;
    }
    if (action === "record-vote") {
      const requestId = crypto.randomUUID();
      modal("Record a listener vote",
        field("Listener reference", "listener") + field("Artist - Song", "text") +
        '<p class="hint">Use the same reference for the same listener. Daily limits and duplicate rules apply. This creates a real manual vote for the selected station. New songs need review before charting.</p>',
        f => api("workspace/votes", {...Object.fromEntries(f), request_id: requestId}));
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
        field("Artist", "artist", song?.artist) +
          field("Title", "title", song?.title),
        (f) =>
          api(
            action === "add"
              ? "workspace/songs/add"
              : `workspace/songs/${id}/review`,
            { ...Object.fromEntries(f), action: "edit" },
          ),
      );
      return;
    }
    if (action === "merge") {
      modal(
        "Merge into a verified song",
        '<p class="hint">Search for the correct song. Its votes will be combined with this entry.</p><label class="field">Search artist or title<input id="merge-search" autocomplete="off"></label><label class="field">Verified song<select required name="target_id" id="merge-target"><option value="">Search to choose a song</option></select></label>',
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
        $("merge-target").innerHTML = '<option value="">Searching…</option>';
        debounce = setTimeout(async () => {
          try {
            const d = await api(
              "workspace/songs?status=verified&q=" +
                encodeURIComponent($("merge-search").value),
            );
            if (seq !== sequence || !$("modal").open) return;
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
  document.addEventListener("submit", (e) => {
    if (e.target.id === "chart-options") {
      e.preventDefault();
      const values = new FormData(e.target);
      chartSize = Number(values.get("size"));
      chartPeriod = values.get("period");
      render();
    }
    if (e.target.id === "search-form") {
      e.preventDefault();
      query = new FormData(e.target).get("q");
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
  $("menu-toggle").onclick = () => $("sidebar").classList.toggle("open");
  $("close-modal").onclick = $("cancel-modal").onclick = () =>
    $("modal").close();
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") $("sidebar").classList.remove("open");
  });
  document.addEventListener("click", (e) => {
    if (!e.target.closest("#sidebar, #menu-toggle"))
      $("sidebar").classList.remove("open");
  });
  window.addEventListener("online", render);
  window.addEventListener("offline", () => status(false));
  render();
})();
