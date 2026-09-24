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
  let page = "overview",
    pagination = 1,
    query = "",
    generation = 0,
    online = false,
    permissions = {},
    timer;
  const titles = {
    overview: "Weekly overview",
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
    $("refresh").disabled = true;
    $("page-content").setAttribute("aria-busy", "true");
    clearTimeout(timer);
    try {
      let html = "";
      const overview = await read("workspace/overview");
      if (current !== generation) return;
      permissions = overview;
      $("nav-pending").textContent = num(overview.pending_songs);
      if (page === "overview") {
        const data = await read("chart/today?limit=100");
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
            "Live weekly chart",
            chart(data.top100),
            '<a class="button secondary" href="/api/workspace/export?limit=100">Export CSV</a>',
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
          !incoming && permissions.can_add ? btn("Add song", "add") : "",
        );
        window.studioSongs = d.items;
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
                  ["Week", "Dates", "Votes", "Open"],
                  d.charts.map(
                    (c) =>
                      `<tr><td>${c.week_number}</td><td>${esc(c.week_start)} – ${esc(c.week_end)}</td><td>${num(c.total_votes)}</td><td>${btn("View chart", "archive", c.id)}</td></tr>`,
                  ),
                )
              : empty),
          permissions.can_publish
            ? btn("Publish completed week", "publish")
            : "",
        );
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
            )}${d.can_retry ? btn("Retry failed jobs", "retry") : ""}</div>`,
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
          `<div class="empty">${esc(error.message)}<p>Use Refresh to reconnect.</p></div>`;
    } finally {
      if (current === generation) {
        $("refresh").disabled = false;
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
        ["overview", "incoming", "connections"].includes(page)
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
      archives: "Completed weekly charts, preserved as published.",
      activity: "A record of catalogue changes and chart publications.",
      connections: "Provider setup, background workers and processing queues.",
    }[page];
    $("breadcrumb-page").textContent = page;
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
          "Archived chart",
          chart(d.entries) +
            `<a class="button secondary" href="/api/workspace/export?archive=${id}">Export CSV</a>`,
          null,
        );
      } catch (err) {
        toast(err.message);
      }
      return;
    }
    if (action === "publish") {
      modal(
        "Publish a completed week",
        field("Week beginning (Monday)", "week_start", "", "date") +
          '<label class="field">Chart size<select name="size"><option>20</option><option>50</option><option>100</option></select></label><p class="hint">This saves a permanent snapshot of verified votes.</p>',
        (f) => api("workspace/publish", Object.fromEntries(f)),
      );
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
    if (e.target.id === "search-form") {
      e.preventDefault();
      query = new FormData(e.target).get("q");
      pagination = 1;
      render();
    }
  });
  $("refresh").onclick = render;
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
