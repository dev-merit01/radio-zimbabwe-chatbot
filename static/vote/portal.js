/* AirVote public portal. No staff routes, tokens or API credentials in browser code. */
(() => {
  "use strict";
  const $ = (id) => document.getElementById(id);
  const root = $("portal");
  const enabled = root.dataset.enabled === "true";
  const csrf = document.querySelector('[name="csrfmiddlewaretoken"]')?.value || "";
  const state = {stations: [], station: null, show: null, selected: null, detail: null, verified: false};
  const escapeHTML = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  })[ch]);
  const message = (text, kind = "") => {
    $("action-message").textContent = text;
    $("action-message").className = "action-message " + kind;
  };
  const showStatus = (text, open) => {
    $("voting-status").hidden = false;
    $("voting-status").textContent = text;
    $("voting-status").className = "status-badge" + (open ? "" : " closed");
  };
  async function get(url) {
    const response = await fetch(url, {credentials:"same-origin", cache:"no-store"});
    if (!response.ok) throw new Error("This information is temporarily unavailable.");
    return response.json();
  }
  async function post(url, data) {
    const response = await fetch(url, {
      method: "POST", cache: "no-store", credentials: "same-origin",
      headers: {"X-CSRFToken": csrf, "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"},
      body: new URLSearchParams(data),
    });
    let payload;
    try {payload = await response.json();} catch {throw new Error("The server is temporarily unavailable.");}
    if (!response.ok) throw new Error(payload.error || "Your request could not be completed.");
    return payload;
  }
  const turnstileToken = (container) =>
    $(container)?.querySelector('[name="cf-turnstile-response"]')?.value || "";
  function resetTurnstile(container) {
    try {window.turnstile?.reset("#" + container);} catch { /* The widget may still be loading. */ }
  }
  function setBusy(form, busy) {
    form.querySelectorAll("button").forEach(btn => {btn.disabled = busy;});
  }
  function updateVoteButton() {
    const open = enabled && Boolean(state.detail?.voting_open);
    $("voting-controls").hidden = !open;
    $("voting-action-notice").hidden = open;
    $("vote-button").disabled = !open || !state.selected || !state.verified;
    $("send-code").disabled = !open || !state.selected || state.verified;
    if (!open) {
      const detail = state.detail;
      const reason = detail?.voting_blockers?.[0]?.message ||
        "Choose a chart show to see whether online voting is available.";
      $("voting-action-notice").textContent = reason;
    }
  }
  function renderAvailability() {
    const detail = state.detail;
    const panel = $("vote-availability");
    const blockers = detail?.voting_blockers || [];
    panel.hidden = !detail || !blockers.length;
    $("vote-availability-title").textContent = detail?.voting_status_label || "";
    $("vote-availability-reasons").innerHTML = blockers.map(blocker =>
      "<li>" + escapeHTML(blocker.message) + "</li>").join("");
    $("vote-schedule").textContent = detail
      ? (detail.show_scheduled_open ? "Configured voting hours: open. " :
          "Configured voting hours: closed. ") +
        (detail.schedule_label || "") : "";
  }
  function renderStations() {
    $("station-count").textContent = state.stations.length + " stations";
    $("station-list").innerHTML = state.stations.length
      ? state.stations.map((station) =>
        '<button type="button" class="station-card' + (state.station?.slug === station.slug ? ' selected' : '') +
        '" data-station="' + escapeHTML(station.slug) + '"><span class="station-card-top">' +
        '<span class="station-avatar">' +
        (station.logo ? '<img src="' + escapeHTML(station.logo) + '" alt="" loading="lazy">' :
          escapeHTML(station.name.split(" ").map(x=>x[0]).join("").slice(0,2))) +
        '</span><span><strong>' + escapeHTML(station.name) + '</strong><small>Radio station</small></span></span>' +
        '<span class="station-card-foot">' + station.shows.length + ' chart show(s) <span aria-hidden="true">→</span></span></button>'
      ).join("")
      : '<div class="empty-state">No stations are available yet.</div>';
  }
  function renderShows() {
    const selected = state.station;
    $("show-instruction").textContent = selected
      ? "Choose a chart show on " + selected.name + "."
      : "Choose a station above to see its programmes.";
    $("show-list").innerHTML = selected
      ? selected.shows.map((show) =>
        '<button type="button" class="show-card' + (state.show?.id === show.id ? ' selected' : '') +
        '" data-show="' + show.id + '"><small>' + escapeHTML(show.code || "CHART") + '</small>' +
        '<strong>' + escapeHTML(show.name) + '</strong><span class="pill ' + (show.open ? 'open' : '') +
        '">' + (show.open ? 'Voting hours open' : 'Voting hours closed') + '</span></button>'
      ).join("")
      : '<div class="empty-state">Your favourite chart show will appear here.</div>';
  }
  function renderSongs() {
    const list = $("song-list");
    const available = state.detail?.songs || [];
    const q = $("song-search").value.toLocaleLowerCase().trim();
    const songs = available.filter(s => (s.artist + " " + s.title).toLocaleLowerCase().includes(q));
    $("song-search").disabled = !available.length;
    $("song-search").placeholder = available.length
      ? "Search available songs…" : "Songs available after publication";
    list.innerHTML = !state.detail
      ? '<div class="empty-state">Select a chart show to see eligible songs.</div>'
      : !available.length
        ? '<div class="empty-state"><strong>No eligible songs to choose yet.</strong><p>The presenter must publish the current voting period\'s verified song list before it appears here.</p></div>'
        : !songs.length
          ? '<div class="empty-state">No matching songs. Try another search.</div>'
          : songs.map((song) =>
              '<button class="song-card' + (state.selected?.id === song.id ? ' selected' : '') +
              '" data-song="' + song.id + '" type="button"><span class="song-art" aria-hidden="true">♫</span>' +
              '<span class="song-text"><strong>' + escapeHTML(song.title) + '</strong><small>' +
              escapeHTML(song.artist) + '</small></span><span class="song-choose">' +
              (state.selected?.id === song.id ? 'Selected ✓' : 'Choose →') + '</span></button>'
            ).join("");
    $("selected-song").textContent = state.selected
      ? state.selected.title + " — " + state.selected.artist + " (change)"
      : available.length ? "Browse eligible songs →"
        : "Browse song list →";
    $("selection-hint").textContent = state.selected
      ? "Tap to choose a different song."
      : available.length
        ? "Tap to find and choose a verified song."
        : "No published song list is available to select from yet.";
    updateVoteButton();
    renderAvailability();
  }
  function resetShow() {
    state.detail = null;
    state.selected = null;
    $("song-search").value = "";
    $("song-instruction").textContent = "Select a chart show to view its official song list.";
    $("voting-status").hidden = true;
    renderSongs();
  }
  $("station-list").addEventListener("click", (event) => {
    const item = event.target.closest("[data-station]");
    if (!item) return;
    state.station = state.stations.find(s=>s.slug === item.dataset.station) || null;
    state.show = null;
    resetShow();
    renderStations();renderShows();
    $("chart-shows").scrollIntoView({behavior:"smooth", block:"start"});
  });
  $("show-list").addEventListener("click", async (event) => {
    const item = event.target.closest("[data-show]");
    if (!item || !state.station) return;
    const show = state.station.shows.find(s=>s.id === Number(item.dataset.show));
    if (!show) return;
    state.show = show;
    resetShow();
    renderShows();
    $("song-instruction").textContent = "Loading the verified music catalogue…";
    try {
      const detail = await get("/vote/api/shows/" + show.id + "/");
      if (state.show?.id !== show.id) return;
      state.detail = detail;
      $("song-instruction").textContent = detail.voting_open
        ? "Choose a published song to support in this chart show."
        : detail.has_published_songs
          ? "You can browse the published list. See below why online voting is unavailable."
          : "The presenter needs to publish this voting period's eligible-song list.";
      showStatus(detail.voting_status_label || "Not available", detail.voting_open);
    } catch (err) {
      $("song-instruction").textContent = err.message;
    }
    renderSongs();
    $("songs").scrollIntoView({behavior:"smooth", block:"start"});
  });
  $("song-list").addEventListener("click", (event) => {
    const item = event.target.closest("[data-song]");
    if (!item || !state.detail) return;
    state.selected = state.detail.songs.find(s => s.id === Number(item.dataset.song)) || null;
    renderSongs();message("");
  });
  $("song-search").addEventListener("input", renderSongs);
  $("selected-song").addEventListener("click", () => {
    const songs = state.detail?.songs || [];
    if (!songs.length) {
      message("No eligible songs have been published for this chart show and voting period.", "error");
      const info = $("vote-availability");
      (info.hidden ? $("song-list") : info).scrollIntoView({
        behavior: "smooth", block: "center",
      });
      return;
    }
    $("song-search").scrollIntoView({behavior: "smooth", block: "center"});
    $("song-search").focus({preventScroll: true});
  });

  $("verify-start-form").addEventListener("submit", async (event) => {
    event.preventDefault();message("");
    if (!enabled || !state.selected || !state.detail?.voting_open) {
      message("Choose an eligible song on an open chart show first.","error");
      return;
    }
    const form = event.currentTarget;
    setBusy(form,true);
    try {
      const token = turnstileToken("verification-turnstile");
      const result = await post("/vote/api/verify/start/", {
        email: $("email").value, show_id: state.show.id,
        song_id: state.selected.id, "cf-turnstile-response": token,
      });
      form.hidden = true;
      $("verify-check-form").hidden = false;
      $("otp").focus();
      message(result.message,"success");
    } catch (err) {message(err.message,"error");}
    finally {setBusy(form,false);updateVoteButton();resetTurnstile("verification-turnstile");}
  });
  $("verify-check-form").addEventListener("submit", async (event) => {
    event.preventDefault();message("");
    const form = event.currentTarget;
    setBusy(form,true);
    try {
      const result = await post("/vote/api/verify/check/", {code:$("otp").value});
      state.verified = true;
      $("verified-state").textContent = "✓ Email verified. You can cast your vote.";
      $("verified-state").classList.add("ok");
      $("verify-start-form").hidden = true;
      $("verify-check-form").hidden = true;
      message(result.message,"success");
      updateVoteButton();
    } catch (err) {message(err.message,"error");}
    finally {setBusy(form,false);}
  });
  $("vote-form").addEventListener("submit", async (event) => {
    event.preventDefault();message("");
    if (!state.selected || !state.show || !state.verified || !state.detail?.voting_open) return;
    const form = event.currentTarget;
    setBusy(form,true);
    try {
      const result = await post("/vote/api/cast/", {
        show_id: state.show.id, song_id: state.selected.id,
        "cf-turnstile-response":turnstileToken("vote-turnstile"),
      });
      message(result.message,"success");
    } catch (err) {message(err.message,"error");}
    finally {resetTurnstile("vote-turnstile");updateVoteButton();}
  });

  (async () => {
    try {
      const data = await get("/vote/api/stations/");
      state.stations = data.stations || [];
      renderStations();
    } catch(err) {
      $("station-list").innerHTML = '<div class="empty-state">' + escapeHTML(err.message) + '</div>';
    }
  })();
})();
