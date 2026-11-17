/* AirVote listener interface. No framework, analytics tracker or token persistence.
 * All texts are assigned via textContent. The AirVote API, not the browser,
 * decides eligible songs, voting hours and daily limits.
 */
const byId = (id) => document.getElementById(id);
const state = {
  site: null, stations: [], station: null, campaign: null,
  selectedSong: null, googleToken: "", humanToken: "",
  humanWidget: null, sending: false, requestId: null,
};

function message(text, success = false) {
  const element = byId("messageBar");
  element.textContent = text;
  element.classList.toggle("success", success);
  element.hidden = !text;
}
function el(name, className, text) {
  const element = document.createElement(name);
  if (className) element.className = className;
  if (text !== undefined) element.textContent = text;
  return element;
}
function getInitials(name) {
  return String(name || "AV").split(/\s+/).filter(Boolean).slice(0, 2)
    .map(s => s.charAt(0).toUpperCase()).join("") || "AV";
}
function addLogo(container, station) {
  const fallback = el("span", "logo-fallback", getInitials(station.name));
  container.append(fallback);
  if (station.logo && /^https:\/\//.test(station.logo)) {
    const image = el("img");
    image.src = station.logo;
    image.alt = "";
    image.loading = "lazy";
    image.referrerPolicy = "no-referrer";
    image.addEventListener("load", () => { fallback.hidden = true; });
    image.addEventListener("error", () => { image.remove(); fallback.hidden = false; });
    container.prepend(image);
  }
}
async function api(route, options = {}) {
  const response = await fetch(route, {
    cache: "no-store", credentials: "same-origin",
    ...options, headers: { Accept: "application/json", ...options.headers },
  });
  let result;
  try { result = await response.json(); } catch { throw Error("The service is temporarily unavailable."); }
  if (!response.ok) throw Error(result.error || "Unable to complete the request.");
  return result;
}
function renderStations() {
  const query = byId("stationSearch").value.trim().toLocaleLowerCase();
  const matching = state.stations.filter(station => station.name.toLocaleLowerCase().includes(query));
  byId("stationCount").textContent = matching.length + " station" + (matching.length === 1 ? "" : "s");
  const fragment = document.createDocumentFragment();
  for (const station of matching) {
    const button = el("button", "station-card" + (station.id === state.station?.id ? " selected" : ""));
    button.type = "button";
    button.setAttribute("aria-label", "Select " + station.name);
    button.setAttribute("aria-pressed", String(station.id === state.station?.id));
    const logo = el("span", "station-logo");
    addLogo(logo, station);
    const bottom = el("span", "station-card-bottom");
    const label = el("span");
    label.append(el("strong", "", station.name), el("small", "", "SELECT STATION"));
    bottom.append(label, el("span", "station-arrow", "↗"));
    button.append(logo, bottom);
    button.addEventListener("click", () => chooseStation(station));
    fragment.append(button);
  }
  if (!matching.length) fragment.append(el("p", "empty-message", "No stations match your search."));
  byId("stationGrid").replaceChildren(fragment);
}
function chooseStation(station) {
  state.station = station;
  byId("chosenStationTitle").textContent = "Vote with " + station.name + ".";
  byId("codeForm").hidden = false;
  renderStations();
  byId("codeForm").scrollIntoView({ behavior: "smooth", block: "center" });
  byId("showCode").focus({ preventScroll: true });
}
function showPolicies(config) {
  const terms = [
    "Up to " + config.daily_limit + " votes per programme daily",
    config.station_daily_limit === null ?
      "Station-wide limits follow AirVote settings" :
      config.station_daily_limit + " station-wide votes per day",
    config.allow_repeat_song ? "Repeat songs allowed" :
      "Same-song repeats blocked " + (config.repeat_scope === "station" ? "across this station" : "within this programme"),
  ];
  byId("policySummary").replaceChildren(...terms.map(x => el("span", "policy-tag", x)));
}
function renderSongs() {
  if (!state.campaign?.open) return;
  const search = byId("songSearch").value.trim().toLocaleLowerCase();
  const matches = state.campaign.songs.filter(song =>
    song.title.toLocaleLowerCase().includes(search) || song.artist.toLocaleLowerCase().includes(search));
  byId("songCount").textContent = matches.length + " eligible track" + (matches.length === 1 ? "" : "s");
  const fragment = document.createDocumentFragment();
  for (const song of matches) {
    const chosen = state.selectedSong?.id === song.id;
    const button = el("button", "song-item" + (chosen ? " chosen" : ""));
    button.type = "button";
    button.setAttribute("aria-pressed", String(chosen));
    button.setAttribute("aria-label", "Choose " + song.title + " by " + song.artist);
    const art = el("span", "song-art");
    const fallback = el("span", "song-placeholder", "♪");
    art.append(fallback);
    if (song.artwork && /^https:\/\//.test(song.artwork)) {
      const image = el("img");
      image.src = song.artwork;
      image.loading = "lazy";
      image.alt = "";
      image.referrerPolicy = "no-referrer";
      image.addEventListener("load", () => { fallback.hidden = true; });
      image.addEventListener("error", () => image.remove());
      art.prepend(image);
    }
    const details = el("span", "song-meta");
    details.append(el("strong", "", song.title), el("small", "", song.artist));
    button.append(art, details, el("span", "song-select-icon", chosen ? "✓" : "+"));
    button.addEventListener("click", () => {
      state.selectedSong = song;
      state.requestId = crypto.randomUUID();
      byId("selectedSongLabel").textContent = song.title + " — " + song.artist;
      byId("receiptPanel").hidden = true;
      renderSongs();
      updateButton();
      byId("castPanel").scrollIntoView({ behavior: "smooth", block: "center" });
    });
    fragment.append(button);
  }
  byId("songsGrid").replaceChildren(fragment);
  byId("noSongs").hidden = Boolean(matches.length);
}
function updateButton() {
  const enabled = Boolean(state.campaign?.open && state.selectedSong &&
    state.googleToken && state.humanToken && state.site?.ready && !state.sending);
  byId("castButton").disabled = !enabled;
  byId("castButton").textContent = state.sending ? "Submitting securely…" : "Submit my vote  ↗";
}
function loadGoogle() {
  const root = byId("googleLogin");
  if (!state.site?.google_client_id) return;
  const tryRender = (attempt) => {
    if (!window.google?.accounts?.id) {
      if (attempt < 45) setTimeout(() => tryRender(attempt + 1), 200);
      else byId("googleState").textContent = "Google sign-in is currently unavailable.";
      return;
    }
    window.google.accounts.id.initialize({
      client_id: state.site.google_client_id,
      callback: response => {
        state.googleToken = response.credential || "";
        byId("googleState").textContent = state.googleToken ? "Google account verified" : "Not signed in";
        byId("googleState").classList.toggle("authenticated", Boolean(state.googleToken));
        updateButton();
      },
      auto_select: false,
      cancel_on_tap_outside: true,
    });
    window.google.accounts.id.renderButton(root, {
      theme: "outline", size: "large", shape: "rectangular", text: "signin_with",
      width: Math.min(290, root.clientWidth || 290),
    });
  };
  tryRender(0);
}
function loadTurnstile() {
  if (!state.site?.turnstile_site_key) return;
  const tryRender = (attempt) => {
    if (!window.turnstile) {
      if (attempt < 45) setTimeout(() => tryRender(attempt + 1), 200);
      return;
    }
    if (state.humanWidget !== null) return;
    state.humanWidget = window.turnstile.render(byId("turnstileWidget"), {
      sitekey: state.site.turnstile_site_key,
      action: "vote",
      theme: "light",
      callback: token => { state.humanToken = token; updateButton(); },
      "expired-callback": () => { state.humanToken = ""; updateButton(); },
      "error-callback": () => { state.humanToken = ""; updateButton(); },
    });
  };
  tryRender(0);
}
function refreshHuman() {
  state.humanToken = "";
  if (window.turnstile && state.humanWidget !== null) window.turnstile.reset(state.humanWidget);
  updateButton();
}
async function displayCampaign(token) {
  message("");
  let config;
  try { config = await api("/api/campaign/" + encodeURIComponent(token)); }
  catch (error) { message(error.message); return; }
  state.campaign = config;
  state.selectedSong = null;
  state.requestId = null;
  byId("hero").hidden = true;
  byId("stationPanel").hidden = true;
  byId("ballotPanel").hidden = false;
  byId("ballotStationName").textContent = config.station.name;
  const img = byId("ballotStationLogo");
  img.style.display = "none";
  img.removeAttribute("src");
  byId("ballotLogoFallback").textContent = getInitials(config.station.name);
  byId("ballotLogoFallback").hidden = false;
  if (config.station.logo && /^https:\/\//.test(config.station.logo)) {
    img.src = config.station.logo;
    img.addEventListener("load", () => {
      img.style.display = "block";
      byId("ballotLogoFallback").hidden = true;
    }, { once: true });
  }
  byId("votingStatus").classList.toggle("closed", !config.open);
  byId("votingStatus").textContent = config.open ? "●  Voting is open" : "●  Voting is closed";
  byId("votingClosed").hidden = config.open;
  byId("votingOpen").hidden = !config.open;
  byId("songSearch").value = "";
  if (config.open) {
    showPolicies(config);
    renderSongs();
    byId("selectedSongLabel").textContent = "Choose a song above to continue.";
    if (!state.site?.ready) message("Online voting is awaiting activation.", false);
    loadGoogle();
    loadTurnstile();
  }
  updateButton();
  window.scrollTo({ top: 0, behavior: "instant" });
}
async function pollReceipt(id, secret) {
  const panel = byId("receiptPanel");
  for (let attempt = 0; attempt < 6; attempt++) {
    await new Promise(resolve => setTimeout(resolve, 3000));
    try {
      const result = await api("/api/receipt/" + encodeURIComponent(id), {
        headers: { Authorization: "Bearer " + secret },
      });
      if (result.state === "accepted") {
        panel.replaceChildren(el("strong", "", "Submitted for verification"), el("p", "", result.message));
        return;
      }
      if (result.state === "rejected") {
        panel.replaceChildren(el("strong", "", "Vote could not be accepted"), el("p", "", result.message));
        return;
      }
    } catch { return; }
  }
  panel.replaceChildren(el("strong", "", "Submission still pending"),
    el("p", "", "Your submission has been stored. The station's verification rules will be applied before it counts."));
}
async function submit() {
  if (state.sending || byId("castButton").disabled || !state.selectedSong) return;
  state.sending = true; updateButton(); message("");
  const id = state.requestId || crypto.randomUUID();
  const payload = {
    request_id: id, campaign: state.campaign.campaign,
    song_id: state.selectedSong.id,
    google_credential: state.googleToken,
    turnstile_token: state.humanToken,
  };
  try {
    const result = await api("/api/vote", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    state.requestId = null;
    const receiptPanel = byId("receiptPanel");
    receiptPanel.hidden = false;
    receiptPanel.replaceChildren(el("strong", "", "Your vote was received."),
      el("p", "", "AirVote is checking your submission before it is included in the official tally."));
    receiptPanel.scrollIntoView({ behavior: "smooth", block: "center" });
    refreshHuman();
    void pollReceipt(result.receipt_id, result.receipt_secret);
  } catch (err) { message(err.message); refreshHuman(); }
  finally { state.sending = false; updateButton(); }
}
async function init() {
  byId("currentYear").textContent = String(new Date().getFullYear());
  byId("stationSearch").addEventListener("input", renderStations);
  byId("songSearch").addEventListener("input", renderSongs);
  byId("castButton").addEventListener("click", submit);
  byId("codeForm").addEventListener("submit", async event => {
    event.preventDefault();
    const code = byId("showCode").value.trim().toUpperCase();
    if (!state.station || !/^[A-Z0-9]{2,16}$/.test(code)) {
      message("Enter the valid voting code shared by your station.");
      return;
    }
    try {
      const data = await api("/api/resolve?station=" +
        encodeURIComponent(state.station.id) + "&code=" + encodeURIComponent(code));
      history.pushState({}, "", "/vote/" + encodeURIComponent(data.campaign));
      await displayCampaign(data.campaign);
    } catch (error) { message(error.message); }
  });
  document.addEventListener("keydown", event => {
    const focused = document.activeElement;
    if (event.key === "/" && !["INPUT","TEXTAREA"].includes(focused?.tagName) &&
      state.campaign?.open) { event.preventDefault(); byId("songSearch").focus(); }
  });
  window.addEventListener("popstate", () => location.reload());
  try {
    const [site, directory] = await Promise.all([api("/api/site"), api("/api/stations")]);
    state.site = site;
    state.stations = Array.isArray(directory.stations) ? directory.stations : [];
    renderStations();
    const match = location.pathname.match(/^\/vote\/([^/]+)$/);
    if (match) await displayCampaign(decodeURIComponent(match[1]));
    if (!site.ready) message("Voting is not yet available. Please try again later.");
  } catch { byId("stationGrid").replaceChildren(
    el("p", "empty-message", "Unable to load stations. Please try again shortly."));
    message("The voting service is temporarily unavailable.");
  }
}
init();
