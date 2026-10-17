"use strict";
(() => {
  const $ = id => document.getElementById(id);
  const form = $("connection-form"), server = $("server"), button = $("connect");
  const invoke = window.__TAURI__?.core?.invoke;
  let busy = false;
  function message(text, error = false) {
    $("status").textContent = text;
    document.body.classList.toggle("error", error);
  }
  function settings() {
    if (busy) return;
    form.hidden = false;
    $("recovery").hidden = true;
    $("heading").textContent = "Your station. Connected.";
    $("intro").textContent = "Set your station address once. We’ll take you there next time.";
    message("Connect to your AirVote workspace.");
    server.focus();
  }
  window.addEventListener("airvote-settings", settings);
  async function connect() {
    if (busy) return;
    if (!invoke) { message("Open AirVote from the installed Windows application.", true); return; }
    busy = true;
    document.body.classList.add("busy");
    form.hidden = true;
    $("recovery").hidden = true;
    button.disabled = server.disabled = true;
    $("retry").disabled = $("settings").disabled = true;
    message("Connecting to your station…");
    try {
      await invoke("connect_server", { serverUrl: server.value.trim() });
      message("Your workspace is ready.");
      // The native menu can reopen this window to change its connection.
      settingsAfterConnect();
    } catch (error) {
      message(String(error || "We couldn’t connect. Check your internet connection and try again."), true);
      $("recovery").hidden = false;
    } finally {
      busy = false;
      document.body.classList.remove("busy");
      button.disabled = server.disabled = false;
      $("retry").disabled = $("settings").disabled = false;
    }
  }
  function settingsAfterConnect() {
    // Remains hidden by the native host until Connection settings is selected.
    form.hidden = false;
  }
  form.addEventListener("submit", event => { event.preventDefault(); void connect(); });
  $("retry").addEventListener("click", () => void connect());
  $("settings").addEventListener("click", settings);
  async function start() {
    document.body.classList.add("busy");
    if (!invoke) {
      document.body.classList.remove("busy");
      message("Open AirVote from the installed Windows application.", true);
      return;
    }
    try {
      const config = await invoke("get_configuration");
      server.value = config.server_url || "";
      if (server.value && !config.warning) { await connect(); return; }
      document.body.classList.remove("busy");
      settings();
      if (config.warning) message(config.warning, true);
    } catch (error) {
      document.body.classList.remove("busy");
      settings();
      message(String(error || "Settings could not be loaded. Enter your station address."), true);
    }
  }
  void start();
})();
