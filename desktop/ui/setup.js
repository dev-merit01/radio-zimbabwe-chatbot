"use strict";
(() => {
  const form = document.getElementById("connection-form");
  const server = document.getElementById("server");
  const button = document.getElementById("connect");
  const recovery = document.getElementById("recovery");
  const retry = document.getElementById("retry");
  const settings = document.getElementById("change-server");
  const status = document.getElementById("status");
  const invoke = window.__TAURI__?.core?.invoke;
  let busy = false;

  function message(text, error = false) {
    status.textContent = text;
    status.classList.toggle("error", error);
  }

  function show(mode) {
    form.hidden = mode !== "settings";
    recovery.hidden = mode !== "offline";
  }

  // This UI is hidden at native startup. Only show it for an offline
  // recovery or when the user explicitly requests connection settings.
  async function reveal() {
    if (!invoke) return;
    try {
      await invoke("reveal_setup");
    } catch (error) {
      console.error("Could not display AirVote connection recovery:", error);
    }
  }

  function showSettings() {
    show("settings");
    server.disabled = false;
    button.disabled = false;
    server.focus();
  }

  window.airvoteShowSettings = showSettings;

  async function connect() {
    if (busy) return;
    if (!invoke) {
      show("offline");
      message("Open AirVote from the installed Windows application.", true);
      return;
    }
    busy = true;
    button.disabled = true;
    retry.disabled = true;
    server.disabled = true;
    form.setAttribute("aria-busy", "true");
    show("connecting");
    message("Connecting to AirVote securely…");
    try {
      await invoke("connect_server", { serverUrl: server.value.trim() });
      message("Workspace opened.");
      // Rust creates and focuses the workspace; the bundled setup window
      // remains hidden. The operator never has to enter a backend URL.
    } catch (error) {
      show("offline");
      message(String(error || "Could not connect. Please try again."), true);
      await reveal();
    } finally {
      busy = false;
      button.disabled = false;
      retry.disabled = false;
      server.disabled = false;
      form.setAttribute("aria-busy", "false");
    }
  }

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    void connect();
  });
  retry.addEventListener("click", () => void connect());
  settings.addEventListener("click", showSettings);

  async function start() {
    show("connecting");
    if (!invoke) {
      show("offline");
      message("Open AirVote from the installed Windows application.", true);
      return;
    }
    try {
      const config = await invoke("get_configuration");
      server.value = config.server_url || "";
      if (server.value) {
        // A damaged saved setting is recoverable with the built-in
        // production server; an operator should not see a URL form.
        await connect();
      } else {
        showSettings();
        message(config.warning || "A server address has not been configured.", !!config.warning);
        await reveal();
      }
    } catch (error) {
      show("offline");
      message(String(error || "AirVote could not load its connection settings."), true);
      await reveal();
    }
  }
  void start();
})();
