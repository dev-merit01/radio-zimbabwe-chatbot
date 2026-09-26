"use strict";
(() => {
  const form = document.getElementById("connection-form");
  const server = document.getElementById("server");
  const button = document.getElementById("connect");
  const status = document.getElementById("status");
  const invoke = window.__TAURI__?.core?.invoke;
  let busy = false;

  function message(text, error = false) {
    status.textContent = text;
    status.classList.toggle("error", error);
  }

  async function connect() {
    if (busy) return;
    if (!invoke) {
      message(
        "Open Voting Studio from the installed Windows application.",
        true,
      );
      return;
    }
    busy = true;
    button.disabled = true;
    server.disabled = true;
    form.setAttribute("aria-busy", "true");
    message("Connecting to your station…");
    try {
      await invoke("connect_server", { serverUrl: server.value.trim() });
      message("Workspace opened. You can change your server here at any time.");
    } catch (error) {
      message(String(error || "Could not connect. Please try again."), true);
    } finally {
      busy = false;
      button.disabled = false;
      server.disabled = false;
      form.setAttribute("aria-busy", "false");
    }
  }

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    void connect();
  });

  async function start() {
    if (!invoke) {
      message(
        "Open Voting Studio from the installed Windows application.",
        true,
      );
      return;
    }
    try {
      const config = await invoke("get_configuration");
      server.value = config.server_url || "";
      if (config.warning) message(config.warning, true);
      else if (server.value) await connect();
    } catch (error) {
      message(
        String(
          error || "Settings could not be loaded. Enter the server address.",
        ),
        true,
      );
    }
  }
  void start();
})();
