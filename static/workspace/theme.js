"use strict";
(() => {
  // Client-side appearance only. No station data, permissions or vote state
  // are changed; the preference stays on this browser/device.
  const KEY = "airvote.appearance.theme";
  const root = document.documentElement;
  let current = "light";

  try {
    current = localStorage.getItem(KEY) === "dark" ? "dark" : "light";
  } catch {
    // Privacy-restricted WebViews may block localStorage.
  }
  root.dataset.theme = current;
  root.style.colorScheme = current;

  function syncControls() {
    const dark = current === "dark";
    const quick = document.getElementById("theme-quick-toggle");
    if (quick) {
      quick.setAttribute("aria-label", dark ? "Switch to light mode" : "Switch to dark mode");
      quick.setAttribute("title", dark ? "Switch to light mode" : "Switch to dark mode");
      quick.setAttribute("aria-pressed", String(dark));
      const icon = quick.querySelector(".theme-icon");
      if (icon) icon.textContent = dark ? "☀" : "☾";
      const label = quick.querySelector(".theme-toggle-label");
      if (label) label.textContent = dark ? "Light" : "Dark";
    }
    document.querySelectorAll('input[name="appearance-theme"]').forEach((input) => {
      input.checked = input.value === current;
    });
  }

  function applyTheme(value) {
    if (value !== "light" && value !== "dark") return;
    current = value;
    root.dataset.theme = value;
    root.style.colorScheme = value;
    try {
      localStorage.setItem(KEY, value);
    } catch {
      // Still allow switching for this page when persistence is restricted.
    }
    syncControls();
  }

  function init() {
    document.getElementById("theme-quick-toggle")?.addEventListener("click", () => {
      applyTheme(current === "dark" ? "light" : "dark");
    });

    const dialog = document.getElementById("appearance-dialog");
    document.getElementById("appearance-open")?.addEventListener("click", () => {
      // Avoid leaving the mobile navigation drawer over the appearance modal.
      if (document.getElementById("sidebar")?.classList.contains("open")) {
        document.getElementById("sidebar-close")?.click();
      }
      syncControls();
      if (dialog && !dialog.open) dialog.showModal();
    });
    document.getElementById("appearance-close")?.addEventListener("click", () => {
      dialog?.close();
    });
    document.querySelectorAll('input[name="appearance-theme"]').forEach((input) => {
      input.addEventListener("change", () => {
        if (input.checked) applyTheme(input.value);
      });
    });

    // Reflect changes from another AirVote tab while preserving this device's choice.
    window.addEventListener("storage", (event) => {
      if (event.key === KEY) {
        current = event.newValue === "dark" ? "dark" : "light";
        root.dataset.theme = current;
        root.style.colorScheme = current;
        syncControls();
      }
    });
    syncControls();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
