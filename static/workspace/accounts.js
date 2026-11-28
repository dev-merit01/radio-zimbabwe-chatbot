"use strict";
(() => {
  const form = document.getElementById("account-editor");
  if (!form) return;
  const password = form.elements.password;
  const toggle = document.getElementById("toggle-account-password");
  toggle.hidden = false;
  toggle.addEventListener("click", () => {
    const reveal = password.type === "password";
    password.type = reveal ? "text" : "password";
    toggle.textContent = reveal ? "Hide" : "Show";
    toggle.setAttribute("aria-label", reveal ? "Hide password" : "Show password");
  });
  const errors = document.getElementById("account-errors");
  if (errors) errors.focus();
  const presetStatus = document.getElementById("preset-status");
  const privileges = form.querySelectorAll('input[name="privileges"]');
  for (const input of privileges) input.addEventListener("change", () => { presetStatus.textContent = ""; });
  document.getElementById("read-only-account").addEventListener("click", () => {
    for (const input of privileges) {
      input.checked = ["archives", "incoming"].includes(input.value);
    }
    presetStatus.textContent = "Read-only privileges selected.";
  });
  const shows = JSON.parse(document.getElementById("account-show-options").textContent);
  const byId = new Map(shows.map(show => [String(show.pk), show]));
  const station = document.getElementById("presenter-station");
  if (station) {
    const programme = form.elements.programme;
    station.value = byId.get(programme.value)?.station || "";
    const filter = () => {
      for (const option of programme.options) {
        if (!option.value) continue;
        option.hidden = option.disabled = byId.get(option.value)?.station !== station.value;
      }
      if (programme.selectedOptions[0]?.disabled) programme.value = "";
    };
    station.addEventListener("change", filter);
    filter();
    return;
  }

  const stationInputs = [...form.querySelectorAll('input[name="stations"]')];
  if (form.dataset.role === "manager") stationInputs.forEach(input => { input.type = "radio"; });
  const specific = document.getElementById("specific-shows");
  const network = form.elements.network_access;
  const picker = form.querySelector("[data-show-picker]");
  const update = () => {
    const allStations = form.dataset.role === "director" || network?.checked;
    const selected = new Set(stationInputs.filter(input => input.checked).map(input => input.value));
    const stationPicker = form.querySelector("[data-station-picker]");
    if (stationPicker) stationPicker.hidden = !!allStations;
    picker.hidden = !specific.checked;
    let availableCount = 0;
    for (const input of picker.querySelectorAll('input[name="shows"]')) {
      const available = allStations || selected.has(byId.get(input.value)?.station);
      if (available) availableCount++;
      input.closest("label").parentElement.hidden = !available;
      input.disabled = !available || !specific.checked;
      if (!available) input.checked = false;
    }
    const help = document.getElementById("show-picker-help");
    help.hidden = availableCount > 0;
    help.textContent = selected.size || allStations
      ? "No chart shows are available for this selection."
      : "Select a radio station to see its chart shows.";
  };
  form.addEventListener("change", update);
  update();
})();
