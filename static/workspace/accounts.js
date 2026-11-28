"use strict";
(() => {
  const form = document.getElementById("account-editor");
  if (!form) return;
  document.getElementById("read-only-account").addEventListener("click", () => {
    for (const input of form.querySelectorAll('input[name="privileges"]')) {
      input.checked = input.value === "archives";
    }
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
    for (const input of picker.querySelectorAll('input[name="shows"]')) {
      const available = allStations || selected.has(byId.get(input.value)?.station);
      input.closest("label").parentElement.hidden = !available;
      input.disabled = !available || !specific.checked;
      if (!available) input.checked = false;
    }
  };
  form.addEventListener("change", update);
  update();
})();
