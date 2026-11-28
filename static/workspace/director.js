"use strict";
(() => {
  const form = document.getElementById("report-filters");
  if (!form) return;
  document.getElementById("apply-report").hidden = true;
  form.addEventListener("change", event => {
    if (event.target.name === "station" || event.target.name === "archived") {
      form.elements.show.value = "";
      if (form.elements.tab.value === "chart") form.elements.tab.value = "overview";
    }
    form.requestSubmit();
  });
  // Refresh read-only panels without disturbing programme editors or keyboard focus.
  const url = new URL(location.href);
  if (url.searchParams.has("archive") || url.searchParams.has("page") ||
      document.querySelector('#report-body form[method="post"]')) return;
  let pending = false;
  setInterval(async () => {
    const body = document.getElementById("report-body");
    if (pending || document.hidden || body.contains(document.activeElement)) return;
    pending = true;
    try {
      const response = await fetch(location.href, {headers: {"X-AirVote-Fragment": "report"}, cache: "no-store"});
      if (response.redirected || response.status === 403) { location.reload(); return; }
      if (!response.ok) return;
      const content = new DOMParser().parseFromString(await response.text(), "text/html");
      const next = content.getElementById("report-body");
      if (next && !body.contains(document.activeElement)) body.replaceWith(next);
    } catch { /* Keep the last successful update time visible while disconnected. */ }
    finally { pending = false; }
  }, 30000);
})();
