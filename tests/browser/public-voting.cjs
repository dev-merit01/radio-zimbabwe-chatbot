/* Public voting: two-breakpoint browser acceptance with mocked read-only
 * campaign API. No Google or Turnstile keys are needed to test the interface.
 */
"use strict";
const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");

const dir = path.join(__dirname, "../../public-voting/public");
const files = {
  "/": ["index.html", "text/html; charset=utf-8"],
  "/index.html": ["index.html", "text/html; charset=utf-8"],
  "/style.css": ["style.css", "text/css; charset=utf-8"],
  "/app.js": ["app.js", "text/javascript; charset=utf-8"],
  "/favicon.svg": ["favicon.svg", "image/svg+xml"],
};
const ballot = {
  station: { id: "national_fm", name: "National FM", logo: "" },
  campaign: "secure-campaign-token",
  open: true, show_id: 123, timezone: "Africa/Harare",
  daily_limit: 5, station_daily_limit: 7, allow_repeat_song: false,
  repeat_scope: "station",
  songs: [
    { id: 1, artist: "Jah Prayzah", title: "Sample One", artwork: "" },
    { id: 2, artist: "Winky D", title: "Sample Two", artwork: "" },
    { id: 3, artist: "Gemma Griffiths", title: "Sample Three", artwork: "" },
  ],
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://localhost");
  const api = (object) => {
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Cache-Control", "no-store");
    res.end(JSON.stringify(object));
  };
  if (url.pathname === "/api/site") return api({
    ready: true, google_client_id: "", turnstile_site_key: "", google_nonce: "",
  });
  if (url.pathname === "/api/stations") return api({
    stations: [
      { id: "national_fm", name: "National FM", logo: "" },
      { id: "power_fm", name: "Power FM", logo: "" },
      { id: "radio_zimbabwe", name: "Radio Zimbabwe", logo: "" },
    ],
  });
  if (url.pathname === "/api/resolve") return api({ campaign: "secure-campaign-token" });
  if (url.pathname === "/api/campaign/secure-campaign-token") return api(ballot);
  const file = files[url.pathname] || (url.pathname.startsWith("/vote/") ? files["/"] : null);
  if (!file) { res.writeHead(404).end(); return; }
  res.setHeader("Content-Type", file[1]);
  res.end(fs.readFileSync(path.join(dir, file[0])));
});

(async () => {
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  let browser;
  try {
    browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
    const origin = "http://127.0.0.1:" + server.address().port;
    for (const width of [375, 1440]) {
      const page = await browser.newPage({ viewport: { width, height: 900 } });
      await page.route("https://accounts.google.com/**", route => route.abort());
      await page.route("https://challenges.cloudflare.com/**", route => route.abort());
      await page.goto(origin, { waitUntil: "domcontentloaded" });
      await page.getByRole("heading", { name: "Find your station." }).waitFor();
      assert.equal(await page.locator(".station-card").count(), 3);
      await page.getByPlaceholder("Search radio stations…").fill("national");
      assert.equal(await page.locator(".station-card").count(), 1);
      await page.getByRole("button", { name: "Select National FM" }).click();
      await page.getByLabel("Programme voting code").fill("PV2026");
      await page.getByRole("button", { name: /Continue/ }).click();
      await page.getByRole("heading", { name: "Cast your vote." }).waitFor();
      assert.equal(await page.locator(".song-item").count(), 3);
      await page.getByPlaceholder("Search songs and artists…").fill("Gemma");
      assert.equal(await page.locator(".song-item").count(), 1);
      await page.getByRole("button", { name: "Choose Sample Three by Gemma Griffiths" }).click();
      await page.getByText("Sample Three — Gemma Griffiths").waitFor();
      assert.equal(await page.locator("#castButton").isDisabled(), true);
      assert.equal(await page.getByText("Top 20").count(), 0);
      assert.equal(await page.getByText("Chart archives").count(), 0);
      assert.equal(await page.locator("#messageBar").isVisible(), false);
      const dimensions = await page.evaluate(() => ({
        viewport: document.documentElement.clientWidth,
        layout: document.documentElement.scrollWidth,
      }));
      assert(dimensions.layout <= dimensions.viewport + 1,
        "Mobile/desktop overflow at " + width + "px: " + JSON.stringify(dimensions));
      await page.close();
    }
    process.stdout.write("AirVote public voting interface: responsive search, selection and privacy checks passed.\n");
  } finally {
    if (browser) await browser.close();
    await new Promise(resolve => server.close(resolve));
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
