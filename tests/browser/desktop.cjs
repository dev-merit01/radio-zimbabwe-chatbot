/* Tests bundled recovery UI with a mocked native bridge. Native acceptance runs on Windows. */
"use strict";
const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");

(async () => {
  const files = {
    "/": ["index.html", "text/html"],
    "/setup.js": ["setup.js", "text/javascript"],
    "/setup.css": ["setup.css", "text/css"],
    "/assets/airvote-icon.svg": ["assets/airvote-icon.svg", "image/svg+xml"],
  };
  const server = http.createServer((req, res) => {
    const asset = files[req.url];
    if (!asset) { res.writeHead(404).end(); return; }
    res.setHeader("Content-Type", asset[1]);
    res.end(fs.readFileSync(path.join(__dirname, "../../desktop/ui", asset[0])));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox"],
      ...(process.env.CHROMIUM_PATH
        ? { executablePath: process.env.CHROMIUM_PATH }
        : {}),
    });
    const url = `http://127.0.0.1:${server.address().port}`;
    async function pageFor(config, error = "") {
      const page = await browser.newPage({ viewport: { width: 780, height: 660 } });
      await page.addInitScript(({ config, error }) => {
        window.calls = [];
        window.connectionError = error;
        window.__TAURI__ = {
          core: {
            invoke: async (command, args) => {
              if (command === "get_configuration") return config;
              window.calls.push({ command, args });
              await new Promise((resolve) => setTimeout(resolve, 80));
              if (command === "connect_server" && window.connectionError)
                throw window.connectionError;
            },
          },
        };
      }, { config, error });
      await page.goto(url);
      return page;
    }

    // A newly installed app always has a trusted default in native Rust.
    // A truly blank address opens advanced settings rather than an inert screen.
    const first = await pageFor({ server_url: "", warning: "" });
    await first.getByLabel("Station server", { exact: true }).waitFor({ state: "visible" });
    assert.deepEqual((await first.evaluate(() => window.calls)).map(x => x.command), ["reveal_setup"]);
    await first.getByLabel("Station server", { exact: true }).fill("https://station.example");
    await first.getByRole("button", { name: "Open AirVote" }).click();
    await first.getByText("Workspace opened.", { exact: false }).waitFor();
    assert.deepEqual((await first.evaluate(() => window.calls)).slice(-1), [{
      command: "connect_server",
      args: { serverUrl: "https://station.example" },
    }]);
    assert.equal(await first.locator(".mark").evaluate(img => img.complete && img.naturalWidth > 0), true);
    await first.close();

    // The trusted configured address connects behind a hidden setup window;
    // no server field or recovery should flash during a healthy launch.
    const automatic = await pageFor({
      server_url: "https://airvote.up.railway.app/", warning: "",
    });
    await automatic.getByText("Workspace opened.", { exact: false }).waitFor();
    assert.equal(await automatic.locator("#connection-form").isHidden(), true);
    assert.equal(await automatic.locator("#recovery").isHidden(), true);
    assert.deepEqual(await automatic.evaluate(() => window.calls), [{
      command: "connect_server", args: { serverUrl: "https://airvote.up.railway.app/" },
    }]);
    await automatic.close();

    const local = await pageFor({ server_url: "http://127.0.0.1:8000", warning: "" });
    await local.getByText("Workspace opened.", { exact: false }).waitFor();
    assert.equal(await local.evaluate(() => window.calls[0].args.serverUrl), "http://127.0.0.1:8000");
    await local.close();

    // Connection failure reveals an offline retry UI, not a server-address form.
    const offline = await pageFor(
      { server_url: "https://airvote.up.railway.app/", warning: "" },
      "Cannot connect. Check the network, then try again.",
    );
    await offline.getByRole("button", { name: "Retry connection" }).waitFor();
    assert.equal(await offline.locator("#connection-form").isHidden(), true);
    assert.deepEqual((await offline.evaluate(() => window.calls)).map(x => x.command),
      ["connect_server", "reveal_setup"]);
    await offline.evaluate(() => { window.connectionError = ""; });
    await offline.getByRole("button", { name: "Retry connection" }).click();
    await offline.getByText("Workspace opened.", { exact: false }).waitFor();
    assert.equal((await offline.evaluate(() => window.calls))
      .filter(x => x.command === "connect_server").length, 2);
    await offline.close();

    // Invalid saved settings should still attempt the safe default. Untrusted
    // error content must never execute, even in the recovery screen.
    const damaged = await pageFor(
      { server_url: "https://airvote.up.railway.app/", warning: "Saved settings are damaged." },
      '<img src=x onerror="window.injected=true">',
    );
    await damaged.locator("#status.error").filter({ hasText: "<img" }).waitFor();
    assert.equal(await damaged.locator("#status img").count(), 0);
    assert.equal(await damaged.evaluate(() => window.injected), undefined);
    await damaged.getByRole("button", { name: "Connection settings" }).click();
    await damaged.getByLabel("Station server", { exact: true }).waitFor({ state: "visible" });
    await damaged.close();

    const ordinary = await browser.newPage();
    await ordinary.goto(url);
    await ordinary.getByText("Open AirVote from the installed Windows application.").waitFor();
    await ordinary.close();

    console.log("Desktop UI passed: hidden automatic startup, first setup, local server, offline retry, advanced settings, XSS-safe errors and missing bridge.");
  } finally {
    if (browser) await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
})().catch((error) => { console.error(error); process.exitCode = 1; });
