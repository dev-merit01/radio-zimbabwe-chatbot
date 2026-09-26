/* Tests the bundled UI with a mocked native bridge; native acceptance runs on Windows. */
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
  };
  const server = http.createServer((req, res) => {
    const asset = files[req.url];
    if (!asset) {
      res.writeHead(404).end();
      return;
    }
    res.setHeader("Content-Type", asset[1]);
    res.end(
      fs.readFileSync(path.join(__dirname, "../../desktop/ui", asset[0])),
    );
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
      const page = await browser.newPage({
        viewport: { width: 780, height: 660 },
      });
      await page.addInitScript(
        ({ config, error }) => {
          window.calls = [];
          window.connectionError = error;
          window.__TAURI__ = {
            core: {
              invoke: async (command, args) => {
                if (command === "get_configuration") return config;
                window.calls.push({ command, args });
                await new Promise((resolve) => setTimeout(resolve, 80));
                if (window.connectionError) throw window.connectionError;
              },
            },
          };
        },
        { config, error },
      );
      await page.goto(url);
      return page;
    }
    const first = await pageFor({ server_url: "", warning: "" });
    assert.equal(await first.evaluate(() => window.calls.length), 0);
    await first
      .getByLabel("Station server", { exact: true })
      .fill("https://station.example");
    await first.getByRole("button", { name: "Open Voting Studio" }).click();
    await first.getByText("Workspace opened.", { exact: false }).waitFor();
    assert.deepEqual(await first.evaluate(() => window.calls), [
      {
        command: "connect_server",
        args: { serverUrl: "https://station.example" },
      },
    ]);
    await first.screenshot({ path: "/tmp/voting-desktop-setup.png" });
    await first.close();

    const offline = await pageFor(
      { server_url: "https://station.example/", warning: "" },
      "Cannot connect. Check the server address and network, then try again.",
    );
    await offline.getByText("Cannot connect.", { exact: false }).waitFor();
    assert.equal(await offline.getByRole("button").isEnabled(), true);
    await offline.evaluate(() => {
      window.connectionError = "";
    });
    await offline.getByRole("button").click();
    await offline.getByText("Workspace opened.", { exact: false }).waitFor();
    assert.equal(await offline.evaluate(() => window.calls.length), 2);
    await offline.close();

    const damaged = await pageFor({
      server_url: "https://station.example/",
      warning: "Saved settings are damaged.",
    });
    await damaged.getByText("Saved settings are damaged.").waitFor();
    assert.equal(await damaged.evaluate(() => window.calls.length), 0);
    await damaged.evaluate(() => {
      window.connectionError = '<img src=x onerror="window.injected=true">';
    });
    await damaged.getByRole("button").click();
    await damaged
      .locator("#status.error")
      .filter({ hasText: "<img" })
      .waitFor();
    assert.equal(await damaged.locator("#status img").count(), 0);
    assert.equal(await damaged.evaluate(() => window.injected), undefined);
    await damaged.close();

    const ordinary = await browser.newPage();
    await ordinary.goto(url);
    await ordinary
      .getByText("Open Voting Studio from the installed Windows application.")
      .waitFor();
    await ordinary.close();
    console.log(
      "Desktop UI passed: first setup, saved-server launch, offline retry, damaged settings, safe errors and missing native bridge.",
    );
  } finally {
    if (browser) await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
