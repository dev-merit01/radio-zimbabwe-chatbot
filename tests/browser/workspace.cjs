/* Against an isolated, seeded development database. Never point at production. */
const { chromium } = require("playwright");
const assert = require("node:assert/strict");
(async () => {
  const browser = await chromium.launch({
    headless: true,
    ...(process.env.CHROMIUM_PATH
      ? { executablePath: process.env.CHROMIUM_PATH }
      : {}),
    args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
  });
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1000 },
    reducedMotion: "reduce",
  });
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(process.env.TEST_BASE_URL || "http://127.0.0.1:8765");
  await page.getByLabel("Username").fill("ui-operator");
  await page
    .getByLabel("Password", { exact: true })
    .fill("local-browser-test-only");
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await page
    .locator("#connection")
    .getByText("Connected", { exact: true })
    .waitFor();
  assert.equal(await page.locator(".metric").count(), 4);
  await page.screenshot({ path: "/tmp/rz-overview.png", fullPage: true });
  for (const section of [
    "incoming",
    "review",
    "catalogue",
    "archives",
    "activity",
    "connections",
  ]) {
    if (["activity", "connections"].includes(section)) {
      await page.locator('nav [data-page="administration"]').click();
      await page.locator('#page-content[aria-busy="false"]').waitFor();
      await page.locator(`#page-content [data-page="${section}"]`).click();
    } else {
      await page.locator(`nav [data-page="${section}"]`).click();
    }
    await page.locator('#page-content[aria-busy="false"]').waitFor();
    assert.equal(
      await page.locator("#page-content .loading").count(),
      0,
      section,
    );
    assert.equal(
      await page.locator("#offline-banner").isVisible(),
      false,
      section,
    );
  }
  await page.locator('nav [data-page="catalogue"]').click();
  await page.getByRole("button", { name: "Add song", exact: true }).click();
  await page.getByLabel("Artist", { exact: true }).fill("Browser Test Artist");
  await page
    .getByLabel("Title", { exact: true })
    .fill("Browser Test Song " + Date.now());
  await page.getByRole("button", { name: "Save changes" }).click();
  await page.locator("#modal").waitFor({ state: "hidden" });
  await page
    .getByRole("textbox", { name: "Search records" })
    .fill("Browser Test Artist");
  await page
    .locator("#search-form")
    .getByRole("button", { name: "Search", exact: true })
    .click();
  await page.locator('#page-content[aria-busy="false"]').waitFor();
  const row = page.locator("tbody tr").first();
  await row.getByRole("button", { name: "Verify", exact: true }).click();
  await page.getByRole("button", { name: "Save changes" }).click();
  await page.locator("#modal").waitFor({ state: "hidden" });
  await page.locator('#page-content[aria-busy="false"]').waitFor();
  assert.ok((await page.locator("tbody").innerText()).includes("verified"));
  await row.getByRole("button", { name: "Merge", exact: true }).click();
  await page.getByLabel("Search artist or title").fill("Example");
  await page
    .locator("#merge-target option")
    .filter({ hasText: "Morning Light" })
    .waitFor({ state: "attached" });
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await page.locator('nav [data-page="overview"]').click();
  await page.locator('#page-content[aria-busy="false"]').waitFor();
  const downloadPromise = page.waitForEvent("download");
  await page.getByRole("link", { name: "Export CSV", exact: true }).click();
  const download = await downloadPromise;
  assert.ok(download.suggestedFilename().endsWith(".csv"));
  await page.locator('nav [data-page="incoming"]').click();
  await page.locator('#page-content[aria-busy="false"]').waitFor();
  await page.context().setOffline(true);
  await page.locator("#offline-banner").waitFor({ state: "visible" });
  await page.context().setOffline(false);
  await page
    .locator("#connection")
    .getByText("Connected", { exact: true })
    .waitFor();
  // Mobile must be usable at real Android WebView viewport sizes.
  for (const width of [320, 360, 390, 430, 700]) {
    await page.setViewportSize({ width, height: 640 });
    const toggle = page.getByRole("button", { name: "Toggle navigation" });
    await toggle.click();
    assert.equal(await toggle.getAttribute("aria-expanded"), "true");
    assert.equal(await page.locator("#sidebar-backdrop").isVisible(), true);
    assert.equal(await page.evaluate(() => document.body.classList.contains("nav-open")), true);
    assert.equal(await page.locator("#sidebar nav").evaluate(el => getComputedStyle(el).overflowY), "auto");
    await page.locator("#sidebar-close").click();
    assert.equal(await toggle.getAttribute("aria-expanded"), "false");
    assert.equal(await page.locator("#sidebar-backdrop").isVisible(), false);
    assert.equal(await page.evaluate(() => document.body.classList.contains("nav-open")), false);

    // Open the current week's chart. It must not make the whole page pan.
    await toggle.click();
    await page.locator('nav [data-page="overview"]').click();
    await page.locator('#page-content[aria-busy="false"]').waitFor();
    assert.equal(await toggle.getAttribute("aria-expanded"), "false");
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false, "overview width " + width);
    const chartScroll = page.locator(".content-grid .table-wrap").first();
    if (await chartScroll.count()) {
      assert.equal(await chartScroll.evaluate(el => el.scrollWidth > el.clientWidth), true, "chart should scroll inside card at " + width);
    }
    await toggle.click();
    await page.locator('#sidebar-backdrop').click({position: {x: width - 12, y: 140}});
    assert.equal(await toggle.getAttribute("aria-expanded"), "false");
  }
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "Toggle navigation" }).click();
  await page.locator('nav [data-page="review"]').click();
  await page.locator('#page-content[aria-busy="false"]').waitFor();
  assert.equal(
    await page.evaluate(() => document.documentElement.scrollWidth > innerWidth),
    false,
  );
  await page.screenshot({ path: "/tmp/rz-mobile.png", fullPage: true });
  assert.deepEqual(errors, []);
  console.log(
    "Browser checks passed: login, seven sections, add/verify/merge search, CSV, offline/reconnect and mobile layout.",
  );
  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
