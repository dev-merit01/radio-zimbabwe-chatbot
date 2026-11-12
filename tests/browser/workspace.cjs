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
  assert.equal(await page.locator("html").getAttribute("data-theme"), "light");
  await page.getByRole("button", { name: "Switch to dark mode" }).click();
  assert.equal(await page.locator("html").getAttribute("data-theme"), "dark");
  await page.getByRole("button", { name: "Switch to light mode" }).click();
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
  // Both themes must work without changing navigation or voting state.
  assert.equal(await page.locator("html").getAttribute("data-theme"), "light");
  assert.equal(
    await page.locator(".panel").first().evaluate(el => getComputedStyle(el).backgroundColor),
    "rgb(255, 255, 255)",
  );
  await page.getByRole("button", { name: "Switch to dark mode" }).click();
  assert.equal(await page.locator("html").getAttribute("data-theme"), "dark");
  assert.equal(
    await page.locator(".panel").first().evaluate(el => getComputedStyle(el).backgroundColor),
    "rgb(26, 43, 64)",
  );
  assert.equal(
    await page.evaluate(() => localStorage.getItem("airvote.appearance.theme")),
    "dark",
  );
  await page.screenshot({ path: "/tmp/airvote-dark-overview.png", fullPage: true });
  // The same appearance preference follows staff onto the profile screen.
  await page.goto(new URL("/accounts/profile/", page.url()).href);
  assert.equal(await page.locator("html").getAttribute("data-theme"), "dark");
  assert.equal(
    await page.locator(".profile-page .panel").first().evaluate(el => getComputedStyle(el).backgroundColor),
    "rgb(26, 43, 64)",
  );
  await page.getByRole("link", { name: "Back to workspace" }).click();
  await page.locator("#connection").getByText("Connected", { exact: true }).waitFor();
  for (const section of ["review", "catalogue", "shows", "eligibility"]) {
    await page.locator('nav [data-page="' + section + '"]').click();
    await page.locator('#page-content[aria-busy="false"]').waitFor();
    assert.equal(await page.locator("html").getAttribute("data-theme"), "dark");
    const surface = await page.locator(".panel").first()
      .evaluate(el => getComputedStyle(el).backgroundColor);
    assert.ok(
      ["rgb(26, 43, 64)", "rgb(32, 51, 75)"].includes(surface),
      "Dark panel in " + section + ": " + surface,
    );
  }
  await page.reload();
  await page.locator("#connection").getByText("Connected", { exact: true }).waitFor();
  assert.equal(await page.locator("html").getAttribute("data-theme"), "dark");
  await page.locator("#appearance-open").click();
  await page.locator("#appearance-dialog").waitFor({ state: "visible" });
  await page.locator('#appearance-dialog input[value="light"]').check();
  assert.equal(await page.locator("html").getAttribute("data-theme"), "light");
  await page.getByRole("button", { name: "Done" }).click();
  await page.locator("#appearance-dialog").waitFor({ state: "hidden" });
  await page.reload();
  await page.locator("#connection").getByText("Connected", { exact: true }).waitFor();
  assert.equal(await page.locator("html").getAttribute("data-theme"), "light");
  assert.equal(
    await page.locator(".panel").first().evaluate(el => getComputedStyle(el).backgroundColor),
    "rgb(255, 255, 255)",
  );
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
  await page.getByLabel("Main artist", { exact: true }).fill("Browser Test Artist");
  await page
    .getByLabel("Song title", { exact: true })
    .fill("Browser Test Song " + Date.now());
  await page.locator('#modal input[name="featured_artists"]').fill("Test Guest");
  await page.getByRole("button", { name: "+ Add featured artist" }).click();
  assert.equal(await page.locator('#modal input[name="featured_artists"]').count(), 2);
  await page.locator('#modal input[name="featured_artists"]').nth(1).fill("Second Guest");
  await page.getByRole("button", { name: "Save changes" }).click();
  await page.locator("#modal").waitFor({ state: "hidden" });
  // Main catalogue search must update without submitting the form, preserve
  // focus and restore the full catalogue when the query is cleared.
  const liveSearch = page.getByRole("searchbox", { name: "Search records" });
  await liveSearch.fill("no-such-record-in-the-catalogue-999");
  await page.locator("#page-content .panel .empty").waitFor({state:"visible"});
  assert.equal(await liveSearch.inputValue(), "no-such-record-in-the-catalogue-999");
  await liveSearch.fill("Browser Test Artist");
  await page.locator("tbody tr").filter({hasText:"Browser Test Artist"}).first().waitFor();
  await page.waitForFunction(() => document.querySelector("#page-content .table-footer")?.textContent.includes("1 records"));
  assert.equal(await liveSearch.evaluate(el=>document.activeElement===el), true);
  await liveSearch.fill("");
  await page.waitForFunction(() => {
    const rows = document.querySelectorAll("#page-content tbody tr");
    return rows.length >= 3 && document.querySelector("#page-content[aria-busy='false']");
  });
  await liveSearch.fill("Browser Test Artist");
  await page.waitForFunction(() =>
    document.querySelector("#page-content .table-footer")?.textContent.includes("1 records") &&
    document.querySelector("#page-content[aria-busy='false']")
  );
  const row = page.locator("tbody tr").first();
  assert.ok((await row.innerText()).includes("Browser Test Artist feat. Test Guest, Second Guest"));
  await row.getByRole("button", { name: "Edit", exact: true }).click();
  assert.equal(await page.getByLabel("Main artist", { exact: true }).inputValue(), "Browser Test Artist");
  assert.deepEqual(
    await page.locator('#modal input[name="featured_artists"]').evaluateAll(
      nodes => nodes.map(node => node.value)
    ),
    ["Test Guest", "Second Guest"],
  );
  await page.locator('#modal input[name="featured_artists"]').nth(1).fill("New Guest");
  await page.getByRole("button", { name: "Save changes" }).click();
  await page.locator("#modal").waitFor({ state: "hidden" });
  await page.locator("tbody tr").filter({hasText:"New Guest"}).first().waitFor();
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
  await page.getByLabel("Search artist or title").fill("");
  assert.equal(await page.locator("#merge-target option").count(), 1);
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await page.locator('nav [data-page="overview"]').click();
  await page.locator('#page-content[aria-busy="false"]').waitFor();
  const downloadPromise = page.waitForEvent("download");
  await page.getByRole("link", { name: "Export CSV", exact: true }).click();
  const download = await downloadPromise;
  assert.ok(download.suggestedFilename().endsWith(".csv"));
  await page.locator('nav [data-page="incoming"]').click();
  await page.locator('#page-content[aria-busy="false"]').waitFor();
  const voteSearch = page.getByRole("searchbox", { name: "Search records" });
  await voteSearch.fill("unmatched-vote-search-999");
  await page.locator("#page-content .panel .empty").waitFor({state:"visible"});
  await voteSearch.fill("Example Artist");
  await page.locator("tbody tr").filter({hasText:"Example Artist"}).first().waitFor();
  assert.equal(await voteSearch.inputValue(),"Example Artist");
  // The search filters while typing; a separate Search button is not needed.
  assert.equal(await page.locator("#search-form button[type=submit]").count(), 0);
  // Incoming-vote cards show their labels and fit the phone viewport without
  // hiding the incoming channel, listener or vote text.
  await page.setViewportSize({ width: 390, height: 844 });
  const incomingCards = page.locator("#page-content .table-wrap--cards").first();
  assert.equal(await incomingCards.count(), 1);
  assert.ok(await incomingCards.locator('td[data-label="Vote"]').count() > 0);
  assert.ok(await incomingCards.locator('td[data-label="Source"]').count() > 0);
  assert.equal(await incomingCards.evaluate(el =>
    el.querySelector("table").getBoundingClientRect().width <=
    el.getBoundingClientRect().width + 1
  ), true, "incoming vote cards must not overflow their panel");
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  const submissions = page.locator("#page-content .panel").filter({
    has: page.getByRole("heading", { name: "Recent submissions" }),
  });
  if (await submissions.count()) {
    assert.ok(await submissions.locator('td.submission-result[data-label="Result"]').count() > 0);
    assert.equal(await submissions.locator(".table-wrap--cards").count(), 1);
  }
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
      const dimensions = await chartScroll.evaluate(el => ({ container: el.clientWidth, table: el.querySelector("table")?.scrollWidth || 0, scrolling: el.scrollWidth > el.clientWidth }));
      if (dimensions.table > dimensions.container) assert.equal(dimensions.scrolling, true, "chart should scroll inside card at " + width);
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
  // Dark remains usable in a compact Android WebView. Appearance is reachable
  // from the drawer and switching it closes the drawer before opening dialog.
  await page.getByRole("button", { name: "Switch to dark mode" }).click();
  assert.equal(await page.locator("html").getAttribute("data-theme"), "dark");
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  await page.getByRole("button", { name: "Toggle navigation" }).click();
  await page.locator("#appearance-open").click();
  await page.locator("#appearance-dialog").waitFor({ state: "visible" });
  assert.equal(await page.locator("#sidebar-backdrop").isVisible(), false);
  assert.equal(await page.locator("#appearance-dialog input[value='dark']").isChecked(), true);
  await page.locator("#appearance-close").click();
  await page.screenshot({ path: "/tmp/airvote-dark-mobile.png", fullPage: true });
  await page.getByRole("button", { name: "Switch to light mode" }).click();
  assert.equal(await page.locator("html").getAttribute("data-theme"), "light");
  assert.deepEqual(errors, []);
  console.log(
    "Browser checks passed: login, seven sections, add/verify/merge search, CSV, offline/reconnect and mobile layout.",
  );
  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
