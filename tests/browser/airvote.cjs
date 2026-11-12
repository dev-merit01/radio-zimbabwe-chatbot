/* Requires isolated browser fixtures and a running vote worker. */
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
(async () => {
  const browser = await chromium.launch({headless: true, args: ['--no-sandbox'],
    ...(process.env.CHROMIUM_PATH ? {executablePath: process.env.CHROMIUM_PATH} : {})});
  try {
    const page = await browser.newPage({viewport: {width: 1360, height: 900}});
    const base = process.env.TEST_BASE_URL || 'http://127.0.0.1:8765';
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    async function login(name, password, station = null) {
      await page.goto(base);
      await page.getByLabel('Username', {exact: true}).fill(name);
      await page.getByLabel('Password', {exact: true}).fill(password);
      if (station) {
        const choice = page.locator('#id_chart_show optgroup[label="' + station + '"] option').first();
        const showId = await choice.getAttribute('value');
        assert.ok(showId, 'Chart show must be selectable for the station.');
        await page.getByLabel('Chart show', {exact: true}).selectOption(showId);
      }
      await page.getByRole('button', {name: 'Sign in', exact: true}).click();
      await page.locator('#connection').getByText('Connected', {exact: true}).waitFor();
    }
    async function navigate(name) {
      if (["activity", "connections"].includes(name)) {
        await page.locator('nav [data-page="administration"]').click();
        await page.locator('#page-content[aria-busy="false"]').waitFor();
        await page.locator(`#page-content [data-page="${name}"]`).click();
      } else {
        await page.locator(`nav [data-page="${name}"]`).click();
      }
      await page.locator('#page-content[aria-busy="false"]').waitFor();
    }
    async function switchTo(station, password) {
      if (password === undefined) {
        await Promise.all([page.waitForNavigation(), page.locator('#station-select').selectOption(station)]);
      } else {
        await page.locator('#station-select').selectOption(station);
        await page.getByLabel('Station password', {exact: true}).fill(password);
        await Promise.all([page.waitForNavigation(), page.locator('#station-dialog button[type="submit"]').click()]);
      }
      await page.locator('#connection').getByText('Connected', {exact: true}).waitFor();
    }
    await login('ui-operator', 'local-browser-test-only');
    await switchTo('national_fm');
    await navigate('connections');
    await page.getByRole('button', {name: 'Set station password', exact: true}).click();
    await page.locator('#modal input[name="password"]').fill('Station-browser-572!');
    await page.locator('#modal-submit').click();
    await page.locator('#modal').waitFor({state: 'hidden'});
    await navigate('incoming');
    await page.getByRole('button', {name: 'Record vote', exact: true}).click();
    await page.getByLabel('Listener reference', {exact: true}).fill('ui-local-listener');
    await page.getByLabel('Artist - Song', {exact: true}).fill('River Artist - Morning Song');
    await page.locator('#modal-submit').click();
    await page.locator('#modal').waitFor({state: 'hidden'});
    await page.getByText('Submission received!', {exact: false}).waitFor({timeout: 35000});
    await navigate('review');
    await page.getByRole('button', {name: 'Verify', exact: true}).click();
    await page.locator('#modal-submit').click();
    await page.locator('#modal').waitFor({state: 'hidden'});
    await navigate('overview');
    const summary = await page.request.get(base + '/api/workspace/overview');
    assert.equal((await summary.json()).received, 1);
    const chart = await page.request.get(base + '/api/chart/today');
    assert.equal((await chart.json()).top100[0].count, 1);
    await page.getByRole('button', {name: 'Sign out', exact: true}).click();
    await login('ui-staff', 'local-staff-test-only', 'Radio Zimbabwe');
    assert.equal(await page.locator('.station strong').textContent(), 'Radio Zimbabwe');
    assert.equal(await page.locator('#station-select').count(), 0,
      'The selected-show station session must not offer station switching.');
    const staffShows = await page.evaluate(async () =>
      (await fetch('/api/workspace/shows')).json());
    assert.equal(staffShows.shows.length, 1);
    assert.equal(await page.getByRole('button', {name: 'Record vote', exact: true}).count(), 0);
    assert.deepEqual(errors, []);
    await page.screenshot({path: '/tmp/airvote-station-chart-show.png', fullPage: true});
    console.log('AirVote browser passed: admin controls, station chart-show sign-in, worker intake and chart totals.');
  } finally { await browser.close(); }
})().catch(error => {console.error(error);process.exitCode=1;});
