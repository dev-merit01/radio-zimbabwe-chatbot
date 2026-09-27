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
    async function login(name, password) {
      await page.goto(base);
      await page.getByLabel('Username', {exact: true}).fill(name);
      await page.getByLabel('Password', {exact: true}).fill(password);
      await page.getByRole('button', {name: 'Sign in', exact: true}).click();
      await page.locator('#connection').getByText('Connected', {exact: true}).waitFor();
    }
    async function navigate(name) {
      await page.locator(`nav [data-page="${name}"]`).click();
      await page.locator('#page-content[aria-busy="false"]').waitFor();
    }
    async function switchTo(station, password) {
      await page.locator('#station-select').selectOption(station);
      if (password !== undefined) await page.getByLabel('Station password', {exact: true}).fill(password);
      await page.locator('.station-switch button').click();
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
    await page.getByText('Vote recorded!', {exact: false}).waitFor({timeout: 35000});
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
    await login('ui-staff', 'local-staff-test-only');
    await switchTo('national_fm', 'wrong-password');
    assert.equal(await page.locator('.station strong').textContent(), 'Radio Zimbabwe');
    await switchTo('national_fm', 'Station-browser-572!');
    assert.equal(await page.locator('.station strong').textContent(), 'National FM');
    assert.equal(await page.getByRole('button', {name: 'Record vote', exact: true}).count(), 0);
    assert.deepEqual(errors, []);
    await page.screenshot({path: '/tmp/airvote-station-switch.png', fullPage: true});
    console.log('AirVote browser passed: admin bypass, password gate, worker intake, review and chart totals.');
  } finally { await browser.close(); }
})().catch(error => {console.error(error);process.exitCode=1;});
