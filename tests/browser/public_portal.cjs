/* Public portal browser smoke test: no payment or OTP requests are sent. */
const { chromium } = require('playwright');
const assert = require('node:assert/strict');

(async () => {
  const browser = await chromium.launch({
    headless: true, args: ['--no-sandbox'],
    ...(process.env.CHROMIUM_PATH ? {executablePath: process.env.CHROMIUM_PATH} : {}),
  });
  try {
    const page = await browser.newPage({viewport: {width: 1366, height: 900}});
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    const base = process.env.TEST_BASE_URL || 'http://127.0.0.1:8765';
    const response = await page.goto(base + '/vote/');
    assert.equal(response.status(), 200);
    await page.getByRole('heading', {name:'Which station is your favourite?'}).waitFor();
    const stations = page.locator('#station-list .station-card');
    await stations.first().waitFor();
    assert.ok(await stations.count() > 0);
    await stations.first().click();
    await page.locator('#show-list .show-card').first().waitFor();
    await page.locator('#show-list .show-card').first().click();
    await page.locator('#voting-status').waitFor();
    assert.equal(await page.locator('#vote-button').isDisabled(), true);
    assert.equal(await page.locator('#send-code').isDisabled(), true);
    assert.equal(await page.locator('#song-search').isEnabled(), true);
    assert.equal(await page.locator('#chart-options').count(), 0,
      'Private presenter chart options must not be exposed on the public page.');
    const songs = page.locator('#song-list .song-card');
    if (await songs.count()) {
      await songs.first().click();
      assert.ok((await page.locator('#selected-song').innerText()).length > 0);
      assert.equal(await page.locator('#vote-button').isDisabled(), true);
    }
    await page.setViewportSize({width:390,height:740});
    assert.equal(await page.evaluate(() =>
      document.documentElement.scrollWidth > innerWidth), false,
      'Public voting portal must not overflow on mobile.');
    assert.deepEqual(errors,[]);
    console.log('Public portal passed: unauthenticated station/show browsing, disabled safeguards, mobile layout.');
  } finally {
    await browser.close();
  }
})().catch(err => {console.error(err);process.exitCode=1;});
