/* Run against the isolated database populated by seed.py. */
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
(async () => {
  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'], ...(process.env.CHROMIUM_PATH ? {executablePath: process.env.CHROMIUM_PATH} : {}) });
  const page = await browser.newPage({viewport: {width: 1440, height: 1000}});
  const base = process.env.TEST_BASE_URL || 'http://127.0.0.1:8765';
  const errors = [];
  page.on('pageerror', e => errors.push(e.message));
  const username = 'member-' + Date.now();
  await page.goto(base + '/accounts/register/');
  assert.equal(await page.evaluate(() => document.documentElement.scrollHeight > innerHeight), false);
  await page.screenshot({path:'/tmp/airvote-signup.png', fullPage:true});
  await page.setViewportSize({width:1366,height:768});
  assert.equal(await page.evaluate(() => document.documentElement.scrollHeight > innerHeight), false);
  await page.setViewportSize({width:390,height:640});
  assert.equal(await page.evaluate(() => document.documentElement.scrollHeight > innerHeight || document.documentElement.scrollWidth > innerWidth), false);
  await page.setViewportSize({width:1440,height:1000});
  await page.getByLabel('First name', {exact: true}).fill('Station');
  await page.getByLabel('Last name', {exact: true}).fill('Member');
  await page.getByLabel('Username', {exact: true}).fill(username);
  await page.getByLabel('Station', {exact: true}).selectOption('national_fm');
  await page.getByLabel('Password', {exact: true}).fill('Member-unique-482!');
  await page.getByLabel('Confirm password', {exact: true}).fill('Member-unique-482!');
  await page.getByRole('button', {name: 'Create account', exact: true}).click();
  await page.getByText('Account created.', {exact: false}).waitFor();
  await page.getByLabel('Username', {exact: true}).fill('ui-operator');
  await page.getByLabel('Password', {exact: true}).fill('local-browser-test-only');
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await page.locator('#chart-options').waitFor();
  assert.ok((await page.locator('.chart-period-meta').innerText()).includes('1 vote = 1 point'));
  // Isolated browser rendering test: API fixture affects this page only,
  // never database votes, published charts or the running backend.
  const movementRoute = '**/api/chart/today?*';
  await page.route(movementRoute, async (route) => {
    const response = await route.fetch();
    const payload = await response.json();
    payload.comparison = {
      available: true, chart_id: 4567,
      chart_date: '2026-11-07', label: 'Previous weekly chart',
    };
    payload.top100 = [
      {rank:1,title:'Alpha',artists:'Artist One',count:120,
       previous_rank:2,movement_status:'up',movement_delta:1,movement:'+1'},
      {rank:2,title:'Bravo',artists:'Artist Two',count:100,
       previous_rank:1,movement_status:'down',movement_delta:-1,movement:'-1'},
      {rank:3,title:'Charlie',artists:'Artist Three',count:90,
       previous_rank:null,movement_status:'new',movement_delta:null,movement:'new'},
      {rank:4,title:'Delta',artists:'Artist Four',count:80,
       previous_rank:null,movement_status:'re_entry',movement_delta:null,movement:'re-entry'},
      {rank:5,title:'Echo',artists:'Artist Five',count:70,
       previous_rank:5,movement_status:'same',movement_delta:0,movement:'='},
    ];
    await route.fulfill({response, body:JSON.stringify(payload),
      contentType:'application/json'});
  });
  await page.getByRole('button',{name:'Show chart',exact:true}).click();
  await page.locator('.chart-movement--up').waitFor();
  assert.equal(await page.locator('.chart-movement--up').innerText(), '↑ 1');
  assert.equal(await page.locator('.chart-movement--down').innerText(), '↓ 1');
  assert.equal(await page.locator('.chart-movement--new').innerText(), 'NEW');
  assert.equal(await page.locator('.chart-movement--re_entry').innerText(), 'RE-ENTRY');
  assert.equal(await page.locator('.chart-movement--same').innerText(), '→ SAME');
  assert.equal(await page.locator('tbody tr').first().locator('.chart-previous-rank').innerText(), '#2');
  assert.equal(await page.locator('.chart-movement--up').getAttribute('aria-label'),
    'Up 1 position; previously position 2');
  const upColour = await page.locator('.chart-movement--up').evaluate((el)=>getComputedStyle(el).color);
  const downColour = await page.locator('.chart-movement--down').evaluate((el)=>getComputedStyle(el).color);
  assert.notEqual(upColour,downColour,'Positive and negative movement must be visually distinct.');
  await page.screenshot({path:'/tmp/airvote-chart-position-movement.png',fullPage:true});
  await page.unroute(movementRoute);
  await page.getByRole('button',{name:'Show chart',exact:true}).click();
  await page.locator('.chart-movement--up').waitFor({state:'detached'});
  const chartInsets = await page.evaluate(() => {
    const toolbar = document.querySelector('#chart-options');
    const note = document.querySelector('.chart-period-meta');
    const panel = toolbar.closest('.panel');
    return {
      note: note.getBoundingClientRect().left - panel.getBoundingClientRect().left +
        parseFloat(getComputedStyle(note).paddingLeft),
      toolbar: toolbar.getBoundingClientRect().left - panel.getBoundingClientRect().left +
        parseFloat(getComputedStyle(toolbar).paddingLeft),
    };
  });
  assert.ok(chartInsets.note >= 16, 'Chart date must have a comfortable panel inset.');
  assert.ok(Math.abs(chartInsets.note - chartInsets.toolbar) <= 3,
    'Chart date should align with the chart toolbar content.');
  assert.equal(await page.locator('#refresh').count(), 0);
  assert.equal(await page.locator('.breadcrumb').count(), 0);
  await page.locator('#chart-options select[name="size"]').selectOption('50');
  await page.getByRole('button', {name:'Show chart', exact:true}).click();
  await page.getByRole('heading', {name:'Live weekly Top 50',exact:true}).waitFor();
  await page.locator('#chart-options select[name="period"]').selectOption('year_end');
  await page.getByRole('button', {name:'Show chart',exact:true}).click();
  await page.getByRole('heading', {name:'Year-to-date Top 50',exact:true}).waitFor();
  await page.locator('nav [data-page="administration"]').click();
  await page.locator('nav [data-page="stations"]').click();
  await page.locator('.network-grid').waitFor();
  assert.equal(await page.locator('.network-station-card').count(), 7);
  await page.getByRole('button', {name:'+ Add station', exact:true}).click();
  await page.locator('#modal-content [name="name"]').fill('Browser Test Coastal FM');
  await page.locator('#modal-content [name="bird_number"]').fill('263790119922');
  await page.getByRole('button', {name:'Save changes', exact:true}).click();
  await page.locator('#modal').waitFor({state:'hidden'});
  await page.locator('.network-station-card').filter({hasText:'Browser Test Coastal FM'}).waitFor();
  assert.equal(await page.locator('#station-select option[value="browser_test_coastal_fm"]').count(),1);
  assert.equal(await page.locator('.network-station-card').count(),8);
  await page.locator('.network-station-card').filter({hasText:'Browser Test Coastal FM'}).getByRole('button', {name:'Configure station'}).click();
  assert.equal(await page.locator('#modal-content [name="station_active"]').inputValue(), 'false');
  await page.getByRole('button', {name:'Cancel',exact:true}).click();
  await page.locator('nav [data-page="administration"]').click();
  const account = page.locator('tbody tr').filter({hasText:username});
  await account.getByRole('button', {name:'Approve access', exact:true}).click();
  await page.getByRole('button', {name:'Save changes',exact:true}).click();
  await page.locator('#modal').waitFor({state:'hidden'});
  await account.getByText('Active', {exact:true}).waitFor();
  await page.screenshot({path:'/tmp/airvote-admin.png',fullPage:true});
  await page.locator('nav [data-page="shows"]').click();
  await page.locator('.show-grid').waitFor();
  await page.locator('#show-spam-title').waitFor();
  assert.equal(await page.locator('#show-spam-title').innerText(), 'Suspected spam votes');
  assert.ok((await page.locator('.show-spam-count').innerText()).includes('0 flagged'));

  assert.equal(await page.locator('.show-multiplier-current').innerText(), '×1');
  await page.getByRole('button', {name:'Change multiplier', exact:true}).click();
  await page.locator('#modal-content select[name="multiplier"]').waitFor();
  await page.locator('#modal-content select[name="multiplier"]').selectOption('3');
  assert.equal(await page.locator('#modal-content select[name="multiplier"]').inputValue(), '3');
  await page.getByRole('button', {name:'Cancel',exact:true}).click();
  assert.equal(await page.locator('.show-card').count(), 4);
  assert.equal(await page.locator('.show-code').filter({hasText:'GP20'}).count(), 1);
  assert.equal(await page.locator('.show-code').filter({hasText:'G20'}).count(), 1);
  assert.equal(await page.locator('.show-presenter').filter({hasText:'Thulani Siziba'}).count(), 1);
  assert.equal(await page.locator('.show-vote-example').filter({hasText:'Artist - Song'}).count(), 3);
  assert.equal(await page.locator('.show-status').filter({hasText:'Disabled'}).count(), 3);
  await page.screenshot({path:'/tmp/airvote-shows.png',fullPage:true});
  await page.getByRole('button', {name:'Edit settings',exact:true}).first().click();
  await page.locator('.show-form').waitFor();
  assert.equal(await page.locator('.show-form-section[data-show-mode="manual"]').isVisible(), true);
  assert.equal(await page.locator('.show-form-section[data-show-mode="campaign"]').isVisible(), false);
  assert.ok(await page.locator('.show-form [name="presenter_name"]').count());
  assert.ok(await page.locator('.show-form [name="broadcast_start"]').count());
  assert.equal(await page.locator('.show-form [name="chart_size"] option').count(), 4);
  await page.locator('.show-form [name="schedule_mode"]').selectOption('weekly');
  assert.equal(await page.locator('.show-form-section[data-show-mode="weekly"]').isVisible(), true);
  await page.locator('.show-form [name="kind"]').selectOption('year_end');
  assert.equal(await page.locator('.show-form [name="chart_size"]').inputValue(), '50');
  assert.equal(await page.locator('.show-form [name="schedule_mode"]').inputValue(), 'campaign');
  assert.equal(await page.locator('.show-form-section[data-show-mode="campaign"]').isVisible(), true);
  await page.getByRole('button', {name:'Cancel',exact:true}).click();
  await page.setViewportSize({width:390,height:740});
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  await page.setViewportSize({width:1440,height:1000});
  await page.locator('nav [data-page="eligibility"]').click();
  await page.locator('.eligible-page').waitFor();
  await page.getByRole('heading',{name:'Choose the songs that can compete'}).waitFor();
  await page.getByRole('button',{name:'+ Create edition', exact:true}).waitFor();
  await page.locator('.eligible-start').waitFor();
  assert.ok((await page.locator('.eligible-start').innerText()).includes('No shortlist prepared yet'));
  const starterHeight = await page.locator('.eligible-start').evaluate((el)=>el.getBoundingClientRect().height);
  assert.ok(starterHeight < 250, 'Empty shortlist guidance should not waste screen space.');
  await page.getByRole('button',{name:'Create first edition',exact:true}).click();
  await page.locator('#modal-content input[name="chart_date"]').waitFor();
  const suggestedDate = await page.locator('#modal-content input[name="chart_date"]').inputValue();
  assert.match(suggestedDate,/^\d{4}-\d{2}-\d{2}$/);
  assert.equal(new Date(suggestedDate+'T12:00:00Z').getUTCDay(), 6,
    'Radio Zimbabwe Saturday edition must default to Saturday.');
  await page.getByRole('button',{name:'Save changes',exact:true}).click();
  await page.locator('#modal').waitFor({state:'hidden'});
  await page.locator('.eligible-status--draft').waitFor();
  await page.locator('.eligible-search-controls input').waitFor();
  // Presenter catalogue search resolves while typing; Enter/Search is optional.
  await page.locator('#eligible-search-input').fill('Morning Light');
  await page.locator('.eligible-search tbody tr').filter({hasText:'Morning Light'}).first().waitFor();
  await page.locator('#eligible-search-input').fill('no-such-record-eligible-999');
  await page.getByText('No matching verified songs',{exact:true}).first().waitFor();
  assert.equal(await page.locator('#eligible-search-input').inputValue(), 'no-such-record-eligible-999');
  await page.getByRole('button',{name:'Propose a new song',exact:true}).click();
  await page.locator('#modal-content input[name="artist"]').fill('Test Studio Artist');
  await page.locator('#modal-content input[name="title"]').fill('Test New Broadcast Song');
  await page.locator('#modal input[name="featured_artists"]').fill('Feature One');
  await page.getByRole('button',{name:'+ Add featured artist'}).click();
  assert.equal(await page.locator('#modal input[name="featured_artists"]').count(), 2);
  await page.locator('#modal input[name="featured_artists"]').nth(1).fill('Feature To Remove');
  await page.getByRole('button',{name:'Remove featured artist'}).last().click();
  assert.equal(await page.locator('#modal input[name="featured_artists"]').count(), 1);
  await page.getByRole('button',{name:'+ Add featured artist'}).click();
  await page.locator('#modal input[name="featured_artists"]').nth(1).fill('Feature Two');
  await page.getByRole('button',{name:'Save changes',exact:true}).click();
  await page.locator('#modal').waitFor({state:'hidden'});
  await page.getByText('Test New Broadcast Song',{exact:true}).waitFor();
  const featuredProposal = page.locator('.eligible-suggestions tbody tr')
    .filter({hasText:'Test New Broadcast Song'});
  await featuredProposal.waitFor();
  const proposalCredits = await featuredProposal.innerText();
  assert.ok(proposalCredits.includes('Feature One') &&
    proposalCredits.includes('Feature Two'),
    'Both credited artists should be visible in the proposal: ' + proposalCredits);
  await page.getByRole('button',{name:'Link verified song',exact:true}).click();
  await page.locator('#eligible-proposal-query').fill('Morning Light');
  await page.locator('.eligible-proposal-option').filter({hasText:'Morning Light'}).first().waitFor();
  await page.locator('#eligible-proposal-query').fill('nonexistent-proposal-test-999');
  await page.getByText('No verified catalogue match.',{exact:false}).waitFor();
  await page.getByRole('button',{name:'Cancel',exact:true}).click();
  await page.setViewportSize({width:390,height:740});
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false,
    'Eligible songs must not overflow on a narrow presenter screen.');
  await page.screenshot({path:'/tmp/airvote-eligible-presenter-ui.png',fullPage:true});
  await page.setViewportSize({width:1440,height:1000});
  await page.locator('nav [data-page="presenters"]').click();
  await page.locator('.presenter-page').waitFor();
  await page.getByRole('button', {name:'Create presenter account', exact:true}).click();
  await page.locator('#modal-content [name="chart_show_id"]').waitFor();
  await page.getByRole('button', {name:'Cancel', exact:true}).click();
  await page.locator('nav [data-page="archives"]').click();
  // Archive year searches update when a valid 4-digit year is entered.
  const archiveYear = page.locator('#archive-year-search');
  await archiveYear.fill('2025');
  await page.waitForFunction(() =>
    document.querySelector('#archive-year-search')?.value === '2025' &&
    document.querySelector("#page-content[aria-busy='false']") &&
    document.querySelector('#page-content .panel')
  );
  await archiveYear.fill('');
  await page.waitForFunction(() =>
    document.querySelector("#page-content[aria-busy='false']") &&
    document.querySelector('#archive-year-search')?.value.length === 4
  );
  // Eligible songs keeps the selected programme, so explicitly return to the
  // historical chart when testing the original legacy publication controls.
  await page.locator('#chart-show-select').selectOption({label:'Historical station chart'});
  await page.getByRole('button', {name:'Save Saturday chart',exact:true}).click();
  assert.ok((await page.locator('#modal').innerText()).includes('Sunday'));
  await page.getByRole('button', {name:'Cancel',exact:true}).click();
  await page.getByRole('button', {name:'Save December Top 50',exact:true}).click();
  assert.ok((await page.locator('#modal').innerText()).includes('annual campaign'));
  await page.getByRole('button', {name:'Cancel',exact:true}).click();
  await page.getByRole('button', {name:'Sign out',exact:true}).click();
  await page.getByLabel('Username', {exact:true}).fill(username);
  await page.getByLabel('Password', {exact:true}).fill('Member-unique-482!');
  await page.getByRole('button', {name:'Sign in',exact:true}).click();
  await page.getByRole('heading',{name:'No chart show assigned',exact:true}).waitFor();
  assert.equal(await page.locator('#chart-options').count(),0);
  assert.equal(await page.locator('nav [data-page="administration"]').count(),0);
  assert.equal(await page.locator('nav [data-page="shows"]').count(),0);
  assert.equal(await page.locator('nav [data-page="catalogue"]').count(),0);
  assert.ok((await page.locator('.station').innerText()).includes('National FM'));
  await page.screenshot({path:'/tmp/airvote-awaiting-assignment.png',fullPage:true});
  await page.getByRole('link', {name:'Open your profile',exact:true}).click();
  await page.getByRole('heading', {name:'Your profile',exact:true}).waitFor();
  await page.getByLabel('First name', {exact:true}).fill('Updated');
  await page.getByRole('button', {name:'Save profile',exact:true}).click();
  await page.getByText('Profile updated.', {exact:true}).waitFor();
  await page.screenshot({path:'/tmp/airvote-profile.png',fullPage:true});
  await page.getByRole('link', {name:'Back to workspace',exact:true}).click();
  await page.getByRole('heading',{name:'No chart show assigned',exact:true}).waitFor();
  assert.equal(await page.locator('#chart-options').count(),0);
  assert.equal(await page.locator('#refresh').count(),0);
  assert.deepEqual(errors,[]);
  await browser.close();
  console.log('Passed: registration, approval, assignment-required sign-in, admin-only controls, Top 20/50 and archive forms.');
})().catch(e=>{console.error(e);process.exit(1);});
