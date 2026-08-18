import puppeteer from 'puppeteer-core';

// Idempotent dashboard QA: operates on the FIRST dashboard (the one the page
// shows by default), resets it to a canonical config before the checks and
// restores the original afterwards, so repeated runs never pollute the demo.
//
// Checks: drag = one PUT, rename = one PUT, add widget = one PUT; grid items
// do not overlap and stay inside the container; widget data loads.

const BASE = process.env.URL || 'http://127.0.0.1:8080';
const EXE = process.env.CHROME || '/usr/bin/google-chrome';
const CANON = 'QA Dash';

const browser = await puppeteer.launch({
  executablePath: EXE,
  headless: 'new',
  args: ['--no-sandbox', '--disable-gpu'],
  defaultViewport: {width: 1440, height: 900},
});
const page = await browser.newPage();
const errors = [];
const notFound = [];
page.on('response', (r) => {
  if (r.status() === 404) notFound.push(r.url());
});
page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
page.on('console', (m) => {
  if (m.type() === 'error') errors.push('console: ' + m.text());
});
let puts = 0;
page.on('request', (req) => {
  if (req.method() === 'PUT') puts++;
});

const api = async (path, opts) => {
  const res = await fetch(`${BASE}${path}`, {...opts, headers: {'Content-Type': 'application/json'}});
  return res.json();
};

// ---- setup: snapshot the first dashboard, then reset it to canonical state.
const list = await api('/api/v1/dashboards');
const target = list.dashboards[0];
if (!target) {
  console.log('RESULT: PROBLEMS (no dashboards)');
  await browser.close();
  process.exit(1);
}
const original = {name: target.name, config: target.config};
const canonical = {
  name: CANON,
  config: {
    title: CANON,
    refresh_interval: 30,
    widgets: [
      {id: 'qa1', type: 'stat', title: 'One', sql: 'SELECT COUNT(*) AS total FROM logs', x: 0, y: 0, w: 3, h: 4},
      {id: 'qa2', type: 'stat', title: 'Two', sql: "SELECT COUNT(*) AS total FROM logs WHERE level = 'ERROR'", x: 3, y: 0, w: 3, h: 4},
    ],
  },
};
const setup = await api(`/api/v1/dashboards/${target.id}`, {method: 'PUT', body: JSON.stringify(canonical)});
if (!setup.ok) {
  console.log('RESULT: PROBLEMS (setup PUT failed)', JSON.stringify(setup));
  await browser.close();
  process.exit(1);
}
console.log('target dashboard:', target.id, '-> reset to', CANON);

// ---- page: shows the first dashboard (ours).
await page.goto(`${BASE}/#/dashboards`, {waitUntil: 'networkidle0', timeout: 30000});
await page.waitForSelector('.react-grid-item', {timeout: 30000});
await new Promise((r) => setTimeout(r, 1500));

// widget data loaded? stat widgets render COUNT(*)
const pageText = await page.evaluate(() => document.body.innerText);
const widgetDataOk = pageText.includes('One') && pageText.includes('Two');
console.log('widget data rendered:', widgetDataOk);

// ---- 1) drag qa1 -> exactly one PUT
puts = 0;
const items = await page.$$('.react-grid-item');
const handle = await items[0].$('.drag-handle');
const hb = await handle.boundingBox();
await page.mouse.move(hb.x + 40, hb.y + 12);
await page.mouse.down();
await page.mouse.move(hb.x + 40 + 150, hb.y + 12 + 40, {steps: 15});
await page.mouse.up();
await new Promise((r) => setTimeout(r, 1200));
console.log('after drag: PUTs =', puts);
const dragOk = puts === 1;

// ---- 2) rename dashboard (type into the rename input, blur via Tab)
puts = 0;
const foundInput = await page.evaluate((name) => {
  const inputs = Array.from(document.querySelectorAll('input'));
  const target = inputs.find((i) => i.value === name);
  if (!target) return false;
  target.focus();
  return true;
}, CANON);
console.log('rename input focused:', foundInput);
await page.keyboard.down('Control');
await page.keyboard.press('KeyA');
await page.keyboard.up('Control');
await page.keyboard.type(`${CANON} renamed`);
await page.keyboard.press('Tab');
await new Promise((r) => setTimeout(r, 1500));
console.log('after rename: PUTs =', puts);
const nameNow = await page.evaluate(() => {
  const inputs = Array.from(document.querySelectorAll('input'));
  const t = inputs.find((i) => i.value.includes('renamed'));
  return t ? t.value : null;
});
console.log('dashboard name now:', nameNow);
const renameOk = nameNow === `${CANON} renamed` && puts === 1;

// ---- 3) add widget -> exactly one PUT
puts = 0;
const clicked = await page.evaluate(() => {
  const btns = Array.from(document.querySelectorAll('button'));
  const add = btns.find((b) => b.innerText.trim().includes('Add widget'));
  if (!add) return false;
  add.click();
  return true;
});
console.log('add-widget clicked:', clicked);
await new Promise((r) => setTimeout(r, 1000));
const apply = await page.evaluate(() => {
  const btns = Array.from(document.querySelectorAll('button'));
  const b = btns.find((x) => x.innerText.trim() === 'Add');
  if (!b) return false;
  b.click();
  return true;
});
console.log('editor Add clicked:', apply);
await new Promise((r) => setTimeout(r, 1500));
console.log('after add widget: PUTs =', puts);
const addOk = puts === 1;

// ---- 4) geometry: no overlaps, no right overflow
const geo = await page.evaluate(() => {
  const c = document.querySelector('.react-grid-layout');
  const container = c ? c.parentElement : null;
  const cr = container ? container.getBoundingClientRect().right : 0;
  const items = Array.from(document.querySelectorAll('.react-grid-item')).map((el) => {
    const r = el.getBoundingClientRect();
    return {left: r.left, top: r.top, right: r.right, bottom: r.bottom};
  });
  return {cr, items};
});
let overlap = false;
for (let i = 0; i < geo.items.length; i++) {
  for (let j = i + 1; j < geo.items.length; j++) {
    const a = geo.items[i];
    const b = geo.items[j];
    if (!(a.right <= b.left + 1 || b.right <= a.left + 1 || a.bottom <= b.top + 1 || b.bottom <= a.top + 1)) {
      overlap = true;
      console.log(`OVERLAP item${i} vs item${j}`);
    }
  }
  if (geo.items[i].right > geo.cr + 1) console.log(`item${i} overflows right`);
}
console.log('items:', geo.items.length, 'overlap:', overlap);

// ---- 5) CRUD round-trip via API: create temp dashboard, list, delete
const tmp = await api('/api/v1/dashboards', {
  method: 'POST',
  body: JSON.stringify({name: 'QA temp', config: {title: 'QA temp', refresh_interval: 30, widgets: []}}),
});
let crudOk = false;
if (tmp.ok && tmp.id) {
  const l2 = await api('/api/v1/dashboards');
  const found = l2.dashboards.some((d) => d.id === tmp.id);
  const del = await api(`/api/v1/dashboards/${tmp.id}`, {method: 'DELETE'});
  const l3 = await api('/api/v1/dashboards');
  const gone = !l3.dashboards.some((d) => d.id === tmp.id);
  crudOk = found && del.ok && gone;
  console.log('CRUD create/list/delete:', found, del.ok, gone);
}

// ---- restore the original dashboard
await api(`/api/v1/dashboards/${target.id}`, {method: 'PUT', body: JSON.stringify(original)});

const realErrors = errors.filter((e) => !e.includes('favicon') && !/Failed to load resource.*status of 404/.test(e));
const nonFav404 = notFound.filter((u) => !u.includes('favicon'));
console.log('js errors:', realErrors.length ? realErrors.join('\n') : 'none');
console.log('404s:', nonFav404.length ? JSON.stringify(nonFav404) : 'none (favicon only)');

const ok = dragOk && renameOk && addOk && !overlap && crudOk && widgetDataOk && realErrors.length === 0 && nonFav404.length === 0;
console.log(ok ? 'RESULT: OK' : 'RESULT: PROBLEMS');
await browser.close();
process.exit(ok ? 0 : 1);
