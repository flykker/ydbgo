import puppeteer from 'puppeteer-core';
const EXE = process.env.CHROME || '/usr/bin/google-chrome';
const browser = await puppeteer.launch({executablePath: EXE, headless: 'new', args: ['--no-sandbox', '--disable-gpu'], defaultViewport: {width: 1440, height: 900}});
const page = await browser.newPage();
const errors = [];
const notFound = [];
page.on('response', (r) => { if (r.status() === 404) notFound.push(r.url()); });
page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
page.on('console', (m) => { if (m.type() === 'error') errors.push('console: ' + m.text()); });

const apiCall = async (path, init) => (await fetch('http://127.0.0.1:8080' + path, init)).json();
const hash = () => page.evaluate(() => window.location.hash);
const shows = (s) => page.evaluate((x) => document.body.innerText.includes(x), s);

let ok = true;
const check = (name, cond, detail) => {
  console.log(`${name}:`, cond ? 'ok' : 'FAIL', detail ?? '');
  ok = ok && cond;
};

// open the plain dashboards route: it must resolve to the first dashboard and
// put its id into the hash (deep-linkable)
await page.goto('http://127.0.0.1:8080/#/dashboards', {waitUntil: 'networkidle0', timeout: 30000});
await new Promise((r) => setTimeout(r, 2000));
const list = await apiCall('/api/v1/dashboards');
const dashA = list.dashboards[0].id;
const nameA = list.dashboards[0].name;
check('resolve first -> #/dashboards/<id>', (await hash()) === `#/dashboards/${dashA}`, await hash());
check('shows first dashboard', await shows(nameA));

// create a second dashboard and deep-link to it
const created = await apiCall('/api/v1/dashboards', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({name: 'Hash QA Dash', config: {title: 'Hash QA Dash', refresh_interval: 30, widgets: []}}),
});
const dashB = created.id;
await page.evaluate((id) => { window.location.hash = '/dashboards/' + id; }, dashB);
await new Promise((r) => setTimeout(r, 1500));
check('deep-link switches dashboard', await shows('Hash QA Dash'));
check('hash stays on dash B', (await hash()) === `#/dashboards/${dashB}`, await hash());

// select dash A back through the Select dropdown UI -> hash must follow
const picked = await page.evaluate((name) => {
  const control = Array.from(document.querySelectorAll('.g-select-control__button')).find((b) => b.innerText.trim() === 'Hash QA Dash');
  if (!control) return false;
  control.click();
  return true;
});
check('open dashboard Select', picked);
await page.waitForSelector('.g-select-list__option-default-label', {timeout: 5000});
const pickedOption = await page.evaluate((name) => {
  const els = Array.from(document.querySelectorAll('.g-select-list__option-default-label'));
  const leaf = els.find((e) => e.textContent.trim() === name);
  if (!leaf) return false;
  leaf.click();
  return true;
}, nameA);
check('pick dash A option', pickedOption);
await new Promise((r) => setTimeout(r, 1000));
check('hash follows selection', (await hash()) === `#/dashboards/${dashA}`, await hash());
check('shows dash A after select', await shows(nameA));

// browser back returns to dash B (history + hashchange driven)
await page.goBack();
await new Promise((r) => setTimeout(r, 1200));
check('back -> hash dash B', (await hash()) === `#/dashboards/${dashB}`, await hash());
check('back -> shows dash B', await shows('Hash QA Dash'));

// bogus deep link falls back to the first dashboard
await page.evaluate(() => { window.location.hash = '/dashboards/does-not-exist-123'; });
await new Promise((r) => setTimeout(r, 1500));
check('bogus id falls back', (await hash()) === `#/dashboards/${dashA}`, await hash());

// cleanup
await apiCall(`/api/v1/dashboards/${dashB}`, {method: 'DELETE'});

const realErrors = errors.filter((e) => !e.includes('favicon') && !/Failed to load resource.*status of 404/.test(e));
const nonFav404 = notFound.filter((u) => !u.includes('favicon'));
check('no js errors', realErrors.length === 0, realErrors.join('; '));
check('no non-favicon 404', nonFav404.length === 0, JSON.stringify(nonFav404));
console.log(ok ? 'RESULT: OK' : 'RESULT: PROBLEMS');
await browser.close();
process.exit(ok ? 0 : 1);
