import puppeteer from 'puppeteer-core';
const EXE = process.env.CHROME || '/usr/bin/google-chrome';
const browser = await puppeteer.launch({executablePath: EXE, headless: 'new', args: ['--no-sandbox', '--disable-gpu'], defaultViewport: {width: 1600, height: 1000}});
const page = await browser.newPage();
const errors = [];
const notFound = [];
page.on('response', (r) => { if (r.status() === 404) notFound.push(r.url()); });
page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
page.on('console', (m) => { if (m.type() === 'error') errors.push('console: ' + m.text()); });

await page.goto('http://127.0.0.1:8080/#/cluster', {waitUntil: 'networkidle0', timeout: 30000});
await page.waitForSelector('[data-qa="metrics-node-n1"]', {timeout: 30000});
await new Promise((r) => setTimeout(r, 8000));

const snapshot = () =>
  page.evaluate(() => ({
    cards: Array.from(document.querySelectorAll('[data-qa^="chart-"]')).map((c) => ({
      qa: c.getAttribute('data-qa'),
      svg: !!c.querySelector('svg'),
      paths: c.querySelectorAll('path').length,
      htmlLen: c.innerHTML.length,
    })),
    chips: Array.from(document.querySelectorAll('[data-qa^="metrics-node-"]')).map((b) => b.textContent.trim()),
  }));

const before = await snapshot();
console.log('initial charts:', JSON.stringify(before.cards));

// generate load: writes via /api/v1/ingest, reads via /api/v1/query
for (let i = 0; i < 6; i++) {
  await fetch('http://127.0.0.1:8080/api/v1/ingest', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      table: 'logs',
      columns: ['ts', 'level', 'msg', 'lat'],
      rows: [['2026-08-17T10:00:00Z', 'info', `verify-metrics ${i}`, 0.3 + i]],
    }),
  });
}
for (let i = 0; i < 6; i++) {
  await fetch('http://127.0.0.1:8080/api/v1/query', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({sql: `SELECT count(*) FROM logs`}),
  });
}

await new Promise((r) => setTimeout(r, 12000));
const after = await snapshot();

const allSvgs = (s) => s.cards.every((c) => c.svg);
const chartGrew = (s1, s2, qa) => {
  const a = s1.cards.find((c) => c.qa === qa);
  const b = s2.cards.find((c) => c.qa === qa);
  return !!a && !!b && b.paths > 0;
};

// node selector: click n1 chip, charts must keep rendering
await page.click('[data-qa="metrics-node-n1"]');
await new Promise((r) => setTimeout(r, 2000));
const single = await snapshot();
const singleSvgs = single.cards.every((c) => c.svg);
const chipsHaveNodes = before.chips.length >= 2 && before.chips.some((t) => t.startsWith('n1'));

const realErrors = errors.filter((e) => !e.includes('favicon') && !/Failed to load resource.*status of 404/.test(e));
const nonFav404 = notFound.filter((u) => !u.includes('favicon'));

console.log('after load:', JSON.stringify(after.cards));
console.log('single-node charts:', JSON.stringify(single.cards));
console.log('js errors:', realErrors.length ? realErrors.join('\n') : 'none');
console.log('404s:', nonFav404.length ? JSON.stringify(nonFav404) : 'none (favicon only)');

const ok =
  before.cards.length === 4 &&
  allSvgs(before) &&
  allSvgs(after) &&
  chartGrew(before, after, 'chart-write-rps') &&
  chartGrew(before, after, 'chart-read-rps') &&
  chipsHaveNodes &&
  singleSvgs &&
  realErrors.length === 0 &&
  nonFav404.length === 0;
console.log(ok ? 'RESULT: OK' : 'RESULT: PROBLEMS');
await browser.close();
process.exit(ok ? 0 : 1);
