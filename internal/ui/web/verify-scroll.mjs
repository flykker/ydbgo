import puppeteer from 'puppeteer-core';
const EXE = process.env.CHROME || '/usr/bin/google-chrome';
const browser = await puppeteer.launch({executablePath: EXE, headless: 'new', args: ['--no-sandbox', '--disable-gpu'], defaultViewport: {width: 1440, height: 900}});
let ok = true;
// Short pages must fit exactly (no hard-coded fixed-height layout bug):
for (const p of ['sql', 'dashboards', 'logs']) {
  const page = await browser.newPage();
  await page.goto(`http://127.0.0.1:8080/#/${p}`, {waitUntil: 'networkidle0', timeout: 30000});
  await new Promise((r) => setTimeout(r, 1500));
  const m = await page.evaluate(() => ({
    clientH: document.documentElement.clientHeight,
    scrollH: document.documentElement.scrollHeight,
    bodyMargin: getComputedStyle(document.body).margin,
    hasScrollbar: document.documentElement.scrollHeight > document.documentElement.clientHeight,
  }));
  const noBar = !m.hasScrollbar && m.bodyMargin === '0px';
  ok = ok && noBar;
  console.log(`${p}: scrollH=${m.scrollH} clientH=${m.clientH} margin=${m.bodyMargin} scrollbar=${m.hasScrollbar}`);
  await page.close();
}
// The cluster page carries live charts and is legitimately taller than the
// viewport: only the body margin regression is checked there, plus it must be
// able to scroll.
const cluster = await browser.newPage();
await cluster.goto('http://127.0.0.1:8080/#/cluster', {waitUntil: 'networkidle0', timeout: 30000});
await new Promise((r) => setTimeout(r, 3000));
const c = await cluster.evaluate(() => ({
  scrollH: document.documentElement.scrollHeight,
  clientH: document.documentElement.clientHeight,
  bodyMargin: getComputedStyle(document.body).margin,
  scrollable: document.documentElement.scrollHeight > document.documentElement.clientHeight,
}));
console.log(`cluster: scrollH=${c.scrollH} clientH=${c.clientH} margin=${c.bodyMargin} scrollable=${c.scrollable}`);
ok = ok && c.bodyMargin === '0px' && c.scrollable;
await cluster.close();

// tall content must still scroll (600px viewport, logs page)
const page = await browser.newPage();
await page.setViewport({width: 1440, height: 600});
// The seeded demo rows live between the top of the current hour and +5h, so the
// default "1h" window can contain only a handful of rows near the top of the
// hour and the page would legitimately fit the 600px viewport. Insert fresh
// rows inside the last few minutes to make the list deterministically tall.
const nowMs = Date.now();
const vals = [];
for (let i = 0; i < 200; i++) {
  const ts = new Date(nowMs - i * 3000).toISOString();
  vals.push(`('${ts}','INFO','scroll check ${i}',0.5)`);
}
const insRes = await fetch('http://127.0.0.1:8080/api/v1/query', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({sql: `INSERT INTO logs VALUES ${vals.join(',')}`}),
});
if (!insRes.ok) throw new Error('seed insert failed: ' + insRes.status);
await page.goto('http://127.0.0.1:8080/#/logs', {waitUntil: 'networkidle0', timeout: 30000});
await page.waitForSelector('tbody tr', {timeout: 10000});
await new Promise((r) => setTimeout(r, 500));
const tall = await page.evaluate(() => ({
  scrollH: document.documentElement.scrollHeight,
  clientH: document.documentElement.clientHeight,
}));
console.log(`logs @600px: scrollH=${tall.scrollH} clientH=${tall.clientH} scrollable=${tall.scrollH > tall.clientH}`);
ok = ok && tall.scrollH > tall.clientH;
console.log(ok ? 'RESULT: OK' : 'RESULT: PROBLEMS');
await browser.close();
process.exit(ok ? 0 : 1);
