import puppeteer from 'puppeteer-core';

const URL = process.env.URL || 'http://127.0.0.1:8080/#/dashboards';
const EXE = process.env.CHROME || '/usr/bin/google-chrome';

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

await page.goto(URL, {waitUntil: 'networkidle0', timeout: 30000});
await page.waitForSelector('.react-grid-item', {timeout: 30000});
// let auto-refresh / measure settle
await new Promise((r) => setTimeout(r, 1500));

const data = await page.evaluate(() => {
  const grid = document.querySelector('.react-grid-layout');
  const container = grid ? grid.parentElement : null;
  const cw = container ? container.getBoundingClientRect().width : 0;
  const cr = container ? container.getBoundingClientRect().right : 0;
  const items = Array.from(document.querySelectorAll('.react-grid-item')).map((el) => {
    const r = el.getBoundingClientRect();
    return {left: r.left, top: r.top, right: r.right, bottom: r.bottom, width: r.width, height: r.height};
  });
  const viewport = {w: window.innerWidth, h: window.innerHeight};
  return {cw, cr, items, viewport};
});

console.log('container width:', data.cw, 'right:', data.cr);
console.log('viewport:', data.viewport.w, 'x', data.viewport.h);
console.log('items:', data.items.length);
let ok = true;
data.items.forEach((r, i) => {
  console.log(`  item${i}: left=${r.left.toFixed(0)} top=${r.top.toFixed(0)} right=${r.right.toFixed(0)} bottom=${r.bottom.toFixed(0)}`);
  if (r.right > data.cr + 1) {
    ok = false;
    console.log(`  -> item${i} OVERFLOWS container right edge by ${(r.right - data.cr).toFixed(0)}px`);
  }
  if (r.left < 0) {
    ok = false;
    console.log(`  -> item${i} left edge negative`);
  }
});
for (let i = 0; i < data.items.length; i++) {
  for (let j = i + 1; j < data.items.length; j++) {
    const a = data.items[i];
    const b = data.items[j];
    const ov = !(a.right <= b.left + 1 || b.right <= a.left + 1 || a.bottom <= b.top + 1 || b.bottom <= a.top + 1);
    if (ov) {
      ok = false;
      console.log(`  OVERLAP item${i} vs item${j}`);
    }
  }
}
const realErrors = errors.filter(
  (e) => !e.includes('favicon') && !/Failed to load resource.*status of 404/.test(e),
);
const nonFav404 = notFound.filter((u) => !u.includes('favicon'));
console.log(errors.length ? 'JS ERRORS:\n' + errors.join('\n') : 'no js errors');
console.log('404s:', nonFav404.length ? JSON.stringify(nonFav404) : 'none (favicon only)');
console.log(ok && realErrors.length === 0 && nonFav404.length === 0 ? 'RESULT: OK' : 'RESULT: PROBLEMS FOUND');
await browser.close();
process.exit(ok && realErrors.length === 0 && nonFav404.length === 0 ? 0 : 1);
