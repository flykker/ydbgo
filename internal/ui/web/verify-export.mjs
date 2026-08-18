import puppeteer from 'puppeteer-core';
import fs from 'node:fs';
const EXE = process.env.CHROME || '/usr/bin/google-chrome';
const DL_DIR = process.env.DL_DIR || '/tmp/ydbgo-dl';
fs.mkdirSync(DL_DIR, {recursive: true});
for (const f of fs.readdirSync(DL_DIR)) fs.rmSync(`${DL_DIR}/${f}`, {force: true});

const browser = await puppeteer.launch({executablePath: EXE, headless: 'new', args: ['--no-sandbox', '--disable-gpu'], defaultViewport: {width: 1440, height: 900}});

// Browser-level download behavior is required for blob-URL anchor downloads in
// headless: the file lands in DL_DIR but puppeteer's 'download' event may not
// fire, so we poll the directory for the new file.
const bs = await browser.target().createCDPSession();
await bs.send('Browser.setDownloadBehavior', {behavior: 'allow', downloadPath: DL_DIR, eventsEnabled: true});

const page = await browser.newPage();
const errors = [];
const notFound = [];
page.on('response', (r) => { if (r.status() === 404) notFound.push(r.url()); });
page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
page.on('console', (m) => { if (m.type() === 'error') errors.push('console: ' + m.text()); });

// SQL page export
await page.goto('http://127.0.0.1:8080/#/sql', {waitUntil: 'networkidle0', timeout: 30000});
await page.waitForSelector('button', {timeout: 30000});
await page.evaluate(() => {
  const btns = Array.from(document.querySelectorAll('button'));
  const run = btns.find((b) => b.innerText.trim() === 'Run');
  run.click();
});
await new Promise((r) => setTimeout(r, 2000));
const hasExport = await page.evaluate(() => {
  const btns = Array.from(document.querySelectorAll('button'));
  return btns.some((b) => b.innerText.includes('Export CSV'));
});
console.log('Export CSV button present:', hasExport);

let downloaded = false;
let csv = '';
if (hasExport) {
  const before = new Set(fs.readdirSync(DL_DIR));
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    btns.find((b) => b.innerText.includes('Export CSV')).click();
  });
  let file = null;
  for (let i = 0; i < 20; i++) {
    await new Promise((r) => setTimeout(r, 500));
    const fresh = fs.readdirSync(DL_DIR).filter((f) => !before.has(f));
    if (fresh.length > 0) { file = fresh[0]; break; }
  }
  if (file) {
    csv = fs.readFileSync(`${DL_DIR}/${file}`, 'utf8');
    downloaded = true;
    console.log('download:', file, '| bytes:', csv.length);
    console.log('csv head:', csv.split('\n').slice(0, 3).join(' | '));
  }
}

const realErrors = errors.filter((e) => !e.includes('favicon') && !/Failed to load resource.*status of 404/.test(e));
const nonFav404 = notFound.filter((u) => !u.includes('favicon'));
console.log('js errors:', realErrors.length ? realErrors.join('\n') : 'none');
console.log('404s:', nonFav404.length ? JSON.stringify(nonFav404) : 'none (favicon only)');

const hasBom = downloaded && csv.charCodeAt(0) === 0xfeff;
const ok = hasExport && downloaded && hasBom && csv.split('\n').length >= 3 && realErrors.length === 0 && nonFav404.length === 0;
console.log(ok ? 'RESULT: OK' : 'RESULT: PROBLEMS');
await browser.close();
process.exit(ok ? 0 : 1);
