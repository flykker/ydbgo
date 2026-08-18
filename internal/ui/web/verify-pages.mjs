import puppeteer from 'puppeteer-core';
const EXE = process.env.CHROME || '/usr/bin/google-chrome';
const browser = await puppeteer.launch({executablePath: EXE, headless: 'new', args: ['--no-sandbox', '--disable-gpu'], defaultViewport: {width: 1440, height: 900}});
let ok = true;
for (const pageName of ['cluster', 'sql', 'dashboards', 'logs']) {
  const page = await browser.newPage();
  const errors = [];
  const notFound = [];
  page.on('response', (r) => { if (r.status() === 404) notFound.push(r.url()); });
  page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
  page.on('console', (m) => { if (m.type() === 'error') errors.push('console: ' + m.text()); });
  await page.goto(`http://127.0.0.1:8080/#/${pageName}`, {waitUntil: 'networkidle0', timeout: 30000});
  await new Promise((r) => setTimeout(r, 1500));
  const info = await page.evaluate(() => {
    const badge = document.querySelector('.ydbgo-logo__badge svg');
    const btn = document.querySelector('.ydbgo-collapse');
    return {
      logo: !!badge,
      collapseBtn: !!btn,
      noScrollbar: document.documentElement.scrollHeight <= document.documentElement.clientHeight + 1,
    };
  });
  const realErrors = errors.filter((e) => !e.includes('favicon') && !/Failed to load resource.*status of 404/.test(e));
  const nonFav404 = notFound.filter((u) => !u.includes('favicon'));
  const pageOk = realErrors.length === 0 && nonFav404.length === 0 && info.logo && info.collapseBtn;
  ok = ok && pageOk;
  const title = await page.evaluate(() => document.querySelector('h1, [class*="Header"]')?.innerText || document.body.innerText.slice(0, 60));
  console.log(
    `${pageName}: logo=${info.logo} collapse=${info.collapseBtn} noScrollbar=${info.noScrollbar}`,
    realErrors.length ? 'ERRORS: ' + realErrors.join('; ') : 'no errors',
    `"${title.replace(/\n/g, ' ').slice(0, 40)}"`,
  );
  await page.close();
}
console.log(ok ? 'RESULT: OK' : 'RESULT: PROBLEMS');
await browser.close();
process.exit(ok ? 0 : 1);
