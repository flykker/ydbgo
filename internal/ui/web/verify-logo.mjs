import puppeteer from 'puppeteer-core';
const EXE = process.env.CHROME || '/usr/bin/google-chrome';
const browser = await puppeteer.launch({executablePath: EXE, headless: 'new', args: ['--no-sandbox', '--disable-gpu'], defaultViewport: {width: 1440, height: 900}});
const page = await browser.newPage();
const errors = [];
const notFound = [];
page.on('response', (r) => { if (r.status() === 404) notFound.push(r.url()); });
page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
page.on('console', (m) => { if (m.type() === 'error') errors.push('console: ' + m.text()); });

await page.goto('http://127.0.0.1:8080/#/cluster', {waitUntil: 'networkidle0', timeout: 30000});
await new Promise((r) => setTimeout(r, 1500));

const exp = await page.evaluate(() => {
  const center = (el) => { const r = el.getBoundingClientRect(); return Math.round(r.left + r.width / 2); };
  const left = (el) => Math.round(el.getBoundingClientRect().left);
  const badge = document.querySelector('.ydbgo-logo__badge svg');
  const menuIcon = document.querySelector('[class*="gn-aside-header__menu-item"] svg');
  const aside = document.querySelector('[class*="gn-aside-header__aside"]');
  const btn = document.querySelector('.ydbgo-collapse');
  const logo = document.querySelector('.ydbgo-logo');
  return {
    boltCenter: center(badge),
    menuIconCenter: menuIcon ? center(menuIcon) : null,
    logoText: document.querySelector('.ydbgo-logo__text')?.innerText ?? null,
    logoTextLeft: document.querySelector('.ydbgo-logo__text') ? left(document.querySelector('.ydbgo-logo__text')) : null,
    menuTitleLeft: left(document.querySelector('[class*="gn-composite-bar-item__title"]')),
    btnBottom: Math.round(btn.getBoundingClientRect().bottom),
    asideBottom: Math.round(aside.getBoundingClientRect().bottom),
    logoTop: Math.round(logo.getBoundingClientRect().top),
  };
});
console.log('expanded:', JSON.stringify(exp));

await page.evaluate(() => document.querySelector('.ydbgo-collapse').click());
await new Promise((r) => setTimeout(r, 800));
const comp = await page.evaluate(() => {
  const center = (el) => { const r = el.getBoundingClientRect(); return Math.round(r.left + r.width / 2); };
  const badge = document.querySelector('.ydbgo-logo__badge svg');
  const aside = document.querySelector('[class*="gn-aside-header__aside"]');
  const menuIcon = document.querySelector('[class*="gn-aside-header__menu-item"] svg');
  return {
    boltCenter: center(badge),
    asideCenter: center(aside),
    menuIconCenter: menuIcon ? center(menuIcon) : null,
    textHidden: !document.querySelector('.ydbgo-logo__text'),
  };
});
console.log('collapsed:', JSON.stringify(comp));

const realErrors = errors.filter((e) => !e.includes('favicon') && !/Failed to load resource.*status of 404/.test(e));
const nonFav404 = notFound.filter((u) => !u.includes('favicon'));

const ok =
  exp.boltCenter === exp.menuIconCenter &&
  exp.logoText === 'YADBGO' &&
  exp.logoTextLeft === exp.menuTitleLeft &&
  exp.asideBottom - exp.btnBottom <= 20 && // collapse button pinned to the bottom of the sidebar
  comp.boltCenter === comp.asideCenter &&
  comp.textHidden &&
  realErrors.length === 0 &&
  nonFav404.length === 0;
console.log('js errors:', realErrors.length ? realErrors.join('\n') : 'none');
console.log('404s:', nonFav404.length ? JSON.stringify(nonFav404) : 'none (favicon only)');
console.log(ok ? 'RESULT: OK' : 'RESULT: PROBLEMS');
await page.screenshot({path: '/tmp/ui-expanded.png'});
await browser.close();
process.exit(ok ? 0 : 1);
