import puppeteer from 'puppeteer-core';
const EXE = process.env.CHROME || '/usr/bin/google-chrome';
const browser = await puppeteer.launch({executablePath: EXE, headless: 'new', args: ['--no-sandbox', '--disable-gpu'], defaultViewport: {width: 1440, height: 900}});
const page = await browser.newPage();
const errors = [];
const notFound = [];
page.on('response', (r) => { if (r.status() === 404) notFound.push(r.url()); });
page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
page.on('console', (m) => { if (m.type() === 'error') errors.push('console: ' + m.text()); });

let ok = true;

await page.goto('http://127.0.0.1:8080/#/cluster', {waitUntil: 'networkidle0', timeout: 30000});
await page.waitForSelector('button', {timeout: 30000});
const tblBtn = await page.evaluateHandle(() => Array.from(document.querySelectorAll('button')).find((b) => b.innerText.trim() === 'logs'));
if (tblBtn) await tblBtn.click();
await page.waitForFunction(() => document.body.innerText.includes('Shards of'), {timeout: 30000});
await new Promise((r) => setTimeout(r, 800));

const btns = await page.evaluate(() => Array.from(document.querySelectorAll('button')).map((b) => b.innerText.trim()).filter(Boolean));
console.log('buttons:', JSON.stringify(btns));

// Peers: result dialog must show the node id/addr columns
const peers = await page.evaluateHandle(() => Array.from(document.querySelectorAll('button')).find((b) => b.innerText.trim() === 'Peers'));
if (peers) await peers.click();
await new Promise((r) => setTimeout(r, 1500));
let dialogs = await page.evaluate(() => Array.from(document.querySelectorAll('.g-dialog')).map((d) => d.innerText.slice(0, 400)));
console.log('dialogs after Peers:', JSON.stringify(dialogs));
const peersOk = dialogs.some((d) => d.includes('SHARD-PEERS') && d.includes('ID') && d.includes('Addr'));
ok = ok && peersOk;
// close all open dialogs
await page.evaluate(() => {
  Array.from(document.querySelectorAll('.g-dialog button')).forEach((b) => b.click());
});
await new Promise((r) => setTimeout(r, 800));

// Split flow: split logs at a PK value -> a new shard logs-0-1 appears
const splits = await page.evaluateHandle(() => Array.from(document.querySelectorAll('button')).filter((b) => b.innerText.trim() === 'Split'));
console.log('Split buttons:', await splits.evaluate((el) => el.length));
if (await splits.evaluate((el) => el.length >= 1)) {
  const clicked = await page.evaluate(() => {
    const trs = Array.from(document.querySelectorAll('tbody tr'));
    const row = trs.find((tr) => {
      const b = Array.from(tr.querySelectorAll('button')).find((x) => x.innerText.trim() === 'logs');
      return !!b;
    });
    if (!row) return 'no logs row';
    const split = Array.from(row.querySelectorAll('button')).find((x) => x.innerText.trim() === 'Split');
    if (!split) return 'no split in logs row';
    split.click();
    return 'clicked logs split';
  });
  console.log('split target:', clicked);
  await new Promise((r) => setTimeout(r, 800));
  dialogs = await page.evaluate(() => Array.from(document.querySelectorAll('.g-dialog')).map((d) => d.innerText.slice(0, 300)));
  console.log('dialog after Split click:', JSON.stringify(dialogs));
  const typed = await page.evaluate(() => {
    const d = Array.from(document.querySelectorAll('.g-dialog')).find((x) => x.innerText.includes('primary-key'));
    const input = d ? d.querySelector('input') : null;
    if (!input) return false;
    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
    setter.call(input, '2026-08-17T15:00:00+03:00');
    input.dispatchEvent(new Event('input', {bubbles: true}));
    return true;
  });
  console.log('typed:', typed);
  await new Promise((r) => setTimeout(r, 400));
  const apply = await page.evaluateHandle(() => {
    const d = Array.from(document.querySelectorAll('.g-dialog')).find((x) => x.innerText.includes('primary-key'));
    return d ? Array.from(d.querySelectorAll('button')).find((b) => b.innerText.trim() === 'Split') : null;
  });
  const applyFound = await apply.evaluate((n) => !!n);
  console.log('apply found:', applyFound);
  ok = ok && applyFound;
  if (applyFound) {
    await apply.click();
    await new Promise((r) => setTimeout(r, 2500));
    dialogs = await page.evaluate(() => Array.from(document.querySelectorAll('.g-dialog')).map((d) => d.innerText.slice(0, 400)));
    console.log('dialogs after Split apply:', JSON.stringify(dialogs));
    await page.evaluate(() => Array.from(document.querySelectorAll('.g-dialog button')).forEach((b) => b.click()));
    await new Promise((r) => setTimeout(r, 1500));
  }
}
const shardRows = await page.evaluate(() => {
  const tbs = Array.from(document.querySelectorAll('table'));
  const sh = tbs.find((tb) => tb.innerText.includes('logs-0')) || tbs[tbs.length - 1];
  return sh ? sh.innerText : 'none';
});
console.log('shards table:', JSON.stringify(shardRows.slice(0, 200)));
const splitOk = shardRows.includes('logs-0') && shardRows.includes('logs-0-1');
ok = ok && splitOk;

// Composite-PK split form: create a 2-column-PK table, the Split dialog must
// render one input per PK column, and applying creates the fresh shard.
await fetch('http://127.0.0.1:8080/api/v1/query', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({sql: 'CREATE TABLE kvp (k string, n int64, v string, PRIMARY KEY (k, n))'}),
});
await page.reload({waitUntil: 'networkidle0'});
await page.waitForFunction(() => document.body.innerText.includes('kvp'), {timeout: 30000});
await new Promise((r) => setTimeout(r, 1200));
const kvpSplit = await page.evaluate(() => {
  const trs = Array.from(document.querySelectorAll('tbody tr'));
  const row = trs.find((tr) => {
    const b = Array.from(tr.querySelectorAll('button')).find((x) => x.innerText.trim() === 'kvp');
    return !!b;
  });
  if (!row) return false;
  const split = Array.from(row.querySelectorAll('button')).find((x) => x.innerText.trim() === 'Split');
  if (!split) return false;
  split.click();
  return true;
});
console.log('kvp split clicked:', kvpSplit);
await new Promise((r) => setTimeout(r, 800));
const kvpInputs = await page.evaluate(() => {
  const d = Array.from(document.querySelectorAll('.g-dialog')).find((x) => x.innerText.includes('primary-key'));
  if (!d) return null;
  return {
    labels: Array.from(d.querySelectorAll('input')).map((i) => i.getAttribute('placeholder') || i.name || '?'),
    count: d.querySelectorAll('input').length,
    hasK: !!d.querySelector('[data-qa="split-input-k"]'),
    hasN: !!d.querySelector('[data-qa="split-input-n"]'),
  };
});
console.log('composite dialog inputs:', JSON.stringify(kvpInputs));
ok = ok && kvpInputs && kvpInputs.count === 2 && kvpInputs.hasK && kvpInputs.hasN;
if (kvpInputs) {
  const typed2 = await page.evaluate(() => {
    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
    const k = document.querySelector('[data-qa="split-input-k"] input') || document.querySelector('[data-qa="split-input-k"]');
    const n = document.querySelector('[data-qa="split-input-n"] input') || document.querySelector('[data-qa="split-input-n"]');
    if (!k || !n) return false;
    setter.call(k, 'alpha');
    k.dispatchEvent(new Event('input', {bubbles: true}));
    setter.call(n, '42');
    n.dispatchEvent(new Event('input', {bubbles: true}));
    return true;
  });
  console.log('composite typed:', typed2);
  await new Promise((r) => setTimeout(r, 400));
  const apply2 = await page.evaluateHandle(() => {
    const d = Array.from(document.querySelectorAll('.g-dialog')).find((x) => x.innerText.includes('primary-key'));
    return d ? Array.from(d.querySelectorAll('button')).find((b) => b.innerText.trim() === 'Split') : null;
  });
  const apply2Found = await apply2.evaluate((n) => !!n);
  console.log('composite apply found:', apply2Found);
  ok = ok && apply2Found;
  if (apply2Found) {
    await apply2.click();
    await new Promise((r) => setTimeout(r, 2500));
    await page.evaluate(() => Array.from(document.querySelectorAll('.g-dialog button')).forEach((b) => b.click()));
    await new Promise((r) => setTimeout(r, 1500));
  }
}
// select the kvp table so the "Shards of kvp" section with shard IDs shows up
const kvpSelected = await page.evaluate(() => {
  const trs = Array.from(document.querySelectorAll('tbody tr'));
  const row = trs.find((tr) => {
    const b = Array.from(tr.querySelectorAll('button')).find((x) => x.innerText.trim() === 'kvp');
    return !!b;
  });
  if (!row) return false;
  const b = Array.from(row.querySelectorAll('button')).find((x) => x.innerText.trim() === 'kvp');
  b.click();
  return true;
});
console.log('kvp selected:', kvpSelected);
await page.waitForFunction(() => document.body.innerText.includes('Shards of kvp'), {timeout: 30000}).catch(() => {});
await new Promise((r) => setTimeout(r, 1200));
const kvpShards = await page.evaluate(() => {
  const tbs = Array.from(document.querySelectorAll('table'));
  const sh = tbs.find((tb) => tb.innerText.includes('kvp-0')) || tbs[tbs.length - 1];
  return sh ? sh.innerText : 'none';
});
console.log('kvp shards table:', JSON.stringify(kvpShards.slice(0, 200)));
const kvpSplitOk = kvpShards.includes('kvp-0') && kvpShards.includes('kvp-0-1');
ok = ok && kvpSplitOk;
// cleanup: drop the composite table so the demo stays tidy
await fetch('http://127.0.0.1:8080/api/v1/query', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({sql: 'DROP TABLE kvp'}),
});

const realErrors = errors.filter((e) => !e.includes('favicon') && !/Failed to load resource.*status of 404/.test(e));
const nonFav404 = notFound.filter((u) => !u.includes('favicon'));
console.log('js errors:', realErrors.length ? realErrors.join('\n') : 'none');
console.log('404s:', nonFav404.length ? JSON.stringify(nonFav404) : 'none (favicon only)');
console.log(ok && realErrors.length === 0 && nonFav404.length === 0 ? 'RESULT: OK' : 'RESULT: PROBLEMS');
await browser.close();
process.exit(ok && realErrors.length === 0 && nonFav404.length === 0 ? 0 : 1);
