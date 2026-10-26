// P1a acceptance walk on roblab: run page opens on Samples -> filter Reads (trimmed) >= 50,000
// -> rail: quant_salmon only -> Pick -> #/compose; then quant_salmon + quant_merged total; then search.
// node walk.mjs http://localhost:<port>/index.html <outdir>   (serve on localhost, not an IP: see the float check)
import { chromium } from '../../../../gate/browser/node_modules/playwright/index.mjs'
const [base, out] = process.argv.slice(2)
const RUN = 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq'
const browser = await chromium.launch()
const page = await (await browser.newContext({ viewport: { width: 1440, height: 900 } })).newPage()
const errors = []; page.on('pageerror', e => errors.push(String(e)))
const ready = async () => { await page.waitForFunction(() => document.body.dataset.state !== 'loading', null, { timeout: 60000 }); await page.waitForTimeout(2500) }
const audit = async (step) => {
  const r = await page.evaluate(() => {
    const main = document.querySelector('#main'), rail = document.querySelector('#panel')
    // Text a reader sees, without code blocks (snippets name CIDs on purpose).
    // The live text keeps cell boundaries; a detached clone would run "49,969" and "96.0%" together.
    const visible = (el) => { let t = el.innerText; for (const n of el.querySelectorAll('pre, code')) t = t.replace(n.innerText, ''); return t }
    const text = visible(main) + '\n' + (rail ? visible(rail) : '')
    const cidOnly = [...main.querySelectorAll('a, h1, h2, td, th, strong, b')].filter(e => !e.closest('.addr, .address, .mono, code, .cid, .sub, small, pre') && /^\s*baf[a-z0-9.…]{6,}\s*$/.test(e.textContent)).map(e => e.outerHTML.slice(0, 120))
    // Raw floats: four or more decimals, or a large integer printed with ".0" (99566.0). "89.0%" is three figures, as the format rule asks.
    const floats = [...text.matchAll(/\b\d+\.\d{4,}\b|\b\d{4,}\.0\b/g)].map(m => m[0])
    const blank = [...main.querySelectorAll('tr')].filter(tr => tr.offsetHeight > 0 && !tr.innerText.trim() && !tr.querySelector('input,button,svg,.seal')).length
    return { route: document.body.dataset.route, cidOnly, floats, blank }
  })
  console.log(step, JSON.stringify(r))
  await page.screenshot({ path: `${out}/walk-${step}.png` })
  return r
}
const rows = () => page.$$eval('#main tr[data-sample]:not(.dim)', els => els.map(e => e.dataset.sample))
const total = async () => { await page.waitForFunction(() => !/…/.test(document.querySelector('[data-total]')?.textContent ?? '…'), null, { timeout: 60000 }); return page.textContent('[data-total]') }

await page.goto(`${base}#/run/${RUN}`); await ready()
await page.waitForSelector('[data-run-tab-body=samples] tr[data-sample]', { timeout: 60000 }); await page.waitForTimeout(1500)
console.log('default tab', await page.getAttribute('[data-run-tab-body]', 'data-run-tab-body'), 'rows', JSON.stringify(await rows()))
await audit('1-samples')

await page.click('[data-add-filter]'); await page.fill('[data-filter-search]', 'num_trimmed')
await page.click('[data-field="preprocessed:num_trimmed_reads"]')
await page.fill('[data-filter-value]', '50000'); await page.keyboard.press('Enter'); await page.waitForTimeout(1500)
console.log('filtered rows', JSON.stringify(await rows()), 'match', await page.textContent('[data-match]'), 'hash', await page.evaluate(() => decodeURIComponent(location.hash)))
await audit('2-filtered')

for (const o of await page.$$eval('#panel input[data-out]:checked', els => els.map(e => e.dataset.out))) if (o !== 'quant_salmon') await page.click(`#panel input[data-out="${o}"]`)
if (!await page.isChecked('#panel input[data-out="quant_salmon"]')) await page.click('#panel input[data-out="quant_salmon"]')
console.log('quant_salmon only', await total())
await page.click('#panel input[data-out="quant_merged"]')
console.log('quant_salmon + quant_merged', await total(), '| note:', (await page.textContent('[data-merged-note]').catch(() => '')).trim())
await audit('3-handoff')
await page.click('#panel input[data-out="quant_merged"]'); await total()
await page.click('[data-handoff-pick]'); await page.waitForTimeout(800)

await page.goto(`${base}#/compose`); await ready()
console.log('compose', JSON.stringify((await page.textContent('#main')).replace(/\s+/g, ' ').slice(0, 400)))
await audit('4-compose')

await page.goto(`${base}#/search?q=WT_REP1`); await ready()
await audit('5-search')
await page.click('#main a[href^="#/run/"][href*="tab=samples"]'); await page.waitForFunction(() => document.body.dataset.route === 'run', null, { timeout: 60000 }); await ready()
await page.waitForSelector('tr[data-sample]', { timeout: 60000 }); await page.waitForTimeout(1500)
console.log('search -> samples rows', JSON.stringify(await rows()))
await audit('6-search-to-samples')
console.log('page errors', JSON.stringify(errors))
await browser.close()
