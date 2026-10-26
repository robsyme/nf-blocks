// Acceptance walk: Pipelines -> nf-core/rnaseq -> latest run -> preprocessed -> filter -> pick -> compose
import { pathToFileURL } from 'node:url'
// Playwright from this repo's gate/browser (npm ci there), or NF_BLOCKS_PLAYWRIGHT=<path to playwright/index.mjs>.
const playwright = process.env.NF_BLOCKS_PLAYWRIGHT ? pathToFileURL(process.env.NF_BLOCKS_PLAYWRIGHT).href
  : new URL('../../../../gate/browser/node_modules/playwright/index.mjs', import.meta.url).href
const { chromium } = await import(playwright)
const base = process.argv[2], out = process.argv[3]
const browser = await chromium.launch()
const page = await (await browser.newContext({ viewport: { width: 1440, height: 900 } })).newPage()
const errors = []; page.on('pageerror', e => errors.push(String(e)))
const ready = async () => { await page.waitForFunction(() => document.body.dataset.state !== 'loading', null, { timeout: 60000 }); await page.waitForTimeout(2500) }
const audit = async (step) => {
  const r = await page.evaluate(() => {
    const main = document.querySelector('#main'), rail = document.querySelector('#panel')
    const text = (main.innerText + '\n' + rail.innerText)
    // CID-only labels: a link or heading whose whole text is a CID or short CID
    const cidOnly = [...main.querySelectorAll('a, h1, h2, td, th, strong')].filter(e => !e.closest('.addr, .address, .mono, code, .cid, .sub, small, pre') && /^\s*baf[a-z0-9.…]{6,}\s*$/.test(e.textContent)).map(e => e.outerHTML.slice(0, 120))
    const floats = [...text.matchAll(/\b\d+\.\d{4,}\b|\b\d{2,}\.0\b(?!\s*(KB|MB|GB|B|TB))/g)].map(m => m[0])
    const blank = [...main.querySelectorAll('tr')].filter(tr => tr.offsetHeight > 0 && !tr.innerText.trim() && !tr.querySelector('input,button,svg,.seal')).length
    return { route: document.body.dataset.route, cidOnly, floats, blank }
  })
  console.log(step, JSON.stringify(r))
  await page.screenshot({ path: `${out}/walk-${step}.png` })
}
await page.goto(`${base}/index.html#/`); await ready(); await audit('1-pipelines')
await page.click('#main a:text-is("nf-core/rnaseq")'); await ready(); await audit('2-pipeline')
await page.click('#main [data-run] a:text-is("distraught_murdock")'); await ready(); await audit('3-run')
await page.click('#main a:text-is("preprocessed")'); await ready(); await audit('4-preprocessed')
// filter through the builder
await page.click('[data-add-filter]'); await page.fill('[data-filter-search]', 'num_trimmed'); await page.keyboard.press('Enter')
await page.click('.fb-op[data-op=">="]'); await page.fill('[data-filter-value]', '50000'); await page.keyboard.press('Enter'); await ready()
const rows = await page.$$eval('[data-item-result]', els => els.map(e => e.innerText.split('\n')[0].trim()))
console.log('filtered rows', JSON.stringify(rows), 'hash', await page.evaluate(() => decodeURIComponent(location.hash)))
await audit('5-filtered')
await page.click('[data-pick-all]'); await page.waitForTimeout(500)
console.log('tray', await page.$eval('#tray', e => e.dataset.count))
await page.click('#tray'); await ready(); await audit('6-compose')
console.log('compose text', JSON.stringify((await page.$eval('#main', e => e.innerText)).slice(0, 400)))
console.log('page errors', JSON.stringify(errors))
await browser.close()
