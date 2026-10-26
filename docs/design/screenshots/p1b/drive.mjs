// node drive.mjs <url-with-token> <outdir> <mode>
// mode "named": rename wigshop (a real signed claim), then screenshot the P1b pages.
// mode "anon": screenshot the top bar and the Identity page only.
import { pathToFileURL } from 'node:url'
const { chromium } = await import(pathToFileURL(process.env.NF_BLOCKS_PLAYWRIGHT).href)
const [launch, out, mode] = process.argv.slice(2)
const WIGSHOP = 'bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce'
const base = launch.split('#')[0]
const browser = await chromium.launch()
const errors = []
const ready = async (page) => {
  await page.waitForFunction(() => document.body.dataset.state && document.body.dataset.state !== 'loading', null, { timeout: 90000 })
  await page.waitForTimeout(2500)
}
const go = async (page, hash) => { await page.goto(`${base}${hash}`); await ready(page) }
const facts = {}

if (mode === 'named') {
  const page = await (await browser.newContext({ viewport: { width: 1440, height: 900 } })).newPage()
  page.on('pageerror', e => errors.push(String(e)))
  // Two renames, so the rail has a superseded claim to show beside the current one.
  for (const name of ['wigshop-draft', 'wigshop']) {
    await go(page, `#/selection/${WIGSHOP}`)
    await page.fill('#rename-name', name)
    await page.click('#rename-save')
    await page.waitForTimeout(4000)
  }
  await go(page, `#/selection/${WIGSHOP}`)
  facts.headerTag = await page.evaluate(() => {
    const tag = document.querySelector('.page-head .claim, [data-name] .claim, .claim')
    const av = tag?.querySelector('.av')
    return { text: tag?.innerText, avatarTitle: av?.getAttribute('title'), avatarText: av?.textContent }
  })
  facts.chip = await page.evaluate(() => document.querySelector('[data-identity]')?.innerText ?? null)
  await go(page, '#/pipeline/nf-core%2Frnaseq?mine=1')
  facts.pipelineMine = await page.evaluate(() => ({
    rows: document.querySelectorAll('#main tbody tr').length,
    empty: document.querySelector('#main .empty')?.innerText ?? null,
    toggle: document.querySelector('[data-mine-toggle]')?.innerText ?? null }))
  await go(page, '#/selections?mine=1')
  facts.selectionsMine = await page.evaluate(() => ({ rows: [...document.querySelectorAll('#main tbody tr')].map(r => r.innerText.replace(/\s+/g, ' ').slice(0, 160)) }))
}

const shots = mode === 'named'
  ? { 'build-08-selection': `#/selection/${WIGSHOP}`, 'build-01-kit': '#/kit', 'build-identity': '#/identity',
      'build-pipeline-mine': '#/pipeline/nf-core%2Frnaseq?mine=1', 'build-selections-mine': '#/selections?mine=1', 'build-pipeline-all': '#/pipeline/nf-core%2Frnaseq' }
  : { 'build-identity-anonymous': '#/identity', 'build-pipeline-anonymous': '#/pipeline/nf-core%2Frnaseq' }
for (const scheme of ['light', 'dark']) {
  const page = await (await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: scheme })).newPage()
  page.on('pageerror', e => errors.push(String(e)))
  for (const [name, hash] of Object.entries(shots)) {
    await go(page, hash)
    await page.screenshot({ path: `${out}/${name}-${scheme}.png` })
  }
  if (mode === 'anon') facts.anonChip = await page.evaluate(() => document.querySelector('[data-identity]')?.outerHTML ?? null)
}
facts.errors = errors
console.log('FACTS ' + JSON.stringify(facts))
await browser.close()
