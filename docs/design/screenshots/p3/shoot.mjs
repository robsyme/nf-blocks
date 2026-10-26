// node shoot.mjs <outdir> <explore url> <phase>   phase: before | after | dest | notkept | mock
// Screenshots the P3 pages at 1440x900 in light and dark. With `mock`, the url
// is the mockups folder (file://.../docs/design/mockups/) and only 11-copy is shot.
const PW = process.env.PLAYWRIGHT ?? new URL('../../../../gate/browser/node_modules/playwright/index.mjs', import.meta.url).pathname
const { chromium } = await import(PW)
const [out, base, phase] = process.argv.slice(2)
const SEL = process.env.SELECTION ?? 'bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce'
const DEST = process.env.DEST ?? 'org-local'

const at = (member, hash) => {
  const u = new URL(base)
  if (member) u.searchParams.set('member', member)
  u.hash = hash
  return u.href
}

const SHOTS = {
  mock: [['mock-11-copy', new URL('11-copy.html', base).href]],
  before: [
    ['build-11-copy', at('roblab', `#/selection/${SEL}/copy?to=${DEST}`)],
    ['build-selection-presence-before', at('roblab', `#/selection/${SEL}`)],
    ['build-switcher', at('roblab', '#/'), async (page) => { await page.click('.store-switch'); await page.waitForTimeout(1200) }],
    ['build-all-stores-home', at('*', '#/')],
    ['build-all-stores-storage', at('*', '#/storage')],
  ],
  after: [
    ['build-selection-presence-after', at('roblab', `#/selection/${SEL}`)],
    ['build-11-copy-after', at('roblab', `#/selection/${SEL}/copy?to=${DEST}`)],
    ['build-all-stores-storage-after', at('*', '#/storage')],
  ],
  // The Selection's copy page after the run was released in the destination: the not-kept note.
  notkept: [
    ['build-11-copy-notkept', at('roblab', `#/selection/${SEL}/copy?to=${DEST}`)],
  ],
  dest: [
    ['build-dest-selection', at(null, `#/selection/${SEL}`)],
    ['build-dest-run-provenance', at(null, `#/selection/${SEL}`), async (page) => {
      const href = await page.getAttribute('a[href^="#/run/"]', 'href')
      await page.goto(at(null, `${href.split('?')[0]}?tab=provenance`))
    }],
    ['build-dest-item-lineage', at(null, `#/selection/${SEL}`), async (page) => {
      const href = await page.getAttribute('a[href^="#/item/"]', 'href')
      await page.goto(at(null, href))
    }],
  ],
}

const ready = async (page) => {
  if (await page.evaluate(() => !!document.body.dataset.render))
    await page.waitForFunction(() => document.body.dataset.state !== 'loading', null, { timeout: 60000 }).catch(() => {})
  await page.waitForTimeout(2500)
}

const browser = await chromium.launch()
for (const scheme of ['light', 'dark']) {
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: scheme })
  const page = await ctx.newPage()
  for (const [name, url, act] of SHOTS[phase]) {
    await page.goto(url, { waitUntil: 'load' }).catch(() => {})
    await page.evaluate(() => { document.querySelector('.spec')?.remove(); document.querySelector('.theme-fab')?.remove() })
    await ready(page)
    if (act) { await act(page); await ready(page) }
    await page.screenshot({ path: `${out}/${name}-${scheme}.png` })
    console.log(`${out}/${name}-${scheme}.png`)
  }
  await ctx.close()
}
await browser.close()
