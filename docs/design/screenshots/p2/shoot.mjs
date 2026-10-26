// node shoot.mjs <outdir> <name>=<url> ...   screenshots each url at 1440x900 in light and dark
const PW = process.env.PLAYWRIGHT ?? new URL('../../../../gate/browser/node_modules/playwright/index.mjs', import.meta.url).pathname
const { chromium } = await import(PW)
const [out, ...pairs] = process.argv.slice(2)
const browser = await chromium.launch()
for (const scheme of ['light', 'dark']) {
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: scheme })
  const page = await ctx.newPage()
  for (const pair of pairs) {
    const i = pair.indexOf('='); const name = pair.slice(0, i); const url = pair.slice(i + 1)
    await page.goto(url, { waitUntil: 'networkidle' }).catch(() => {})
    await page.evaluate(() => { document.querySelector('.spec')?.remove(); document.querySelector('.theme-fab')?.remove() })
    if (await page.evaluate(() => !!document.body.dataset.render)) {
      await page.waitForFunction(() => document.body.dataset.state !== 'loading', null, { timeout: 60000 }).catch(() => {})
    }
    await page.waitForTimeout(2500)
    await page.screenshot({ path: `${out}/${name}-${scheme}.png` })
    console.log(`${out}/${name}-${scheme}.png`)
  }
  await ctx.close()
}
await browser.close()
