// Drives web/dist/bench.html in Playwright's pinned Chromium and counts the
// requests and bytes that reach the snapshot URL between marks, twice over:
// from the browser's network events (Content-Length of each response, since a
// synchronous XHR's body is not always readable from the driver) and from the
// counting server's own /__stats. Prints one JSON object.
//   node gate/browser/bench.mjs <bench page url> <snapshot url> <params.json> <params2.json>
import { chromium } from 'playwright'
import { readFileSync } from 'node:fs'

const [pageUrl, snapshotUrl, paramsFile, params2File] = process.argv.slice(2)
const statsUrl = new URL('/__stats', pageUrl).href
const browser = await chromium.launch()
const context = await browser.newContext()
const seen = { requests: 0, bytes: 0 }
context.on('requestfinished', async (request) => {
  if (!request.url().startsWith(snapshotUrl)) return
  const response = await request.response()
  const headers = await response.allHeaders()
  seen.requests++
  seen.bytes += Number(headers['content-length'] || 0)
})
const page = await context.newPage()
const steps = []
await page.exposeFunction('__mark', async (step, extra) => {
  await new Promise((r) => setTimeout(r, 50))       // let requestfinished land
  const server = await (await fetch(statsUrl)).json()
  steps.push({ step, requests: seen.requests, bytes: seen.bytes,
               server: { requests: server.requests, bytes: server.bytes }, ...(extra ?? {}) })
  seen.requests = 0
  seen.bytes = 0
})
page.on('pageerror', (e) => console.error('[pageerror]', String(e)))
await fetch(statsUrl)                                   // reset the server's counters
await page.goto(pageUrl)
await page.waitForFunction(() => window.bench)
const result = await page.evaluate((opts) => window.bench(opts), {
  url: snapshotUrl,
  params: JSON.parse(readFileSync(paramsFile, 'utf8')),
  params2: JSON.parse(readFileSync(params2File, 'utf8')),
})
console.log(JSON.stringify({ mode: result.mode, sqlite: result.sqlite, steps }, null, 2))
await browser.close()
