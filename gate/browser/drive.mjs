// gate/browser/drive.mjs
// Plays scenario.json in Playwright's pinned Chromium: one cold context per
// step, waiting on the page's body[data-render] counter, recording every
// request from the browser's own network events and reading back only the
// DOM contract of DESIGN.md §15. It judges nothing; browser_assert.py does.
//   node gate/browser/drive.mjs <scenario.json> <observed.json> <name>=<base url> ...
import { chromium } from 'playwright'
import { readFileSync, writeFileSync } from 'node:fs'

const [scenarioFile, observedFile, ...named] = process.argv.slice(2)
const servers = Object.fromEntries(named.map((arg) => { const i = arg.indexOf('='); return [arg.slice(0, i), arg.slice(i + 1).replace(/\/$/, '')] }))
const fill = (text) => (text ?? '').replace(/\{(\w+)\}/g, (_, name) => servers[name] ?? `{${name}}`)
const { steps } = JSON.parse(readFileSync(scenarioFile, 'utf8'))

async function waitRender(page, after) {
  await page.waitForFunction((n) => Number(document.body?.dataset.render ?? 0) > n && document.body.dataset.state !== 'loading',
    after, { timeout: 180_000 })
}

const settle = () => new Promise((r) => setTimeout(r, 250))

const EXTRACT = () => {
  const data = (selector, names) => [...document.querySelectorAll(selector)]
    .map((e) => Object.fromEntries(names.map((n) => [n, e.dataset[n] ?? null])))
  return {
    state: document.body.dataset.state,
    route: document.body.dataset.route,
    mode: document.getElementById('snapshot-mode')?.dataset.mode ?? null,
    stale: document.getElementById('stale')?.dataset.staleCount ?? null,
    command: !!document.querySelector('#stale [data-command]'),
    runs: data('[data-run]', ['run', 'pipeline', 'status', 'source']),
    producers: data('[data-producer]', ['content', 'item', 'collection', 'completion', 'filename']),
    latest: document.querySelector('[data-latest]')?.dataset.latest ?? null,
    items: [...document.querySelectorAll('[data-item-result]')].map((e) => e.dataset.itemResult),
    errors: data('[data-error]', ['error', 'cid']),
    verified: [...(window.__nfBlocks?.verified ?? [])],
  }
}

const browser = await chromium.launch()
const observed = []
for (const step of steps) {
  const context = await browser.newContext()
  const requests = []
  const consoleErrors = []
  const pageErrors = []
  const pending = []
  let phase = 'open'
  // Record the request, and its phase, the moment it finishes; the status and
  // byte count arrive later from the response, and every step awaits those
  // lookups (settled()) before its requests are read.
  context.on('requestfinished', (request) => {
    const entry = { url: request.url(), method: request.method(), status: null,
      range: request.headers().range ?? null, bytes: 0, phase }
    requests.push(entry)
    pending.push((async () => {
      const response = await request.response()
      if (!response) return
      entry.status = response.status()
      const headers = await response.allHeaders()
      entry.bytes = Number(headers['content-length'] || 0)
    })().catch((e) => { entry.lookupError = String(e) }))
  })
  const settled = async () => { await settle(); while (pending.length) await Promise.all(pending.splice(0)) }
  context.on('requestfailed', (request) => {
    requests.push({ url: request.url(), method: request.method(), status: null, range: request.headers().range ?? null,
      bytes: 0, phase, failure: request.failure()?.errorText ?? null })
  })
  const page = await context.newPage()
  page.on('console', (m) => { if (m.type() === 'error') consoleErrors.push(m.text()) })
  page.on('pageerror', (e) => pageErrors.push(String(e)))
  const record = { id: step.id }
  try {
    const url = `${servers[step.server]}/${step.path}${fill(step.query)}${step.idle ? '#/idle' : step.hash}`
    await page.goto(url)
    await waitRender(page, 0)
    if (step.idle && (await page.evaluate(() => document.body.dataset.state)) === 'ready') {
      await settled()
      phase = 'query'
      const before = Number(await page.evaluate(() => document.body.dataset.render))
      await page.evaluate((h) => { location.hash = h }, step.hash)
      await waitRender(page, before)
    }
    await settled()
    Object.assign(record, await page.evaluate(EXTRACT))
    if (step.head) {
      record.head = await page.evaluate((u) => fetch(u, { method: 'HEAD', cache: 'no-store' })
        .then((r) => ({ status: r.status, length: r.headers.get('content-length') }))
        .catch((e) => ({ error: String(e) })), fill(step.head))
    }
    if (step.members) {
      // browser_assert.py's cloud_check reads this to confirm a member
      // (e.g. "priv") is actually registered, apart from store.js's own
      // routing, which falls back to members[0] when the requested one is
      // not found.
      record.members = await page.evaluate((u) => fetch(u, { cache: 'no-store' })
        .then((r) => r.json().then((body) => ({ status: r.status, body })))
        .catch((e) => ({ error: String(e) })), fill(step.members))
    }
  } catch (e) {
    record.state = 'driver_error'
    record.driverError = String(e)
  }
  await Promise.all(pending.splice(0))
  Object.assign(record, { requests, consoleErrors, pageErrors })
  observed.push(record)
  console.log(`${step.id}: ${record.state}`)
  await context.close()
}
await browser.close()
writeFileSync(observedFile, JSON.stringify({ steps: observed }, null, 1))
