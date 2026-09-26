// gate/browser/drive.mjs
// Plays scenario.json in Playwright's pinned Chromium: one cold context per
// step, waiting on the page's body[data-render] counter, recording every
// request from the browser's own network events and reading back only the
// DOM contract of DESIGN.md §15. It judges nothing; browser_assert.py and
// browser_b_assert.py do.
//   node gate/browser/drive.mjs <scenario.json> <observed.json> <name>=<base url> ...
//
// Tier B's steps add `actions` (hash, click, fill, waitWrite, extract), may
// open `pages` > 1 in one context (two sessions), and `save` attributes of
// page 0 as variables that later steps' hashes, queries and selectors name as
// {NAME}. A `name=value` argument that is not a URL (the launch token) is a
// variable like any other.
import { chromium } from 'playwright'
import { readFileSync, writeFileSync } from 'node:fs'

const [scenarioFile, observedFile, ...named] = process.argv.slice(2)
const servers = Object.fromEntries(named.map((arg) => { const i = arg.indexOf('='); return [arg.slice(0, i), arg.slice(i + 1).replace(/\/$/, '')] }))
const vars = {}
const fill = (text) => (text ?? '').replace(/\{(\w+)\}/g, (_, name) => servers[name] ?? vars[name] ?? `{${name}}`)
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

// Tier B's reading of the DOM contract (Task 14's final form, DESIGN.md §15).
const EXTRACT_B = () => ({
  write: document.body.dataset.write ?? null,
  writeOutcome: document.body.dataset.writeOutcome ?? null,
  writeSeq: document.body.dataset.writeSeq ?? null,
  written: document.body.dataset.written ?? null,
  tray: document.getElementById('tray')?.dataset.count ?? null,
  selections: [...document.querySelectorAll('[data-selection]')].map((e) => ({ cid: e.dataset.selection, deletion: e.dataset.deletion,
    source: e.dataset.source, names: JSON.parse(e.dataset.names ?? '[]') })),
  view: document.querySelector('[data-selection-view]')?.dataset.selectionView ?? null,
  deletion: document.querySelector('[data-selection-view]')?.dataset.deletion ?? null,
  names: [...document.querySelectorAll('[data-name]')].map((e) => ({ name: e.dataset.name, claim: e.dataset.claim, conflicted: 'conflicted' in e.dataset })),
  members: [...document.querySelectorAll('[data-member]')].map((e) => ({ address: e.dataset.member, kind: e.dataset.kind, held: e.dataset.held })),
  errors: [...document.querySelectorAll('[data-error]')].map((e) => ({ error: e.dataset.error, cid: e.dataset.cid ?? null })),
  composeName: document.getElementById('compose-name')?.value ?? null,
  heldElsewhere: document.querySelector('[data-held-elsewhere]')?.dataset.heldElsewhere ?? null,
  heldElsewhereNames: JSON.parse(document.querySelector('[data-held-elsewhere]')?.dataset.names ?? 'null'),
})

const writeSeq = (page) => page.evaluate(() => Number(document.body.dataset.writeSeq ?? 0))

// One tier B action on one page. A click remembers the page's write counter
// first, so the waitWrite after it sees a write that ended before it ran.
async function act(pages, action, record) {
  const index = action.page ?? 0
  const page = pages[index]
  if (action.hash !== undefined) {
    const before = Number(await page.evaluate(() => document.body.dataset.render))
    await page.evaluate((h) => { location.hash = h }, fill(action.hash))
    await waitRender(page, before)
  } else if (action.click) {
    record.seq[index] = await writeSeq(page)
    await page.click(fill(action.click))
  } else if (action.fill) {
    await page.fill(fill(action.fill[0]), fill(action.fill[1]))
  } else if (action.waitWrite) {
    const after = action.after ?? record.seq[index] ?? 0
    await page.waitForFunction((n) => Number(document.body.dataset.writeSeq ?? 0) > n, after, { timeout: 60_000 })
    // The write's own re-render (history.pushState, then render) may still be running.
    await page.waitForFunction(() => document.body.dataset.state !== 'loading', null, { timeout: 60_000 })
  } else if (action.extract) {
    record.extracts[action.extract] = await page.evaluate(EXTRACT_B)
  } else {
    throw new Error(`unknown action ${JSON.stringify(action)}`)
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
  const pages = []
  const pageOf = (request) => { try { return pages.indexOf(request.frame().page()) } catch { return -1 } }
  // Record the request, and its phase, the moment it finishes; the status and
  // byte count arrive later from the response, and every step awaits those
  // lookups (settled()) before its requests are read.
  context.on('requestfinished', (request) => {
    const entry = { url: request.url(), method: request.method(), status: null,
      range: request.headers().range ?? null, bytes: 0, phase }
    if (request.method() === 'POST') Object.assign(entry, { page: pageOf(request), body: request.postData() })
    requests.push(entry)
    pending.push((async () => {
      const response = await request.response()
      if (!response) return
      entry.status = response.status()
      const headers = await response.allHeaders()
      entry.bytes = Number(headers['content-length'] || 0)
      if (request.method() === 'POST' && new URL(request.url()).pathname === '/api/put') entry.responseBody = await response.text()
    })().catch((e) => { entry.lookupError = String(e) }))
  })
  const settled = async () => { await settle(); while (pending.length) await Promise.all(pending.splice(0)) }
  context.on('requestfailed', (request) => {
    requests.push({ url: request.url(), method: request.method(), status: null, range: request.headers().range ?? null,
      bytes: 0, phase, failure: request.failure()?.errorText ?? null,
      ...(request.method() === 'POST' ? { page: pageOf(request), body: request.postData() } : {}) })
  })
  for (let i = 0; i < (step.pages ?? 1); i++) {
    const p = await context.newPage()
    p.on('console', (m) => { if (m.type() === 'error') consoleErrors.push(m.text()) })
    p.on('pageerror', (e) => pageErrors.push(String(e)))
    pages.push(p)
  }
  const page = pages[0]
  const record = { id: step.id }
  try {
    const url = `${servers[step.server]}/${step.path}${fill(step.query)}${step.idle ? '#/idle' : fill(step.hash)}`
    for (const p of pages) {
      await p.goto(url)
      await waitRender(p, 0)
    }
    if (step.actions) {
      Object.assign(record, { extracts: {}, seq: {} })
      for (const action of step.actions) await act(pages, action, record)
      for (const [name, selector, attribute] of step.save ?? []) {
        vars[name] = await page.evaluate(([s, a]) => document.querySelector(s)?.getAttribute(a) ?? null, [selector, attribute])
      }
      record.saved = Object.fromEntries((step.save ?? []).map(([name]) => [name, vars[name] ?? null]))
    }
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
