// gate/browser/page-smoke.mjs
// The page over the Task 10 fixture, in Playwright's pinned Chromium, served
// with Range and without. Exits non-zero on the first wrong answer.
//   node gate/browser/page-smoke.mjs
import { chromium } from 'playwright'
import { execFileSync, spawn } from 'node:child_process'
import { copyFileSync, mkdtempSync, renameSync, writeFileSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import assert from 'node:assert/strict'

const repo = new URL('../../', import.meta.url).pathname
const dir = mkdtempSync(join(tmpdir(), 'nfb-page-'))
const expected = JSON.parse(execFileSync('node', [join(repo, 'web/test/write-fixture.mjs'), dir]).toString())
const { runs, item, content } = expected

function serve(port, extra = []) {
  const child = spawn('python3', [join(repo, 'gate/browser/serve.py'), dir, String(port), ...extra], { stdio: 'inherit' })
  return child
}

async function waitFor(url) {
  for (let i = 0; i < 100; i++) {
    try { if ((await fetch(url)).ok) return } catch {}
    await new Promise(r => setTimeout(r, 100))
  }
  throw new Error(`no server at ${url}`)
}

/** Navigates to a route and waits for its render to finish. */
async function open(page, url) {
  const before = Number(await page.evaluate(() => document.body?.dataset.render ?? 0).catch(() => 0))
  await page.goto(url)
  await page.waitForFunction((n) => Number(document.body.dataset.render) > n && document.body.dataset.state !== 'loading', before)
}

async function go(page, hash) {
  const before = Number(await page.evaluate(() => document.body.dataset.render))
  await page.evaluate((h) => { location.hash = h }, hash)
  await page.waitForFunction((n) => Number(document.body.dataset.render) > n && document.body.dataset.state !== 'loading', before)
}

const all = (page, selector, attrs) => page.$$eval(selector, (els, names) => els.map(e => Object.fromEntries(names.map(n => [n, e.dataset[n] ?? null]))), attrs)

const servers = [serve(8841), serve(8842, ['--no-range'])]
const browser = await chromium.launch()
try {
  await waitFor('http://127.0.0.1:8841/index.html')
  await waitFor('http://127.0.0.1:8842/index.html')
  const context = await browser.newContext()
  const page = await context.newPage()
  const errors = []
  page.on('pageerror', e => errors.push(String(e)))
  const fetched = new Set()
  context.on('requestfinished', r => { const m = /\/blocks\/..\/(b[a-z2-7]{58})$/.exec(r.url()); if (m) fetched.add(m[1]) })

  await open(page, 'http://127.0.0.1:8841/index.html#/')
  assert.equal(await page.evaluate(() => document.body.dataset.state), 'ready')
  assert.equal(await page.$eval('#snapshot-mode', e => e.dataset.mode), 'range')
  assert.equal(await page.$eval('#stale', e => e.dataset.staleCount), '2')

  await go(page, '#/pipeline/demo')
  assert.deepEqual((await all(page, '[data-run]', ['run', 'source', 'status'])).map(r => [r.run, r.source, r.status]), [
    [runs.R3.completion, 'tail', 'failed'], [runs.R2.completion, 'tail', 'succeeded'], [runs.R1.completion, 'snapshot', 'succeeded']])
  assert.deepEqual(await all(page, '[data-page]', ['first', 'last', 'total']), [{ first: '1', last: '3', total: '3' }])
  assert.equal(await page.$('[data-page-next]'), null)

  await go(page, `#/collection/${runs.R1.collection}`)
  assert.deepEqual(await all(page, '[data-page]', ['first', 'last', 'total']), [{ first: '1', last: '2', total: '2' }])
  await go(page, `#/collection/${runs.R1.collection}?offset=1`)
  assert.deepEqual(await all(page, '[data-page]', ['first', 'last', 'total']), [{ first: '2', last: '2', total: '2' }])
  assert.equal((await page.$$('[data-page-prev]')).length, 1)

  await go(page, `#/content/${content.B}`)
  assert.deepEqual((await all(page, '[data-producer]', ['completion', 'item', 'filename'])).map(p => p.completion).sort(),
    [runs.R1.completion, runs.R2.completion].sort())

  await go(page, '#/latest/demo')
  assert.equal(await page.$eval('[data-latest]', e => e.dataset.latest), runs.R2.completion)

  await go(page, `#/items/${runs.R2.completion}/aligned?where=${encodeURIComponent(JSON.stringify([['sample', 'string', 'C']]))}`)
  assert.deepEqual(await page.$$eval('[data-item-result]', els => els.map(e => e.dataset.itemResult)), [item.C])

  await go(page, `#/items/${runs.R1.completion}/aligned?where=${encodeURIComponent(JSON.stringify([['lane', 'int', '1']]))}`)
  assert.deepEqual(await page.$$eval('[data-item-result]', els => els.map(e => e.dataset.itemResult)), [item.A])

  await go(page, `#/run/${runs.R1.completion}`)
  assert.deepEqual(await all(page, '[data-collection]', ['collection', 'output']), [{ collection: runs.R1.collection, output: 'aligned' }])

  await go(page, `#/item/${runs.R1.collection}/${item.A}`)
  assert.equal(await page.evaluate(() => document.body.dataset.state), 'ready')

  await go(page, '#/no/such/view')
  assert.equal(await page.evaluate(() => document.body.dataset.state), 'error')
  assert.equal(await page.$eval('[data-error]', e => e.dataset.error), 'bad_route')

  const verified = new Set(await page.evaluate(() => window.__nfBlocks.verified))
  for (const cid of fetched) assert.ok(verified.has(cid), `fetched ${cid} but never verified it`)
  assert.deepEqual(errors, [])

  const plain = await context.newPage()
  await open(plain, `http://127.0.0.1:8842/index.html#/content/${content.A}`)
  assert.equal(await plain.$eval('#snapshot-mode', e => e.dataset.mode), 'whole')
  assert.equal((await all(plain, '[data-producer]', ['completion'])).length, 1)

  // A snapshot rewritten under an open page (final review finding 1): the
  // next query that reads a page it has not cached fails with snapshot_changed.
  const swapped = await context.newPage()
  await open(swapped, 'http://127.0.0.1:8841/index.html#/idle')
  const snapshot = join(dir, 'index/v2.sqlite')
  copyFileSync(snapshot, snapshot + '.orig')
  writeFileSync(snapshot + '.tmp', Buffer.concat([readFileSync(snapshot), Buffer.alloc(4096)]))
  renameSync(snapshot + '.tmp', snapshot)
  await go(swapped, `#/content/${content.B}`)
  assert.equal(await swapped.evaluate(() => document.body.dataset.state), 'error')
  assert.equal(await swapped.$eval('[data-error]', e => e.dataset.error), 'snapshot_changed')
  renameSync(snapshot + '.orig', snapshot)

  // Opened straight from disk (final review finding 4): a clear refusal.
  const disk = await context.newPage()
  await open(disk, `file://${join(dir, 'index.html')}#/`)
  assert.equal(await disk.evaluate(() => document.body.dataset.state), 'error')
  assert.equal(await disk.$eval('[data-error]', e => e.dataset.error), 'file_protocol')

  console.log('page smoke: ok')
} finally {
  await browser.close()
  for (const s of servers) s.kill()
}
