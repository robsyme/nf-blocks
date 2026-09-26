// gate/browser/page-smoke.mjs
// The page over the Task 10 fixture, in Playwright's pinned Chromium, served
// with Range and without. Exits non-zero on the first wrong answer.
//   node gate/browser/page-smoke.mjs
import { chromium } from 'playwright'
import { execFileSync, spawn } from 'node:child_process'
import { copyFileSync, mkdirSync, mkdtempSync, renameSync, writeFileSync, readFileSync } from 'node:fs'
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
  assert.equal(await page.$eval('#stale', e => e.dataset.log), 'read')
  assert.equal(await page.evaluate(() => document.body.dataset.write), 'unavailable')

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
  await go(page, `#/collection/${runs.R1.collection}?offset=5`)
  assert.deepEqual(await all(page, '[data-page]', ['first', 'total']), [{ first: '0', total: '2' }])
  assert.match(await page.$eval('[data-page]', e => e.textContent), /^no items on this page; there are 2/)

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
  const snapshot = join(dir, 'index/v3.sqlite')
  copyFileSync(snapshot, snapshot + '.orig')
  writeFileSync(snapshot + '.tmp', Buffer.concat([readFileSync(snapshot), Buffer.alloc(4096)]))
  renameSync(snapshot + '.tmp', snapshot)
  await go(swapped, `#/content/${content.B}`)
  assert.equal(await swapped.evaluate(() => document.body.dataset.state), 'error')
  assert.equal(await swapped.$eval('[data-error]', e => e.dataset.error), 'snapshot_changed')
  renameSync(snapshot + '.orig', snapshot)

  // A member whose Store Log no listing answers for (final review finding 6).
  mkdirSync(join(dir, 'nolog/index'), { recursive: true })
  copyFileSync(join(dir, 'index/v3.sqlite'), join(dir, 'nolog/index/v3.sqlite'))
  const nolog = await context.newPage()
  await open(nolog, 'http://127.0.0.1:8841/index.html?store=nolog/#/idle')
  assert.equal(await nolog.$eval('#stale', e => e.dataset.log), 'unreadable')
  assert.equal(await nolog.$eval('#stale', e => e.dataset.staleCount), '0')
  assert.match(await nolog.$eval('#stale', e => e.textContent), /Store Log not readable/)

  // Opened straight from disk (final review finding 4): a clear refusal.
  const disk = await context.newPage()
  await open(disk, `file://${join(dir, 'index.html')}#/`)
  assert.equal(await disk.evaluate(() => document.body.dataset.state), 'error')
  assert.equal(await disk.$eval('[data-error]', e => e.dataset.error), 'file_protocol')

  // Composing against a stub POST /api/put (Task 14): members.json says the
  // page may write, and each POST is recorded and answered with a canned body.
  const S1 = 'bafyreieqfispnqxoy5soafwru7dgsenxgmkdhzhiy6fc7nfjzdi6r3ujmy' // any valid CID; no such block
  const writeContext = await browser.newContext()
  const posts = []
  // Two aliases for the one fixture member: `main` writable, `other` not.
  await writeContext.route(/\/members\.json$/, r => r.fulfill({ contentType: 'application/json',
    body: JSON.stringify({ members: [{ alias: 'main', writable: true, base: './' }, { alias: 'other', writable: false, base: './' }], write: true }) }))
  // While brokenLog is set, the Store Log listing is malformed, so the tail refresh after a write throws.
  let brokenLog = false
  await writeContext.route(/\/log\/$/, r => (brokenLog
    ? r.fulfill({ contentType: 'application/json', body: '{"entries": 5}' }) : r.continue()))
  await writeContext.route(/\/api\/put(\?.*)?$/, async (r) => {
    const req = r.request()
    posts.push({ url: req.url(), token: req.headers()['x-nf-blocks-token'], body: JSON.parse(req.postData()) })
    const dry = req.url().endsWith('?dry_run=true')
    // DAG-JSON with its keys sorted, as the server writes it.
    const body = dry ? { address: { '/': S1 }, exists: false, names: [] } : { address: { '/': S1 }, block: {}, entry: 'e', written: true }
    await r.fulfill({ status: 200, contentType: 'application/vnd.ipld.dag-json', body: JSON.stringify(body) })
  })
  const composer = await writeContext.newPage()
  const composeErrors = []
  composer.on('pageerror', e => composeErrors.push(String(e)))
  await open(composer, `http://127.0.0.1:8841/?token=t#/item/${runs.R1.collection}/${item.A}`)
  assert.equal(await composer.evaluate(() => document.body.dataset.write), 'available')
  assert.equal(await composer.$eval('#tray', e => e.dataset.count), '0')
  await composer.click(`[data-pick="${item.A}"]`)
  assert.equal(await composer.$eval('#tray', e => e.dataset.count), '1')
  await go(composer, '#/compose')
  assert.deepEqual(await all(composer, '[data-tray-entry]', ['trayEntry', 'kind']), [{ trayEntry: item.A, kind: 'item' }])
  await composer.fill('#compose-name', 'smoke, "one"')
  // A double click is one attempt: the busy guard ignores the second click.
  await composer.dblclick('#compose-save')
  await composer.waitForFunction(() => document.body.dataset.writeSeq === '1')
  assert.equal(await composer.evaluate(() => document.body.dataset.writeOutcome), 'written')
  assert.equal(await composer.evaluate(() => document.body.dataset.written), S1)
  assert.equal(await composer.$eval('#tray', e => e.dataset.count), '0')
  assert.deepEqual(posts.map(p => [new URL(p.url).pathname + new URL(p.url).search, p.token, p.body.kind]), [
    ['/api/put?dry_run=true', 't', 'Selection'], ['/api/put', 't', 'Selection'], ['/api/put', 't', 'Claim']])
  assert.deepEqual(posts[1].body.members, [{ item: { address: { '/': item.A }, via: [{ '/': runs.R1.collection }] } }])
  assert.deepEqual([posts[2].body.subject, posts[2].body.verb, posts[2].body.attribute, posts[2].body.value, posts[2].body.supersedes],
    [{ '/': S1 }, 'set', 'name', 'smoke, "one"', []])

  // The write succeeds but the tail refresh throws: the outcome is still
  // recorded, and the status says to reload.
  await go(composer, `#/item/${runs.R1.collection}/${item.A}`)
  await composer.click(`[data-pick="${item.A}"]`)
  await go(composer, '#/compose')
  brokenLog = true
  await composer.click('#compose-save')
  await composer.waitForFunction(() => document.body.dataset.writeSeq === '2')
  brokenLog = false
  assert.equal(await composer.evaluate(() => document.body.dataset.writeOutcome), 'written')
  assert.match(await composer.$eval('[data-refresh-failed]', e => e.textContent), /^Saved, but the page could not refresh/)
  assert.equal(posts.length, 5)
  assert.equal(await composer.$eval('#compose-save', e => e.disabled), false)

  // A deleted Selection SD in the Store Log tail: the deleted list offers
  // [data-undo] per row, and undo supersedes the delete Claim D.
  const { block, entryName } = await import(join(repo, 'web/test/fixture.mjs'))
  const { CID } = await import(join(repo, 'web/node_modules/multiformats/dist/src/cid.js'))
  const store = (b) => {
    mkdirSync(join(dir, 'blocks', b.cid.toString().slice(-2)), { recursive: true })
    writeFileSync(join(dir, 'blocks', b.cid.toString().slice(-2), b.cid.toString()), b.bytes)
    return b.cid
  }
  const SD = store(block({ kind: 'Selection', schema: 1, asserted_by: 'smoke', derived_from: [],
    members: [{ item: { address: CID.parse(item.A), via: [CID.parse(runs.R1.collection)] } }] }))
  const D = store(block({ kind: 'Claim', schema: 1, asserted_by: 'smoke', subject: SD, verb: 'delete', attribute: null, value: null,
    supersedes: [], timestamp: new Date().toISOString() }))
  writeFileSync(join(dir, 'log', entryName(Date.now() - 2000, 'selection', SD.toString())), '')
  writeFileSync(join(dir, 'log', entryName(Date.now() - 1000, 'claim', D.toString())), '')
  const undoer = await writeContext.newPage()
  undoer.on('pageerror', e => composeErrors.push(String(e)))
  await open(undoer, 'http://127.0.0.1:8841/?token=t#/selections?deleted=1')
  assert.deepEqual(await all(undoer, '[data-selection]', ['selection', 'deletion']), [{ selection: SD.toString(), deletion: 'deleted' }])
  assert.deepEqual(await undoer.$$eval('[data-undo]', els => els.map(e => e.dataset.undo)), [SD.toString()])
  assert.equal(await undoer.$('#undo'), null)
  await undoer.click(`[data-undo="${SD}"]`)
  await undoer.waitForFunction(() => document.body.dataset.writeSeq === '1')
  assert.deepEqual([posts[5].body.subject, posts[5].body.verb, posts[5].body.supersedes], [{ '/': SD.toString() }, 'del', [{ '/': D.toString() }]])

  // Viewed through the non-writable alias: no rename or delete, a link to the
  // writable member instead; composing there opens the writable member and
  // the outcome survives that navigation.
  const elsewhere = await writeContext.newPage()
  elsewhere.on('pageerror', e => composeErrors.push(String(e)))
  await open(elsewhere, `http://127.0.0.1:8841/?token=t&member=other#/selection/${SD}`)
  assert.equal(await elsewhere.$('#rename-save'), null)
  assert.equal(await elsewhere.$('#delete'), null)
  assert.match(await elsewhere.$eval('[data-unavailable] a', e => e.href), /member=main/)
  await go(elsewhere, `#/item/${runs.R1.collection}/${item.A}`)
  await elsewhere.click(`[data-pick="${item.A}"]`)
  await go(elsewhere, '#/compose')
  await elsewhere.click('#compose-save')
  await elsewhere.waitForURL(/member=main/)
  await elsewhere.waitForFunction(() => document.body.dataset.writeSeq === '1' && document.body.dataset.state !== 'loading')
  assert.equal(await elsewhere.evaluate(() => document.body.dataset.writeOutcome), 'written')
  assert.equal(await elsewhere.evaluate(() => document.body.dataset.written), S1)
  assert.equal(await elsewhere.$eval('#tray', e => e.dataset.count), '0')
  assert.equal(posts.length, 8)

  assert.deepEqual(composeErrors, [])

  console.log('page smoke: ok')
} finally {
  await browser.close()
  for (const s of servers) s.kill()
}
