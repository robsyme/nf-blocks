// A snapshot rewritten under an open page (final review finding 1, spec
// section 4: "A stale snapshot costs requests, never a wrong answer"). The
// page keeps the version the probe saw and refuses a range from any other.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { loadSqlite, makeDb } from './helpers.mjs'
import { installReadOnlyVfs } from '../src/vfs/httpvfs.js'
import { ChunkedSource, Counter, xhrRange } from '../src/vfs/sources.js'
import { openSnapshot, probeSnapshot, SnapshotError } from '../src/db.js'

const mk = (sqlite3, rows, salt) => makeDb(sqlite3, [
  'CREATE TABLE producer(content_cid TEXT, item_cid TEXT, filename TEXT)',
  'CREATE INDEX producer_content_cid ON producer(content_cid)',
  `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ${rows})
   INSERT INTO producer SELECT 'c' || ((i * ${salt}) % 50), 'item' || i, 'f' || i || '.bam' FROM n`])

/** A server holding one file that can be replaced; `headers` picks what its 206s carry. */
function server(file) {
  const state = { file, requests: 0 }
  class FakeXhr {
    open(method, url, async) { assert.equal(async, false) }
    setRequestHeader(name, value) { if (name === 'Range') this.range = value }
    send() {
      state.requests++
      const { bytes, headers } = state.file
      const [, s, e] = /^bytes=(\d+)-(\d+)$/.exec(this.range)
      const start = Number(s), end = Math.min(Number(e), bytes.length - 1)
      this.status = 206
      this.response = bytes.slice(start, end + 1).buffer
      this.headers = { ...headers, 'content-range': `bytes ${start}-${end}/${bytes.length}` }
    }
    getResponseHeader(name) { return this.headers[name.toLowerCase()] ?? null }
  }
  state.newXhr = () => new FakeXhr()
  return state
}

let vfsCount = 0
async function openOver(srv, version) {
  const sqlite3 = await loadSqlite()
  const name = `swap-${++vfsCount}`
  const vfs = installReadOnlyVfs(sqlite3, name)
  const size = srv.file.bytes.length
  vfs.register('snap', new ChunkedSource(size, xhrRange('http://h/index/v2.sqlite', new Counter(), { ...version, size, newXhr: srv.newXhr })))
  const db = new sqlite3.oo1.DB({ filename: 'file:snap?immutable=1', flags: 'r', vfs: name })
  return { db, vfs }
}

const count = (db, c) => db.selectValue(`SELECT count(*) FROM producer WHERE content_cid = '${c}'`)

for (const [label, before, after] of [
  ['a new ETag', { etag: '"a"' }, { etag: '"b"' }],
  ['a new Last-Modified when there is no ETag', { 'last-modified': 'Thu, 25 Sep 2026 10:00:00 GMT' }, { 'last-modified': 'Thu, 25 Sep 2026 10:05:00 GMT' }],
  ['a new size when neither header is readable', {}, {}],
]) {
  test(`a snapshot replaced under an open page fails its queries with snapshot_changed: ${label}`, async () => {
    const sqlite3 = await loadSqlite()
    const A = mk(sqlite3, 2000, 1)
    const B = mk(sqlite3, 2200, 7)
    const srv = server({ bytes: A, headers: before })
    const { db, vfs } = await openOver(srv, { tag: before.etag ?? before['last-modified'] ?? null })
    assert.equal(count(db, 'c3'), 40)
    srv.file = { bytes: B, headers: after }
    for (const c of ['c7', 'c11', 'c19', 'c23']) {
      assert.throws(() => count(db, c), `query for ${c} must fail, not answer from a mix of two files`)
      assert.equal(vfs.lastError?.code, 'snapshot_changed')
      assert.match(vfs.lastError.message, /reload the page/)
      vfs.clearError()
    }
    db.close()
  })
}

test('an unchanged snapshot checks its version without extra requests', async () => {
  const sqlite3 = await loadSqlite()
  const A = mk(sqlite3, 2000, 1)
  const srv = server({ bytes: A, headers: { etag: '"a"' } })
  const { db } = await openOver(srv, { tag: '"a"' })
  assert.equal(count(db, 'c3'), 40)
  const cold = srv.requests
  assert.equal(count(db, 'c7'), 40)
  assert.ok(srv.requests - cold <= 3, `a second lookup cost ${srv.requests - cold} requests`)
  db.close()
})

test('the probe records the snapshot version: ETag, else Last-Modified', async () => {
  const head = new Uint8Array(4096)
  const probe = (headers) => probeSnapshot('http://h/index/v2.sqlite', {
    fetchFn: async () => new Response(head, { status: 206, headers: { 'Content-Range': 'bytes 0-4095/8192', ...headers } }) })
  assert.equal((await probe({ ETag: '"x"', 'Last-Modified': 'Thu, 25 Sep 2026 10:00:00 GMT' })).tag, '"x"')
  assert.equal((await probe({ 'Last-Modified': 'Thu, 25 Sep 2026 10:00:00 GMT' })).tag, 'Thu, 25 Sep 2026 10:00:00 GMT')
  assert.equal((await probe({})).tag, null)
})

test('the worker hands snapshot_changed to the page as that code, and the probe version to the worker', async () => {
  const head = new Uint8Array(4096)
  const fetchFn = async () => new Response(head, { status: 206, headers: { 'Content-Range': 'bytes 0-4095/8192', ETag: '"x"' } })
  const opened = []
  const worker = {
    postMessage(msg) {
      if (msg.op === 'open') opened.push(msg)
      const reply = msg.op === 'open' ? { id: msg.id, ok: true, result: {} }
        : { id: msg.id, ok: false, code: 'snapshot_changed', error: 'the snapshot was rewritten; reload the page' }
      queueMicrotask(() => worker.onmessage({ data: reply }))
    },
    terminate() {},
  }
  const db = await openSnapshot('http://h/index/v2.sqlite', { wasm: new Uint8Array(4), createWorker: () => worker, fetchFn })
  assert.equal(opened[0].tag, '"x"')
  await assert.rejects(db.query('SELECT 1'), e => e instanceof SnapshotError && e.code === 'snapshot_changed')
})
