// A member's snapshot rewritten by explore while a page has it open (a push,
// a refresh, a copy into a member indexed on this machine; acceptance F1).
// The Explorer re-opens the snapshot once and asks again; only a second
// change during that retry reaches the page as snapshot_changed.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { loadSqlite } from './helpers.mjs'
import { buildMember, blockFetch } from './fixture.mjs'
import { installReadOnlyVfs } from '../src/vfs/httpvfs.js'
import { ChunkedSource, Counter, xhrRange } from '../src/vfs/sources.js'
import { SnapshotError } from '../src/db.js'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'

/**
 * A server holding one snapshot file. Each tag in `rewrites` is applied at the
 * first range request after an open (a probe), so a rewrite lands in the
 * middle of whatever reads next.
 */
function server(bytes) {
  const srv = { bytes, tag: '"a"', rewrites: [], armed: false, opens: 0, closed: 0 }
  class FakeXhr {
    open() {}
    setRequestHeader(name, value) { if (name === 'Range') this.range = value }
    send() {
      if (srv.armed && srv.rewrites.length) { srv.tag = srv.rewrites.shift(); srv.armed = false }
      const [, s, e] = /^bytes=(\d+)-(\d+)$/.exec(this.range)
      const start = Number(s), end = Math.min(Number(e), srv.bytes.length - 1)
      this.status = 206
      this.response = srv.bytes.slice(start, end + 1).buffer
      this.headers = { etag: srv.tag, 'content-range': `bytes ${start}-${end}/${srv.bytes.length}` }
    }
    getResponseHeader(name) { return this.headers[name.toLowerCase()] ?? null }
  }
  let vfsCount = 0
  // What openSnapshot and the worker do: probe the version, then read pages in chunks checked against it.
  srv.openDb = async (url) => {
    const sqlite3 = await loadSqlite()
    const name = `reopen-${++vfsCount}-${Math.random()}`
    const vfs = installReadOnlyVfs(sqlite3, name)
    const size = srv.bytes.length
    vfs.register('snap', new ChunkedSource(size, xhrRange(url, new Counter(), { tag: srv.tag, size, newXhr: () => new FakeXhr() }), { chunk: 4096 }))
    const db = new sqlite3.oo1.DB({ filename: 'file:snap?immutable=1', flags: 'r', vfs: name })
    srv.opens++
    srv.armed = true
    return {
      query: async (sql, params = []) => {
        vfs.clearError()
        try {
          return db.selectObjects(sql, params).map(r => ({ ...r }))
        } catch (e) {
          throw vfs.lastError ? new SnapshotError(vfs.lastError.code ?? 'query_failed', vfs.lastError.message) : e
        }
      },
      close: async () => { srv.closed++; db.close() },
    }
  }
  return srv
}

async function openOver(srv, member) {
  const explorer = await Explorer.open({
    base: 'http://h/m/lab/', openDb: srv.openDb,
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }),
    listFn: async () => ({ names: member.log, readable: true }),
  })
  srv.armed = true
  return explorer
}

const attrs = (explorer, member) => explorer.db.query('SELECT path, value FROM item_attr WHERE item_cid = ? ORDER BY path', [member.item.A])

test('a query that meets a rewritten snapshot re-opens it once and answers from the new file', async () => {
  const member = await buildMember()
  const srv = server(member.snapshot)
  const explorer = await openOver(srv, member)
  srv.rewrites = ['"b"']
  const rows = await attrs(explorer, member)
  assert.deepEqual(rows.map(r => r.path), ['depth', 'lane', 'sample'])
  assert.equal(srv.opens, 2, 'the member is opened once more')
  assert.equal(srv.closed, 1, 'and the old file is let go')
  assert.equal(explorer.snapshotWrittenAt, '2026-09-01T10:00:01.000Z')
  assert.deepEqual((await explorer.runsOfPipeline('demo')).map(r => r.run_name).sort(), ['R1', 'R2', 'R3'])
})

test('a second rewrite during the retry reaches the page as snapshot_changed', async () => {
  const member = await buildMember()
  const srv = server(member.snapshot)
  const explorer = await openOver(srv, member)
  srv.rewrites = ['"b"', '"c"']
  await assert.rejects(attrs(explorer, member), e => e instanceof SnapshotError && e.code === 'snapshot_changed')
  assert.equal(srv.opens, 2, 'it re-opens once, not again')
})

test('queries that meet the same rewrite wait on one re-open', async () => {
  const member = await buildMember()
  const srv = server(member.snapshot)
  const explorer = await openOver(srv, member)
  srv.rewrites = ['"b"']
  const answers = await Promise.all([attrs(explorer, member), explorer.db.query('SELECT count(*) AS n FROM producer'),
    explorer.db.query('SELECT count(*) AS n FROM collection_item')])
  assert.equal(answers[0].length, 3)
  assert.equal(answers[1][0].n, 2)
  assert.equal(srv.opens, 2)
})

test('a failure that is not a rewrite is not retried', async () => {
  const member = await buildMember()
  const srv = server(member.snapshot)
  const explorer = await openOver(srv, member)
  await assert.rejects(explorer.db.query('SELECT * FROM no_such_table'))
  assert.equal(srv.opens, 1)
})

test('after a retry that failed, a later read re-opens the member again', async () => {
  const member = await buildMember()
  const srv = server(member.snapshot)
  const explorer = await openOver(srv, member)
  srv.rewrites = ['"b"', '"c"']
  await assert.rejects(attrs(explorer, member), e => e?.code === 'snapshot_changed')
  assert.equal((await attrs(explorer, member)).length, 3)
  assert.equal(srv.opens, 3)
  assert.equal(srv.closed, 2, 'the file that failed to load and the one it replaced are both let go')
})
