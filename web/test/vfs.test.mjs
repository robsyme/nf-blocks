import { test } from 'node:test'
import assert from 'node:assert/strict'
import { loadSqlite, makeDb, countingRange } from './helpers.mjs'
import { installReadOnlyVfs } from '../src/vfs/httpvfs.js'
import { ChunkedSource, MemorySource } from '../src/vfs/sources.js'

const ROWS = 2000
const statements = [
  'CREATE TABLE producer(content_cid TEXT, item_cid TEXT, filename TEXT)',
  'CREATE INDEX producer_content_cid ON producer(content_cid)',
  `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ${ROWS})
   INSERT INTO producer SELECT 'c' || (i % 50), 'item' || i, 'f' || i || '.bam' FROM n`,
]

let counter = 0
async function openOver(source) {
  const sqlite3 = await loadSqlite()
  const vfs = installReadOnlyVfs(sqlite3, `test-${++counter}`)
  vfs.register('snap', source)
  const db = new sqlite3.oo1.DB({ filename: 'file:snap?immutable=1', flags: 'r', vfs: `test-${counter}` })
  return { db, vfs }
}

test('rows read through the VFS are the rows the SQL selects', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const { db } = await openOver(new ChunkedSource(bytes.length, countingRange(bytes)))
  const expected = Array.from({ length: ROWS }, (_, k) => k + 1)
    .filter((i) => i % 50 === 7)
    .map((i) => [`item${i}`, `f${i}.bam`])
    .sort((a, b) => (a[0] < b[0] ? -1 : 1))
  const sql = "SELECT item_cid, filename FROM producer WHERE content_cid = 'c7' ORDER BY item_cid"
  assert.deepEqual(db.selectArrays(sql), expected)
  db.close()
})

test('a warm query costs no fetches', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const range = countingRange(bytes)
  const { db } = await openOver(new ChunkedSource(bytes.length, range))
  const sql = "SELECT count(*) FROM producer WHERE content_cid = 'c3'"
  db.selectValue(sql)
  const cold = range.calls.length
  assert.ok(cold > 0)
  db.selectValue(sql)
  assert.equal(range.calls.length, cold)
  db.close()
})

test('an index lookup reads a few chunks, not the table', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const range = countingRange(bytes)
  const { db } = await openOver(new ChunkedSource(bytes.length, range))
  db.selectValue("SELECT count(*) FROM producer WHERE content_cid = 'c3'")
  const fetched = range.calls.reduce((n, [a, b]) => n + (b - a + 1), 0)
  assert.ok(fetched < bytes.length / 2, `fetched ${fetched} of ${bytes.length} bytes`)
  db.close()
})

test('reads spanning chunk boundaries come back intact', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const { db } = await openOver(new ChunkedSource(bytes.length, countingRange(bytes), { chunk: 1000 }))
  assert.equal(db.selectValue('SELECT count(*) FROM producer'), ROWS)
  db.close()
})

test('contiguous missing chunks are fetched in one request', () => {
  const bytes = new Uint8Array(40960).map((_, i) => i & 0xff)
  const range = countingRange(bytes)
  const source = new ChunkedSource(bytes.length, range)
  assert.deepEqual([...source.read(4000, 9000)], [...bytes.subarray(4000, 13000)])
  assert.deepEqual(range.calls, [[0, 16383]])
  source.read(4096, 100)
  assert.equal(range.calls.length, 1)
})

test('the probe head seeds chunk 0 so it is not fetched again', () => {
  const bytes = new Uint8Array(8192).fill(7)
  const range = countingRange(bytes)
  const source = new ChunkedSource(bytes.length, range, { head: bytes.subarray(0, 4096) })
  source.read(0, 100)
  assert.equal(range.calls.length, 0)
})

test('the chunk cache is bounded', () => {
  const bytes = new Uint8Array(4096 * 10)
  const range = countingRange(bytes)
  const source = new ChunkedSource(bytes.length, range, { maxChunks: 2 })
  for (let c = 0; c < 10; c++) source.read(c * 4096, 1)
  assert.ok(source.cachedChunks() <= 2)
})

test('a short range answer fails the read instead of returning holes', () => {
  const source = new ChunkedSource(8192, () => new Uint8Array(10))
  assert.throws(() => source.read(0, 100), /short range read/)
})

test('a failed fetch surfaces as a query error with the cause', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  let calls = 0
  const flaky = (a, b) => { if (++calls > 1) throw new Error('network down'); return bytes.subarray(a, b + 1) }
  const { db, vfs } = await openOver(new ChunkedSource(bytes.length, flaky))
  assert.throws(() => db.selectValue('SELECT count(*) FROM producer'))
  assert.match(String(vfs.lastError?.message), /network down/)
  db.close()
})

test('the whole-file source answers the same', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const { db } = await openOver(new MemorySource(bytes))
  assert.equal(db.selectValue('SELECT count(*) FROM producer'), ROWS)
  db.close()
})

test('nothing can be written', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const { db } = await openOver(new MemorySource(bytes))
  assert.throws(() => db.exec("INSERT INTO producer VALUES ('x', 'y', 'z')"), /readonly|read-only/i)
  db.close()
})
