// web/test/run-counts.test.mjs
// Explorer.runCounts hides exactly the runs the pipeline page hides (claimState).
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { Explorer } from '../src/model.js'
import { loadSqlite, makeDb, snapshotDb } from './helpers.mjs'

async function countsWith(claims, current, tail = []) {
  const sqlite3 = await loadSqlite()
  const run = (c) => `('${c}', 'p', '${c}', 'succeeded', 0, '2026-10-24T17:00:00.000Z')`
  const bytes = makeDb(sqlite3, [readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'),
    `INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES ${['a', 'b', 'c', 'd'].map(run).join(', ')}`,
    ...claims.map(([cid, subject, verb]) => `INSERT INTO claim VALUES ('${cid}', '${subject}', '${verb}', NULL, NULL, '2026-10-24T20:00:00.000Z', 'anonymous')`),
    ...current.map(([cid, subject]) => `INSERT INTO claim_current VALUES ('${subject}', NULL, NULL, '${cid}', 0)`)])
  const ex = new Explorer({ base: 'http://h/m/lab/', db: await snapshotDb(bytes), blocks: null, now: () => Date.now() })
  ex.tailClaims = tail.map(([cid, subject, verb, supersedes = []]) => ({ cid, value: { subject, verb, attribute: null, value: null, timestamp: 't', asserted_by: 'anonymous', supersedes } }))
  return (await ex.runCounts()).get('p').runs
}

test('a conflicted deletion stays counted; only exactly one current delete hides a run', async () => {
  // a: two current deletes (conflicted); b: one delete (hidden); c: a delete and a del both current (conflicted); d: none.
  const claims = [['k1', 'a', 'delete'], ['k2', 'a', 'delete'], ['k3', 'b', 'delete'], ['k4', 'c', 'delete'], ['k5', 'c', 'del']]
  const current = [['k1', 'a'], ['k2', 'a'], ['k3', 'b'], ['k4', 'c'], ['k5', 'c']]
  assert.equal(await countsWith(claims, current), 3)
})

test('a tail delete hides a snapshot run and a tail undo restores a snapshot-deleted one', async () => {
  const claims = [['k1', 'b', 'delete']]
  const current = [['k1', 'b']]
  assert.equal(await countsWith(claims, current), 3, 'b is hidden by the snapshot')
  assert.equal(await countsWith(claims, current, [['t1', 'a', 'delete']]), 2, 'a tail delete of a')
  assert.equal(await countsWith(claims, current, [['t2', 'b', 'del', ['k1']]]), 4, 'a tail undo of b')
})
