// The Mine filter on a pipeline's runs and on the Selections list (P1b task 5).
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { loadSqlite, makeDb, snapshotDb } from './helpers.mjs'
import { blockFetch } from './fixture.mjs'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { viewerOf } from '../src/identity.js'
import { pipeline } from '../src/views/pipeline.js'
import { selections } from '../src/views/selection.js'
import { parsePipelineQuery, parseSelectionsQuery } from '../src/listquery.js'
import SQL from '../src/queries.json' with { type: 'json' }

const SCHEMA = readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8')

/** Runs of `p` by gate and Rob (in two spellings), Selections by gate and Rob, and no tail yet. */
async function openMine(listed = []) {
  const sqlite3 = await loadSqlite()
  const run = (cid, by, at) => `INSERT INTO run (completion_cid, pipeline, run_name, asserted_by, status, possibly_incomplete, finished_at)
    VALUES ('${cid}', 'p', '${cid}', ${by === null ? 'NULL' : `'${by}'`}, 'succeeded', 0, '2026-09-01T10:0${at}:00.000Z')`
  const sel = (cid, by, at) => [`INSERT INTO collection(collection_cid, kind, asserted_by) VALUES ('${cid}', 'selection', '${by}')`,
    `INSERT INTO log_entry VALUES ('${cid}', 'selection', NULL, '2026-09-01T10:0${at}:00.000Z')`]
  const bytes = makeDb(sqlite3, [SCHEMA,
    run('r1', 'gate', 1), run('r2', 'Ada@Example.org', 2), run('r3', ' ada@example.org', 3), run('r4', null, 4),
    ...sel('s1', 'gate', 1), ...sel('s2', 'Ada@Example.org', 2)])
  return Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(bytes),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(new Map()) }), listFn: async () => ({ names: listed, readable: true }) })
}

test('every ...By query prepares against the index schema and keeps its sibling\'s shape', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, [SCHEMA])
  const db = await snapshotDb(bytes)
  for (const [name, params] of [['runsOfPipelineBy', ['p', 'rob', 10, 0]], ['runCountBy', ['p', 'rob']],
    ['selectionsPageBy', ['rob', 10, 0]], ['selectionCountBy', ['rob']]]) {
    assert.equal(typeof SQL[name], 'string', name)
    await db.query(SQL[name], params)
    assert.match(SQL[name], /lower\(trim\((\w+\.)?asserted_by, char\(32,9,10,13,12,11\)\)\) = lower\(trim\(\?, char\(32,9,10,13,12,11\)\)\)/, name)
  }
  assert.match(SQL.runsOfPipelineBy, /asserted_by/)
})

test('every ...By query matches an asserted_by padded with tab and newline, on real SQLite', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, [SCHEMA,
    `INSERT INTO run (completion_cid, pipeline, run_name, asserted_by, status, possibly_incomplete, finished_at)
      VALUES ('rw', 'p', 'rw', char(9) || 'ADA@example.org' || char(10), 'succeeded', 0, '2026-09-01T10:00:00.000Z')`,
    `INSERT INTO collection(collection_cid, kind, asserted_by) VALUES ('sw', 'selection', char(9) || 'ada@example.org' || char(10))`,
    `INSERT INTO log_entry VALUES ('sw', 'selection', NULL, '2026-09-01T10:00:00.000Z')`])
  const db = await snapshotDb(bytes)
  assert.equal((await db.query(SQL.runsOfPipelineBy, ['p', 'ada@example.org', 10, 0])).length, 1)
  assert.equal((await db.query(SQL.runCountBy, ['p', 'ada@example.org']))[0].n, 1)
  assert.equal((await db.query(SQL.selectionsPageBy, ['ada@example.org', 10, 0])).length, 1)
  assert.equal((await db.query(SQL.selectionCountBy, ['ada@example.org']))[0].n, 1)
  // SQLite's lower() folds ASCII only, which is the rule identity.js principalKey copies.
  assert.equal((await db.query("SELECT lower('\u00c9') AS l"))[0].l, '\u00c9')
})

test('runPage by a principal returns only that person\'s runs and a matching total, trimmed and case-folded', async () => {
  const ex = await openMine()
  const all = await ex.runPage('p')
  assert.equal(all.total, 4)
  const mine = await ex.runPage('p', { by: 'ada@example.org ' })
  assert.deepEqual(mine.rows.map(r => r.completion_cid), ['r3', 'r2'])
  assert.equal(mine.total, 2)
  assert.deepEqual([mine.first, mine.last], [1, 2])
  assert.deepEqual(mine.rows.map(r => r.asserted_by), [' ada@example.org', 'Ada@Example.org'])
})

test('the tail keeps a run by ADA@example.org under Mine and drops one by gate', async () => {
  const ex = await openMine()
  const tailRow = (cid, by) => ({ cid, row: { completion_cid: cid, pipeline: 'p', run_name: cid, asserted_by: by, status: 'succeeded',
    possibly_incomplete: 0, finished_at: '2026-09-02T00:00:00.000Z', source: 'tail' } })
  ex.stale = [tailRow('t1', 'ADA@example.org'), tailRow('t2', 'gate')]
  const mine = await ex.runPage('p', { by: 'ada@example.org' })
  assert.deepEqual(mine.rows.map(r => r.completion_cid), ['t1', 'r3', 'r2'])
  assert.equal(mine.total, 3, 'the tail counts only the kept runs')
  assert.equal((await ex.runPage('p')).total, 6)
})

test('selectionPage by a principal lists only that person\'s Selections, snapshot and tail', async () => {
  const ex = await openMine()
  assert.deepEqual((await ex.selectionPage()).rows.map(r => r.cid), ['s2', 's1'])
  const mine = await ex.selectionPage({ by: ' ada@example.org' })
  assert.deepEqual(mine.rows.map(r => r.cid), ['s2'])
  assert.equal(mine.total, 1)
  const tail = (cid, by) => ({ cid, entry: { writtenAtMillis: Date.parse('2026-09-03T00:00:00Z') }, error: null, value: { asserted_by: by, members: [] } })
  ex.tailSelections = [tail('t1', 'ADA@example.org'), tail('t2', 'gate')]
  assert.deepEqual((await ex.selectionPage({ by: 'ada@example.org' })).rows.map(r => r.cid), ['t1', 's2'])
})

test('the list routes read offset and mine, and old offset-only links still work', () => {
  assert.deepEqual(parsePipelineQuery('offset=25&mine=1'), { offset: 25, mine: true })
  assert.deepEqual(parsePipelineQuery('offset=25'), { offset: 25, mine: false })
  assert.deepEqual(parsePipelineQuery(undefined), { offset: 0, mine: false })
  assert.deepEqual(parseSelectionsQuery('offset=25&deleted=1&mine=1'), { offset: 25, deleted: true, mine: true })
  assert.deepEqual(parseSelectionsQuery(undefined), { offset: 0, deleted: false, mine: false })
})

test('an offset that is not a finite number at or above 0 reads as 0', () => {
  for (const bad of ['abc', '-5', 'Infinity', 'NaN', '']) {
    assert.equal(parsePipelineQuery(`offset=${bad}`).offset, 0, bad)
    assert.equal(parseSelectionsQuery(`offset=${bad}&mine=1`).offset, 0, bad)
  }
  assert.equal(parseSelectionsQuery('offset=abc&mine=1').mine, true)
})

test('a tail run row takes asserted_by from its RunCompletion, as the snapshot does', async () => {
  const blocks = { ofKind: async (cid, kind) => ({ value: kind === 'RunCompletion'
    ? { run: 'bafymanifest', status: 'succeeded', possibly_incomplete: false, finished_at: '2026-09-02T00:00:00.000Z', asserted_by: 'completion@x' }
    : { pipeline: 'p', run_name: 'r', asserted_by: 'manifest@x', nf_run_hash: null } }) }
  const stale = await Explorer.prototype.staleRun.call({ blocks }, { cid: 'bafycompletion' })
  assert.equal(stale.row.asserted_by, 'completion@x')
})

const ROB = { principal: 'Ada@Example.org', source: 'git' }
const runRow = (cid, by) => ({ completion_cid: cid, pipeline: 'p', run_name: cid, status: 'succeeded', possibly_incomplete: 0, asserted_by: by,
  finished_at: '2026-09-30T21:10:38.345Z', source: 'snapshot' })
const RUNS = [runRow('rA', 'gate'), runRow('rB', 'Ada@Example.org')]
const ctxOf = (identity) => ({ tray: new Tray(null), identity, viewer: viewerOf(identity), store: { name: 'roblab', key: 'a' },
  write: { available: false, reason: 'read only', served: true, hrefFor: (x) => x, here: false }, use() {}, mark() {} })
function pipeEx(asked = []) {
  return { stale: [], completionOf: async () => ({ anomalies: {} }),
    runPage: async (name, opts) => {
      asked.push(opts)
      const rows = opts.by ? RUNS.filter(r => r.asserted_by.toLowerCase() === opts.by.toLowerCase()) : RUNS
      return { rows, hidden: 0, first: 1, last: rows.length, total: rows.length, prev: null, next: null }
    } }
}
const toggleOf = (node) => node.querySelector('[data-mine-toggle]')

test('the pipeline view under Mine asks for the viewer\'s runs, shows only them, and presses Mine', async () => {
  installDom()
  const asked = []
  const node = await pipeline(pipeEx(asked), 'p', 0, ctxOf(ROB), { mine: true })
  await frame()
  assert.equal(asked[0].by, 'Ada@Example.org')
  assert.deepEqual(node.querySelectorAll('[data-run]').map(r => r.dataset.run), ['rB'])
  const [all, mine] = toggleOf(node).querySelectorAll('a')
  assert.equal(all.getAttribute('aria-pressed'), 'false')
  assert.equal(mine.getAttribute('aria-pressed'), 'true')
  assert.equal(all.getAttribute('href'), '#/pipeline/p')
  assert.equal(mine.getAttribute('href'), '#/pipeline/p?mine=1')
  assert.ok(node.querySelector('[data-run]').querySelector('.av'), 'the By cell holds an avatar')
})

test('the pipeline view without Mine reads no filter, and the toggle keeps the page at offset 0', async () => {
  installDom()
  const asked = []
  const node = await pipeline(pipeEx(asked), 'p', 50, ctxOf(ROB))
  await frame()
  assert.equal(asked[0].by ?? null, null)
  assert.equal(node.querySelectorAll('[data-run]').length, 2)
  const [, mine] = toggleOf(node).querySelectorAll('a')
  assert.equal(mine.getAttribute('aria-pressed'), 'false')
  assert.equal(mine.getAttribute('href'), '#/pipeline/p?mine=1')
})

test('Mine with nothing signed by the viewer says so', async () => {
  installDom()
  const ex = { ...pipeEx(), runPage: async () => ({ rows: [], hidden: 0, first: 0, last: 0, total: 0, prev: null, next: null }) }
  const node = await pipeline(ex, 'p', 0, ctxOf(ROB), { mine: true })
  assert.match(node.querySelector('.empty').textContent, /None of these runs were signed by Ada@Example\.org\./)
  assert.equal(node.querySelector('table'), null, 'no empty table header under the empty state')
})

test('anonymous identity: Mine is a disabled button with the reason, and nothing is filtered', async () => {
  installDom()
  const asked = []
  const node = await pipeline(pipeEx(asked), 'p', 0, ctxOf({ principal: 'anonymous', source: 'anonymous' }), { mine: true })
  await frame()
  assert.equal(asked[0].by ?? null, null)
  const mine = toggleOf(node).querySelector('button')
  assert.ok(mine.hasAttribute('disabled'))
  assert.match(mine.getAttribute('title'), /No identity is configured, so none of these can be marked as yours\./)
  assert.equal(node.querySelectorAll('[data-run]').length, 2)
})

test('null identity: no Mine toggle at all, on either list', async () => {
  installDom()
  const node = await pipeline(pipeEx(), 'p', 0, ctxOf(null))
  assert.equal(toggleOf(node), null)
  const page = await selections(selEx(), {}, ctxOf(null))
  assert.equal(toggleOf(page), null)
})

const state = { names: ['a'], deletion: 'current', nameConflicted: false }
const SELS = [{ cid: 'sA', source: 'snapshot', firstSeen: 'x', assertedBy: 'gate', state }, { cid: 'sB', source: 'snapshot', firstSeen: 'y', assertedBy: 'Ada@Example.org', state },
  { cid: 'sC', source: 'snapshot', firstSeen: 'z', assertedBy: null, state }]
function selEx(asked = []) {
  return { selectionPage: async (opts) => {
    asked.push(opts)
    const rows = opts.by ? SELS.filter(r => r.assertedBy?.toLowerCase() === opts.by.toLowerCase()) : SELS
    return { rows, hiddenCount: 0, first: 1, last: rows.length, total: rows.length, prev: null, next: null }
  } }
}

test('the Selections list: Mine filters, keeps other params, and the by column shows avatars with the principal', async () => {
  installDom()
  const asked = []
  const all = await selections(selEx(asked), { deleted: true }, ctxOf(ROB))
  assert.equal(asked[0].by ?? null, null)
  const [allLink, mineLink] = toggleOf(all).querySelectorAll('a')
  assert.equal(allLink.getAttribute('href'), '#/selections?deleted=1')
  assert.equal(mineLink.getAttribute('href'), '#/selections?deleted=1&mine=1')
  const rows = all.querySelectorAll('[data-selection]')
  assert.equal(rows.length, 3)
  assert.ok(rows[1].querySelector('.av'))
  assert.match(rows[1].textContent, /Ada@Example\.org/)
  assert.ok(rows[2].querySelector('.av.anon'), 'a Selection with no signer shows the anonymous avatar')

  const mine = await selections(selEx(asked), { mine: true }, ctxOf(ROB))
  assert.equal(asked.at(-1).by, 'Ada@Example.org')
  assert.deepEqual(mine.querySelectorAll('[data-selection]').map(r => r.dataset.selection), ['sB'])
  assert.equal(toggleOf(mine).querySelectorAll('a')[1].getAttribute('aria-pressed'), 'true')
})

test('the Selections list under Mine with nothing to show, and with anonymous identity', async () => {
  installDom()
  const none = { selectionPage: async () => ({ rows: [], hiddenCount: 0, first: 0, last: 0, total: 0, prev: null, next: null }) }
  const page = await selections(none, { mine: true }, ctxOf(ROB))
  assert.match(page.querySelector('.empty').textContent, /No Selections signed by Ada@Example\.org\./)
  const anon = await selections(selEx(), { mine: true }, ctxOf({ principal: 'anonymous', source: 'anonymous' }))
  assert.ok(toggleOf(anon).querySelector('button').hasAttribute('disabled'))
})
