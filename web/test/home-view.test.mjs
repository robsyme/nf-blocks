// web/test/home-view.test.mjs
// The Pipelines home, the pipeline page's health and sizes, the health copy
// and Explorer.recentActivity (explorer v0.3, task 3).
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { frame, installDom } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { shortCid } from '../src/icons.js'
import { claimState } from '../src/claims.js'
import { Explorer } from '../src/model.js'
import { healthLine, sharedHealth } from '../src/words.js'
import { home } from '../src/views/home.js'
import { pipeline } from '../src/views/pipeline.js'
import { loadSqlite, makeDb, snapshotDb } from './helpers.mjs'
import { openRoblab, openSnapshots, roblabLeanSnapshot } from './storage-fixture.mjs'

const ctx = () => ({ tray: new Tray(null), rerender: () => {}, trayChanged: () => {}, mark: () => {},
  store: { name: 'roblab', key: 'a' }, write: { available: false, reason: 'read only', hrefFor: (h) => h } })
const settle = async () => { await frame(); await frame(); await new Promise(r => setTimeout(r, 10)) }
const cells = (tr) => tr.querySelectorAll('td').map(td => td.textContent)

test('healthLine: unjoined files are named with the fix; inputs alone are expected', () => {
  const both = healthLine({ unjoined: 5, never_published: 5, declined: 439 })
  assert.equal(both.act, true)
  assert.match(both.text, /5 published files are outside any workflow output/)
  assert.match(both.text, /Declare them in the output \{ \} block to keep their lineage/)
  assert.match(both.text, /5 inputs are referenced, not stored/)
  const inputs = healthLine({ never_published: 5 })
  assert.equal(inputs.act, false)
  assert.match(inputs.text, /referenced, not stored/)
  assert.match(inputs.text, /Expected for local inputs/)
  assert.equal(healthLine({ declined: 3 }), null)
  assert.equal(healthLine({}), null)
  assert.match(healthLine({ unjoined: 1 }).text, /^1 published file is outside any workflow output\. Declare it in the output/)
})

test('sharedHealth: the line when every run has the same anomalies, else null', () => {
  const a = { anomalies: { unjoined: 5, never_published: 5 } }
  assert.equal(sharedHealth([a, { anomalies: { ...a.anomalies } }]).act, true)
  assert.equal(sharedHealth([a, { anomalies: { unjoined: 5, never_published: 4 } }]), null)
  assert.equal(sharedHealth([{ anomalies: {} }, { anomalies: {} }]), null)
  assert.equal(sharedHealth([{ anomalies: { unjoined: 5, declined: 439 } }, { anomalies: { unjoined: 5, declined: 12 } }]).act, true, 'declined slots are routine and do not split the line')
  assert.equal(sharedHealth([]), null)
})

const RUNS = {
  rnaseq: [
    { completion_cid: 'r3', pipeline: 'nf-core/rnaseq', run_name: 'distraught_murdock', status: 'succeeded', possibly_incomplete: 0, finished_at: '2026-10-24T17:29:03.967Z', source: 'snapshot' },
    { completion_cid: 'r2', pipeline: 'nf-core/rnaseq', run_name: 'sad_yonath', status: 'succeeded', possibly_incomplete: 0, finished_at: '2026-10-23T19:01:08.237Z', source: 'snapshot' },
    { completion_cid: 'r1', pipeline: 'nf-core/rnaseq', run_name: 'sharp_magritte', status: 'failed', possibly_incomplete: 1, finished_at: '2026-10-23T18:59:36.352Z', source: 'snapshot' }],
  main: [{ completion_cid: 'm1', pipeline: 'main.nf', run_name: 'sleepy_heyrovsky', status: 'succeeded', possibly_incomplete: 0, finished_at: '2026-10-24T17:58:50.649Z', source: 'snapshot' }],
}
const ANOMALIES = { unjoined: 5, never_published: 5, declined: 0 }

function homeEx() {
  const name = claimState([{ cid: 'k1', subject: 'sel1', verb: 'set', attribute: 'name', value: 'wigshop', timestamp: '2026-10-24T19:40:16.459Z', asserted_by: 'anonymous', supersedes: [] }])
  return {
    stale: [],
    pipelines: async () => [{ pipeline: 'main.nf', runs: 1, latest: RUNS.main[0].finished_at }, { pipeline: 'nf-core/rnaseq', runs: 3, latest: RUNS.rnaseq[0].finished_at }],
    runsOfPipeline: async (p) => (p === 'main.nf' ? RUNS.main : RUNS.rnaseq),
    runCounts: async () => new Map([['main.nf', { runs: 1, ok: 1, failed: 0 }], ['nf-core/rnaseq', { runs: 3, ok: 2, failed: 1 }]]),
    completionOf: async (cid) => ({ anomalies: cid === 'r3' ? ANOMALIES : {} }),
    latestSuccessfulRun: async (p) => (p === 'main.nf' ? 'm1' : 'r3'),
    runRow: async (cid) => [...RUNS.main, ...RUNS.rnaseq].find(r => r.completion_cid === cid),
    pipelineSize: async (p) => (p === 'main.nf' ? { bytes: 0, blocks: 0, partial: false } : { bytes: 142_000_000, blocks: 10, partial: false }),
    selectionPage: async () => ({ rows: [{ cid: 'sel1', state: name }] }),
    selection: async () => ({ members: [{}, {}, {}] }),
    selectionSize: async () => ({ bytes: 1_135_852, blocks: 4, files: 3, partial: false }),
    recentActivity: async () => [
      { cid: 'k1', kind: 'claim', written_at: '2026-10-24T19:40:16.474Z', claim: { verb: 'set', attribute: 'name', value: 'wigshop', subject: 'sel1', assertedBy: 'anonymous', timestamp: '2026-10-24T19:40:16.459Z' } },
      { cid: 'sel1', kind: 'selection', written_at: '2026-10-24T19:40:16.451Z', name: 'wigshop' },
      { cid: 'm1', kind: 'run', written_at: '2026-10-24T17:58:50.672Z', name: 'sleepy_heyrovsky' }],
  }
}

test('home: the pipelines table, newest run first, with counts, sizes and the empty state', async () => {
  installDom()
  const page = await home(homeEx(), ctx())
  await settle()
  assert.deepEqual(page.querySelectorAll('th').map(t => t.textContent), ['Pipeline', 'Runs', 'Latest good run', 'Last run', 'Stored', 'Health'])
  const rows = page.querySelectorAll('tr[data-pipeline]')
  assert.deepEqual(rows.map(r => r.dataset.pipeline), ['main.nf', 'nf-core/rnaseq'])
  const rna = cells(rows[1])
  assert.equal(rna[1], '3 · 2 ok · 1 failed')
  assert.match(rna[2], /distraught_murdock/)
  assert.equal(rna[4], '142 MB')
  assert.match(rna[5], /5 published files are outside any workflow output/)
  assert.equal(cells(rows[0])[4], 'No outputs published')
  assert.equal(cells(rows[0])[5], '')
  assert.ok(rows[1].querySelectorAll('a').some(a => a.getAttribute('href') === '#/run/r3'))
  assert.match(page.querySelector('.page-head').textContent, /roblab/)
  assert.match(page.querySelector('.page-head').textContent, /4 runs/)
  assert.doesNotMatch(page.textContent, /undefined|null|bafy/)
})

test('home: a partial Stored figure reads "at least", and its title says what it counts', async () => {
  installDom()
  const ex = homeEx()
  ex.pipelineSize = async () => ({ bytes: 5_290_000, blocks: 15, partial: true, runs: 10, more: true })
  const page = await home(ex, ctx())
  await settle()
  const stored = page.querySelectorAll('[data-stored]')
  assert.ok(stored.every(c => c.textContent === 'at least 5.29 MB'), stored.map(c => c.textContent).join('|'))
  assert.match(stored[0].getAttribute('title'), /newest 10 runs/)
  assert.match(stored[0].getAttribute('title'), /hidden by a delete Claim/)
})

test('home: Selections with name, size and claim tag; activity with fact and claim tags', async () => {
  installDom()
  const page = await home(homeEx(), ctx())
  await settle()
  const sel = page.querySelector('[data-home-selection=sel1]')
  assert.match(sel.textContent, /wigshop/)
  assert.match(sel.textContent, /3 items · 1\.14 MB/)
  assert.ok(sel.querySelector('.anon'), 'anonymous claims take the anonymous avatar')
  assert.match(page.textContent, /Activity in roblab/)
  const acts = page.querySelectorAll('[data-activity]')
  assert.deepEqual(acts.map(a => a.dataset.activity), ['claim', 'selection', 'run'])
  assert.ok(acts[0].querySelector('.claim'))
  assert.match(acts[0].textContent, /named/)
  assert.ok(acts[1].querySelector('.fact'))
  assert.equal(acts[2].querySelector('.fact a, a.fact').getAttribute('href'), '#/run/m1')
})

function pipelineEx(anomaliesFor) {
  return {
    stale: [],
    runPage: async () => ({ rows: RUNS.rnaseq, hidden: 0, first: 1, last: 3, total: 3, prev: null, next: null }),
    completionOf: async (cid) => ({ anomalies: anomaliesFor(cid) }),
    runSize: async (cid) => ({ bytes: cid === 'r3' ? 142_000_000 : 21_100_000, blocks: 3, partial: false }),
    pipelineSize: async () => ({ bytes: 150_000_000, blocks: 9, partial: false }),
  }
}

test('pipeline page: shared health shows once above the table, rows are blank, sizes and names show', async () => {
  installDom()
  const page = await pipeline(pipelineEx(() => ANOMALIES), 'nf-core/rnaseq', 0, ctx())
  await settle()
  const line = page.querySelector('[data-shared-health]')
  assert.match(line.textContent, /Declare them in the output \{ \} block/)
  for (const td of page.querySelectorAll('[data-anomalies-for]')) assert.equal(td.textContent, '')
  const rows = page.querySelectorAll('[data-run]')
  assert.deepEqual(rows.map(r => r.dataset.run), ['r3', 'r2', 'r1'])
  assert.deepEqual(rows.map(r => cells(r)[4]), ['142 MB', '21.1 MB', '21.1 MB'])
  assert.match(rows[0].textContent, /distraught_murdock/)
  assert.match(rows[2].textContent, /Failed/)
  assert.doesNotMatch(page.textContent, /bafy/)
})

test('pipeline page: runs that differ keep their own health cell', async () => {
  installDom()
  const page = await pipeline(pipelineEx(cid => (cid === 'r3' ? ANOMALIES : {})), 'nf-core/rnaseq', 0, ctx())
  await settle()
  assert.equal(page.querySelector('[data-shared-health]').textContent, '')
  assert.match(page.querySelector('[data-anomalies-for=r3]').textContent, /outside any workflow output/)
  assert.equal(page.querySelector('[data-anomalies-for=r2]').textContent, '')
})

test('recentActivity: runs, Selections and claims newest first, joined to run names and claim fields', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, [readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'),
    "INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES ('r1', 'p', 'cold_cat', 'succeeded', 0, '2026-10-24T17:00:00.000Z')",
    "INSERT INTO collection(collection_cid, kind, asserted_by) VALUES ('sel1', 'selection', 'anonymous')",
    "INSERT INTO claim VALUES ('k1', 'sel1', 'set', 'name', 'wigshop', '2026-10-24T19:00:00.000Z', 'anonymous')",
    "INSERT INTO claim_current VALUES ('sel1', 'name', 'wigshop', 'k1', 0)",
    "INSERT INTO log_entry VALUES ('r1', 'run', NULL, '2026-10-24T17:00:01.000Z'), ('sel1', 'selection', NULL, '2026-10-24T18:59:00.000Z'), ('k1', 'claim', NULL, '2026-10-24T19:00:01.000Z'), ('r1', 'run', 'other', '2026-10-24T17:00:02.000Z')"])
  const ex = new Explorer({ base: 'http://h/m/lab/', db: await snapshotDb(bytes), blocks: null, now: () => Date.now() })
  const rows = await ex.recentActivity(8)
  assert.deepEqual(rows.map(r => [r.kind, r.cid]), [['claim', 'k1'], ['selection', 'sel1'], ['run', 'r1']])
  assert.equal(rows[2].name, 'cold_cat')
  assert.equal(rows[1].name, 'wigshop')
  assert.deepEqual(rows[0].claim, { verb: 'set', attribute: 'name', value: 'wigshop', subject: 'sel1', assertedBy: 'anonymous', timestamp: '2026-10-24T19:00:00.000Z' })
  assert.equal((await ex.recentActivity(2)).length, 2)
})

test('runCounts: one grouped query, tail merged, runs hidden by a delete Claim left out', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, [readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'),
    "INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES ('a', 'p', 'a', 'succeeded', 0, '2026-10-24T17:00:00.000Z'), ('b', 'p', 'b', 'failed', 1, '2026-10-24T18:00:00.000Z'), ('c', 'p', 'c', 'succeeded', 0, '2026-10-24T19:00:00.000Z'), ('d', 'q', 'd', 'succeeded', 1, '2026-10-24T19:00:00.000Z')",
    "INSERT INTO claim VALUES ('k1', 'c', 'delete', NULL, NULL, '2026-10-24T20:00:00.000Z', 'anonymous')",
    "INSERT INTO claim_current VALUES ('c', NULL, NULL, 'k1', 0)"])
  const ex = new Explorer({ base: 'http://h/m/lab/', db: await snapshotDb(bytes), blocks: null, now: () => Date.now() })
  assert.deepEqual([...await ex.runCounts()], [['p', { runs: 2, ok: 1, failed: 1 }], ['q', { runs: 1, ok: 0, failed: 0 }]])
})

test('home: a health lookup that fails leaves the Runs cell filled', async () => {
  installDom()
  const ex = homeEx()
  ex.completionOf = async () => { throw Object.assign(new Error('gone'), { code: 'block_missing' }) }
  const page = await home(ex, ctx())
  await settle()
  const rna = cells(page.querySelectorAll('tr[data-pipeline]')[1])
  assert.equal(rna[1], '3 · 2 ok · 1 failed')
  assert.equal(rna[5], '')
})

test('#/latest names the run from its blocks, the short CID beside it', async () => {
  installDom()
  const { latest } = await import('../src/views/pipeline.js')
  const ex = { latestSuccessfulRun: async () => 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq',
    runIdentity: async () => ({ run_name: 'distraught_murdock' }),
    runRow: () => { throw new Error('#/latest reads the snapshot once, for the query') } }
  const page = await latest(ex, 'nf-core/rnaseq')
  const p = page.querySelector('[data-latest]')
  assert.equal(p.dataset.latest, 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq')
  assert.match(p.textContent, /distraught_murdock/)
  assert.match(p.textContent, /bafy…26ocxq/)
  const unnamed = await latest({ ...ex, runIdentity: async () => { throw new Error('no blocks') } }, 'nf-core/rnaseq')
  assert.match(unnamed.querySelector('[data-latest]').textContent, /bafy…26ocxq/)
})

test('home: a dedup ratio drops its trailing zeros', async () => {
  installDom()
  const { explorer } = await openRoblab()
  const stats = await explorer.storeStats()
  explorer.storeStats = async () => ({ ...stats, dedup: 3.1 })
  const page = await home(explorer, ctx())
  await settle()
  assert.equal(page.querySelector('[data-store-figures]').textContent, '142 MB stored · 220 MB referenced · 3.1× dedup')
})

test('home: the subline carries the store-wide figures from the snapshot, and nothing more on a lean one', async () => {
  installDom()
  const { explorer } = await openRoblab()
  const page = await home(explorer, ctx())
  await settle()
  const subline = page.querySelector('.page-head').querySelector('.subline')
  assert.match(subline.textContent, /roblab/)
  assert.equal(subline.querySelector('[data-store-figures]').textContent, '142 MB stored · 220 MB referenced · 1.55× dedup')
  const lean = (await openSnapshots({ 'index/v7.sqlite': await roblabLeanSnapshot('over_cap') })).explorer
  const none = await home(lean, ctx())
  await settle()
  assert.equal(none.querySelector('[data-store-figures]'), null)
  assert.doesNotMatch(none.querySelector('.page-head').textContent, /dedup/)
})

test("Home's activity and the Selection rail name a Claim with one verb (claimVerb)", async () => {
  const { claimVerb } = await import('../src/views/common.js')
  const { activityTag } = await import('../src/views/home.js')
  const cases = [
    [{ attribute: 'name', verb: 'set' }, 'named'], [{ attribute: 'name', verb: 'del' }, 'unnamed'],
    [{ attribute: 'pin', verb: 'add' }, 'pinned'], [{ attribute: 'pin', verb: 'del' }, 'unpinned'],
    [{ attribute: 'retain', verb: 'set' }, 'released'], [{ attribute: 'retain', verb: 'del' }, 'restored'],
    [{ attribute: null, verb: 'delete' }, 'deleted'], [{ attribute: null, verb: 'del' }, 'restored'],
  ]
  installDom()
  for (const [c, verb] of cases) {
    assert.equal(claimVerb(c), verb)
    assert.match(activityTag({ kind: 'claim', claim: { ...c, assertedBy: 'ana@x', timestamp: '2026-10-24T00:00:00Z' } }).textContent, new RegExp(verb))
  }
})

test("Home's activity names a P4 Claim in the project page's words, a project linked by its short cid until named", async () => {
  const { activityTag } = await import('../src/views/home.js')
  const { nameProjectLinks } = await import('../src/views/common.js')
  const P = 'bafyreiprojectcidxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
  const ref = { pipeline: 'nf-core/rnaseq', id: 'WT_REP1' }
  const occ = (id) => ({ pipeline: 'nf-core/rnaseq', id, run: { '/': 'bafyrun' } })
  const cases = [
    [{ attribute: 'project', verb: 'add', value: JSON.stringify({ '/': P }) }, 'added to', null],
    [{ attribute: 'project', verb: 'del', value: null }, 'removed from', null],
    [{ attribute: 'sample', verb: 'add', value: JSON.stringify(ref) }, 'added sample', 'WT_REP1'],
    [{ attribute: 'sample', verb: 'del', value: null }, 'removed sample', null],
    [{ attribute: 'annotation', verb: 'add', value: JSON.stringify({ sample: ref, name: 'condition', value: 'treated' }) }, 'condition', 'WT_REP1 = treated'],
    [{ attribute: 'annotation', verb: 'del', value: null }, 'cleared', 'an annotation'],
    [{ attribute: 'same_sample', verb: 'add', value: JSON.stringify({ a: occ('A'), b: occ('B'), same: true }) }, 'merged', 'A and B'],
    [{ attribute: 'same_sample', verb: 'add', value: JSON.stringify({ a: occ('A'), b: occ('B'), same: false }) }, 'split', 'B from A'],
    [{ attribute: 'policy.default_store', verb: 'set', value: 'org-s3' }, 'default store', 'org-s3'],
    [{ attribute: 'policy.default_store', verb: 'del', value: null }, 'cleared', 'the default store'],
    [{ attribute: 'policy.retention', verb: 'set', value: 'pin_selections' }, 'retention', 'pins every Selection'],
    [{ attribute: 'policy.retention', verb: 'set', value: 'none' }, 'retention', 'off'],
    [{ attribute: 'policy.people', verb: 'add', value: 'ben@x' }, 'added person', 'ben@x'],
    [{ attribute: 'policy.people', verb: 'del', value: null }, 'removed person', null],
  ]
  installDom()
  for (const [c, verb, value] of cases) {
    const tag = activityTag({ kind: 'claim', claim: { ...c, assertedBy: 'ana@x', timestamp: '2026-10-24T00:00:00Z' } })
    assert.equal(tag.querySelector('.verb').textContent, verb, JSON.stringify(c))
    if (value !== null) assert.equal(tag.querySelector('.val').textContent, value, JSON.stringify(c))
    assert.doesNotMatch(tag.textContent, /[{}“]|object Object/, JSON.stringify(c))
  }
  const tag = activityTag({ kind: 'claim', claim: { ...cases[0][0], assertedBy: 'ana@x', timestamp: '2026-10-24T00:00:00Z' } })
  const a = tag.querySelector('[data-project-name]')
  assert.equal(a.getAttribute('href'), `#/project/${P}`)
  assert.equal(a.textContent, shortCid(P))
  await nameProjectLinks([a], async (cids) => new Map(cids.map(c => [c, { name: 'RAP1 depletion' }])))
  assert.equal(a.textContent, 'RAP1 depletion')
})

test('a claim tag\'s project link, named after the tag was built, takes the name as its title too, never the short cid', async () => {
  const { activityTag } = await import('../src/views/home.js')
  const { nameProjectLinks } = await import('../src/views/common.js')
  const P = 'bafyreiprojectcidxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
  installDom()
  const tag = activityTag({ kind: 'claim', claim: { attribute: 'project', verb: 'add', value: JSON.stringify({ '/': P }), assertedBy: 'ana@x',
    timestamp: '2026-10-24T00:00:00Z' } })
  const val = tag.querySelector('.val')
  const a = val.querySelector('[data-project-name]')
  assert.equal(val.getAttribute('title'), shortCid(P), 'built before the name is known')
  // The test DOM keeps no parent links; a browser's does.
  a.parentElement = val
  await nameProjectLinks([a], async (cids) => new Map(cids.map(c => [c, { name: 'RAP1 depletion, auxin time course' }])))
  assert.equal(val.getAttribute('title'), 'RAP1 depletion, auxin time course')
  assert.equal(a.getAttribute('title'), 'RAP1 depletion, auxin time course')
  // A later rename names it again.
  await nameProjectLinks([a], async (cids) => new Map(cids.map(c => [c, { name: 'RAP1 depletion v2' }])))
  assert.equal(val.getAttribute('title'), 'RAP1 depletion v2')
  assert.equal(a.textContent, 'RAP1 depletion v2')
})
