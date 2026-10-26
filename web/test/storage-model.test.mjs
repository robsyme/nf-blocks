// The snapshot's storage figures through the Explorer (P2 task 5): the
// store, per run, per person and per kind, sizes read from the index with no
// block walk, and the lean snapshot that keeps P0's walks.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { probeSnapshot } from '../src/db.js'
import { samePrincipal } from '../src/identity.js'
import { snapshotModeText } from '../src/words.js'
import { sizeText } from '../src/format.js'
import { storedTitle } from '../src/views/home.js'
import { run } from '../src/views/run.js'
import { item } from '../src/views/item.js'
import { collection } from '../src/views/collection.js'
import { Tray } from '../src/tray.js'
import { resetFolds } from '../src/folds.js'
import { frame, installDom } from './dom.mjs'
import { snapshotDb } from './helpers.mjs'
import { buildMember, entryName } from './fixture.mjs'
import { CID } from 'multiformats/cid'
import { ROBLAB, openRoblab, openSnapshots, roblabLeanSnapshot, roblabSnapshot } from './storage-fixture.mjs'

const R = ROBLAB.runs

test('storeStats: the store row, its dedup and the sweep grace', async () => {
  const { explorer } = await openRoblab()
  const s = await explorer.storeStats()
  assert.deepEqual({ ...s, dedup: undefined }, { stored: 142_134_100, storedBlocks: 1285, referenced: 219_874_873, dedup: undefined,
    pinned: 0, held: 142_134_100, releasing: 0, shared: 62_751_397, partial: false, graceMs: 1_209_600_000 })
  assert.equal(s.dedup.toFixed(2), '1.55')
  const { explorer: noGrace } = await openRoblab({ grace: null })
  assert.equal((await noGrace.storeStats()).graceMs, null)
})

test('storageRuns: every run root with its figures, newest first', async () => {
  const { explorer, fetchFn } = await openRoblab()
  const runs = await explorer.storageRuns()
  assert.deepEqual(runs.map(r => [r.name, r.exclusive]), [['sleepy_heyrovsky', 0], ['distraught_murdock', 63_743_945],
    ['sad_yonath', 15_638_758], ['sharp_magritte', 0]])
  const dm = runs[1]
  assert.deepEqual({ ...dm, id: undefined }, { id: undefined, cid: R.distraught_murdock.cid, name: 'distraught_murdock', pipeline: 'nf-core/rnaseq',
    status: 'succeeded', finishedAt: R.distraught_murdock.finished, state: 'held', since: R.distraught_murdock.finished,
    reachable: 78_392_535, blocks: 894, exclusive: 63_743_945, pinned: 0, partial: false })
  assert.deepEqual(fetchFn.asked, [])
})

test('runSize reads the run root, with no block fetch', async () => {
  const { explorer, fetchFn, queries } = await openRoblab()
  assert.deepEqual(await explorer.runSize(R.distraught_murdock.cid), { bytes: 78_392_535, blocks: 894, partial: false })
  assert.deepEqual(await explorer.runSize(R.sleepy_heyrovsky.cid), { bytes: 0, blocks: 0, partial: false })
  assert.deepEqual(fetchFn.asked, [])
  assert.equal(queries.length, 2)
})

test('home asks every pipeline its size with one snapshot query between them', async () => {
  const { explorer, fetchFn, queries } = await openRoblab()
  const [main, rnaseq] = await Promise.all([explorer.pipelineSize('main.nf'), explorer.pipelineSize('nf-core/rnaseq')])
  assert.deepEqual([rnaseq.bytes, rnaseq.blocks, rnaseq.partial, rnaseq.runs, rnaseq.more], [142_134_100, 1285, false, 3, false])
  assert.deepEqual([main.bytes, main.runs], [0, 1])
  assert.deepEqual((await explorer.pipelineSize('gone.nf')), { bytes: 0, blocks: 0, partial: false, upTo: false, runs: 0, more: false, releasing: 0, source: 'snapshot', newer: 0 })
  assert.equal(queries.length, 1)
  assert.deepEqual(fetchFn.asked, [])
})

test('a pipeline size from the index says which runs it counts (R5)', () => {
  assert.match(storedTitle({ bytes: 1, source: 'snapshot' }), /held runs/)
  assert.doesNotMatch(storedTitle({ bytes: 1, source: 'snapshot' }), /hidden by a delete Claim too/)
  assert.match(storedTitle({ bytes: 1, more: true, runs: 10 }), /newest 10 runs/)
  assert.match(storedTitle({ bytes: 1, source: 'snapshot' }, { store: 'org-s3' }), /^Stored in org-s3\. Counts the runs/)
  assert.doesNotMatch(storedTitle({ bytes: 1, source: 'snapshot' }), /Stored in/, 'one store: the page already says which')
})

test('selectionSize and collectionSize read the index; a collection\'s bytes do not wait on its kinds', async () => {
  const { explorer, fetchFn, queries } = await openRoblab()
  assert.deepEqual(await explorer.selectionSize(ROBLAB.selection.cid), { bytes: 1_135_852, blocks: 9, files: 9, partial: false })
  assert.deepEqual(await explorer.collectionSize(ROBLAB.collection.cid), { bytes: 105, blocks: 2, items: 2, kinds: null, partial: false })
  assert.deepEqual(fetchFn.asked, [])
  assert.deepEqual(queries.filter(q => /block_ref|FROM block\b/.test(q)), [], 'no block_ref or block read')
  const { explorer: partial } = await openRoblab({ extra: ['UPDATE store_stats SET partial = 1'] })
  assert.equal((await partial.collectionSize(ROBLAB.collection.cid)).partial, true, 'the store\'s partial')
})

/** buildMember with R1's storage-figure rows: its run root, its collection's and item A's sizes (A's from the index is 777, the walk says 10). */
async function memberWithIndex() {
  const now = Date.now()
  const member = await buildMember({ now, extra: ({ runs, item }) => ({ log: [], rows: (db) => {
    db.exec({ sql: "INSERT INTO root VALUES (1, ?, 'run', 'test', 'held', ?)", bind: [runs.R1.completion, runs.R1.finished] })
    db.exec('INSERT INTO root_stats VALUES (1, 20, 2, 20, 2, 0, 0)')
    db.exec('INSERT INTO store_stats VALUES (20, 2, 20, 0, 20, 0, 0, 0)')
    db.exec({ sql: 'INSERT INTO collection_size VALUES (?, 20, 2)', bind: [runs.R1.collection] })
    db.exec({ sql: 'INSERT INTO item_size VALUES (?, 777, 1)', bind: [item.A.toString()] })
  } }) })
  const opened = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  opened.fetchFn.asked.length = 0
  return { member, ...opened }
}

const viewCtx = () => ({ tray: new Tray(null), trayChanged: () => {}, rerender: () => {}, progress: () => {}, use: () => {}, mark: () => {},
  sources: new Map(), store: { name: 'lab', key: 'a' }, write: { available: false, reason: 'read only', hrefFor: (h) => h } })
const settle = async () => { for (let i = 0; i < 6; i++) { await frame(); await new Promise(r => setTimeout(r, 2)) } }

test('collectionSize from the index resolves with no block fetch; collectionKinds walks for the chips', async () => {
  const { member, explorer, fetchFn, queries } = await memberWithIndex()
  const size = await explorer.collectionSize(member.runs.R1.collection)
  assert.deepEqual(size, { bytes: 20, blocks: 2, items: 2, kinds: null, partial: false })
  assert.deepEqual(fetchFn.asked, [])
  assert.deepEqual(queries.filter(q => /block_ref/.test(q)), [])
  assert.deepEqual(await explorer.collectionKinds(member.runs.R1.collection), [['bam', 2, 20]])
  assert.ok(fetchFn.asked.includes(member.item.A), 'the chips walk the items')
})

test('the run page with storage figures shows the index size and still fills its kind chips', async () => {
  installDom()
  resetFolds()
  const { member, explorer } = await memberWithIndex()
  const page = await run(explorer, member.runs.R1.completion, viewCtx())
  await settle()
  const row = page.querySelectorAll('tr').find(r => r.dataset.collection === member.runs.R1.collection)
  assert.ok(row, 'the aligned row')
  assert.match(row.textContent, /20 B/)
  assert.deepEqual(row.querySelector('[data-files]').querySelectorAll('.kind').map(k => k.textContent), ['bam×2'])
})

test('the collection page with storage figures shows the index size, then its kind chips', async () => {
  installDom()
  resetFolds()
  const { member, explorer } = await memberWithIndex()
  const page = await collection(explorer, member.runs.R1.collection, 0, viewCtx())
  await settle()
  const line = page.querySelector('[data-size-line]')
  assert.match(line.textContent, /^2 items · 20 B/)
  assert.deepEqual(line.querySelectorAll('.kind').map(k => k.textContent), ['bam×2'])
})

test('itemStoredSize reads item_size; null with no row', async () => {
  const { explorer, fetchFn } = await openRoblab()
  assert.deepEqual(await explorer.itemStoredSize('item-1'), { bytes: 100, blocks: 1, partial: false })
  assert.equal(await explorer.itemStoredSize('item-2'), null)
  assert.deepEqual(fetchFn.asked, [])
})

test('the item page with storage figures takes its size from the index, with no walk for it', async () => {
  installDom()
  resetFolds()
  const { member, explorer } = await memberWithIndex()
  let walked = 0
  const walk = explorer.itemSize.bind(explorer)
  explorer.itemSize = (cid) => { walked++; return walk(cid) }
  const page = await item(explorer, member.runs.R1.collection, member.item.A, viewCtx())
  await settle()
  assert.equal(walked, 0)
  assert.match(page.textContent, /1 files · 777 B/)
})

test('pinRoots: every pin root, newest first, with its subject\'s kind, name and collection', async () => {
  const { explorer, fetchFn } = await openRoblab({ extra: [
    `INSERT INTO root VALUES (6, '${R.sad_yonath.cid}', 'pin', 'Ana@X', 'pinned', '2026-10-25T09:00:00.000Z'), (7, 'item-1', 'pin', 'ben', 'pinned', '2026-10-25T10:00:00.000Z'),
      (8, '${ROBLAB.selection.cid}', 'pin', 'ben', 'pinned', '2026-10-25T08:00:00.000Z'), (9, 'coll-dm-aligned', 'pin', 'ben', 'pinned', '2026-10-25T07:00:00.000Z'),
      (10, 'bafk-file', 'pin', 'ben', 'pinned', '2026-10-25T06:00:00.000Z')`,
    'INSERT INTO root_stats VALUES (6, 78390155, 894, 0, 0, 78390155, 0), (7, 100, 1, 100, 1, 100, 0), (8, 1135852, 9, 0, 0, 1135852, 0), (9, 105, 2, 0, 0, 105, 0), (10, 7, 1, 7, 1, 7, 0)'] })
  const pins = await explorer.pinRoots()
  assert.deepEqual(pins.slice(0, 2), [
    { id: 7, cid: 'item-1', subjectKind: 'item', name: null, collection: 'coll-dm-aligned', assertedBy: 'ben', since: '2026-10-25T10:00:00.000Z', reachable: 100, exclusive: 100 },
    { id: 6, cid: R.sad_yonath.cid, subjectKind: 'run', name: 'sad_yonath', collection: null, assertedBy: 'Ana@X', since: '2026-10-25T09:00:00.000Z', reachable: 78_390_155, exclusive: 0 }])
  assert.deepEqual(pins.slice(2).map(p => [p.id, p.subjectKind, p.name, p.collection]), [
    [8, 'selection', 'wigshop', null], [9, 'collection', null, null], [10, 'block', null, null]])
  assert.deepEqual(fetchFn.asked, [], 'no block read')
  assert.deepEqual(await (await openRoblab()).explorer.pinRoots(), [])
})

test('asserterStats, kindStats and pinnedBy', async () => {
  const { explorer } = await openRoblab({ assertedBy: ' Ada@Example.org ', extra: [
    "INSERT INTO asserter_stats VALUES ('ana@x', 5, 5, 5, 0)",
    "INSERT INTO pinned_overlap VALUES ('item-1', 'ben', 2000000), ('item-1', 'ana@x', 5000000), ('item-2', 'ana@x', 1)"] })
  assert.deepEqual(await explorer.asserterStats(), [
    { assertedBy: 'ada@example.org', reachable: 142_134_100, exclusive: 142_134_100, pinned: 0, releasing: 0 },
    { assertedBy: 'ana@x', reachable: 5, exclusive: 5, pinned: 5, releasing: 0 }])
  const kinds = await explorer.kindStats()
  assert.deepEqual(kinds.slice(0, 2), [{ kind: 'bam', files: 10, bytes: 38_868_638 }, { kind: 'html', files: 60, bytes: 32_697_303 }])
  assert.equal(kinds.length, ROBLAB.kinds.length)
  assert.deepEqual(await explorer.pinnedBy('item-1'), [{ assertedBy: 'ana@x', bytes: 5_000_000 }, { assertedBy: 'ben', bytes: 2_000_000 }])
  assert.deepEqual(await explorer.pinnedBy('nothing'), [])
})

test('rootStorage: a hand-off root is named from its template and links to its Selection', async () => {
  const { explorer } = await openRoblab({ extra: [
    "INSERT INTO root VALUES (7, 'handoff-1', 'handoff', 'ana@x', 'held', '2026-10-25T10:00:00.000Z')",
    'INSERT INTO root_stats VALUES (7, 1135852, 9, 0, 0, 0, 0)',
    "INSERT INTO handoff VALUES ('handoff-1', 'nf-core/differentialabundance', 'sel-1', 'proj-1', 'ana@x', '2026-10-25T10:00:00.000Z')",
    'INSERT INTO root_shared VALUES (3, 7, 1135852)'] })
  const dm = await explorer.rootStorage(R.distraught_murdock.cid)
  const h = dm.shared.find(s => s.kind === 'handoff')
  assert.equal(h.name, 'hand-off to nf-core/differentialabundance')
  assert.equal(h.href, '#/selection/sel-1')
  assert.equal(dm.shared.find(s => s.kind === 'run').href, `#/run/${dm.shared.find(s => s.kind === 'run').cid}`)
})

test('rootStorage: a run\'s roots, and the runs and Selections it shares with, named', async () => {
  const { explorer, fetchFn } = await openRoblab({ extra: [
    `INSERT INTO root VALUES (6, '${R.sad_yonath.cid}', 'pin', 'Ana@X', 'pinned', '2026-10-25T09:00:00.000Z')`,
    'INSERT INTO root_stats VALUES (6, 78390155, 894, 0, 0, 78390155, 0)',
    'INSERT INTO root_shared VALUES (3, 6, 14648590)'] })
  const dm = await explorer.rootStorage(R.distraught_murdock.cid)
  assert.deepEqual(dm.roots, [{ id: 3, kind: 'run', assertedBy: 'anonymous', state: 'held', since: R.distraught_murdock.finished,
    reachable: 78_392_535, blocks: 894, exclusive: 63_743_945, pinned: 0, partial: false }])
  assert.deepEqual(dm.shared.map(s => [s.kind, s.name, s.bytes]), [
    ['run', 'sad_yonath', 14_648_590], ['pin', 'Ana@X', 14_648_590], ['run', 'sharp_magritte', 5_297_681], ['selection', 'wigshop', 1_135_852]])
  assert.deepEqual(dm.shared[0], { cid: R.sad_yonath.cid, kind: 'run', name: 'sad_yonath', href: `#/run/${R.sad_yonath.cid}`, assertedBy: 'anonymous', bytes: 14_648_590 })
  const sy = await explorer.rootStorage(R.sad_yonath.cid)
  assert.deepEqual(sy.roots.map(r => [r.kind, r.assertedBy, r.state]), [['run', 'anonymous', 'held'], ['pin', 'Ana@X', 'pinned']])
  assert.equal(sy.shared.length, 3, 'from the run root, not the pin')
  assert.equal(await explorer.rootStorage('no-such-root'), null)
  assert.deepEqual(fetchFn.asked, [])
})

test('the page opens index/v6.sqlite and no other; the footer names v6', async () => {
  const asked = []
  const fetchFn = async (url) => { asked.push(new URL(url).pathname); return new Response('no', { status: 404 }) }
  const { opened } = await openRoblab()
  assert.deepEqual(opened, ['http://h/m/lab/index/v6.sqlite'])
  // The 404 is db.js's no_snapshot, the code Explorer.open looks for older snapshots on.
  await assert.rejects(probeSnapshot('http://h/m/lab/index/v6.sqlite', { fetchFn }), (e) => e.code === 'no_snapshot')
  assert.deepEqual(asked, ['/m/lab/index/v6.sqlite'])
  const app = readFileSync(new URL('../src/app.js', import.meta.url), 'utf8')
  assert.match(app, /snapshotModeText\(mode\)/)
  assert.equal(snapshotModeText('range'), 'Index v6 · read by range')
})

test('a store whose only snapshot is v5, v4 or v3 has no current snapshot, and the error says to rewrite it', async () => {
  const old = await roblabSnapshot()
  for (const path of ['index/v5.sqlite', 'index/v4.sqlite', 'index/v3.sqlite']) {
    let failure
    const probed = []
    const opened = []
    await assert.rejects(openSnapshots({ [path]: old }, { probed, opened }).catch((e) => { failure = e; throw e }), (e) => e.code === 'no_snapshot' && e.older === path)
    assert.equal(failure.message, `This store's snapshot (${path}) is older than this explorer, which reads index/v6.sqlite. Run nextflow plugin nf-blocks:snapshot to rewrite it.`)
    assert.deepEqual(opened, ['http://h/m/lab/index/v6.sqlite'], 'no database is opened for the probe')
    assert.ok(new Set(probed).size === probed.length && probed.length <= 3, 'at most one request per old path')
  }
})

test('no snapshot at any path is the v6 path\'s no_snapshot; any other failure does not fall back', async () => {
  const probed = []
  const probeOpened = []
  await assert.rejects(openSnapshots({}, { probed, opened: probeOpened }), (e) => e.code === 'no_snapshot' && /v6\.sqlite/.test(e.message) && !e.older)
  assert.deepEqual(probed, ['http://h/m/lab/index/v5.sqlite', 'http://h/m/lab/index/v4.sqlite', 'http://h/m/lab/index/v3.sqlite'], 'one request per old path')
  assert.deepEqual(probeOpened, ['http://h/m/lab/index/v6.sqlite'], 'no database is opened for the probe')
  const { Explorer } = await import('../src/model.js')
  const opened = []
  await assert.rejects(Explorer.open({ base: 'http://h/m/lab/', blocks: null, listFn: async () => ({ names: [], readable: true }),
    openDb: async (url) => { opened.push(url); throw Object.assign(new Error('CORS'), { code: 'cors_headers' }) } }), (e) => e.code === 'cors_headers')
  assert.equal(opened.length, 1)
})

test('a run newer than the snapshot walks its blocks, and so does its pipeline; a snapshot run beside it reads the index (Review Focus 4)', async () => {
  const now = Date.now()
  const member = await buildMember({ now, extra: ({ runs }) => ({ log: [], rows: (db) => {
    db.exec({ sql: "INSERT INTO root VALUES (1, ?, 'run', 'test', 'held', ?)", bind: [runs.R1.completion, runs.R1.finished] })
    db.exec('INSERT INTO root_stats VALUES (1, 20, 2, 20, 2, 0, 0)')
    db.exec("INSERT INTO pipeline_stats VALUES ('demo', 1, 20, 2, 0), ('other', 1, 5, 1, 0)")
    db.exec('INSERT INTO store_stats VALUES (25, 3, 25, 0, 25, 0, 0, 0)')
  } }) })
  const { explorer, fetchFn } = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  fetchFn.asked.length = 0
  assert.deepEqual(await explorer.runSize(member.runs.R1.completion), { bytes: 20, blocks: 2, partial: false })
  assert.deepEqual(fetchFn.asked, [], 'R1 is in the snapshot')
  assert.deepEqual(await explorer.runSize(member.runs.R2.completion), { bytes: 20, blocks: 2, partial: false })
  assert.ok(fetchFn.asked.includes(member.item.C), 'R2 is a tail run: walked')
  // pipeline_stats' 20 B for R1, plus R2 and R3 walked and unioned with each other only (B and C, 20 B): R2 shares B with
  // R1, which the walk cannot know, so 40 B is an upper bound ("up to"), not a floor.
  const demo = await explorer.pipelineSize('demo')
  assert.deepEqual([demo.bytes, demo.blocks, demo.partial, demo.upTo, demo.source, demo.newer], [40, 4, false, true, 'snapshot', 2])
  assert.equal(sizeText(demo), 'up to 40 B')
  assert.equal(fetchFn.asked.includes(member.item.A), false, 'R1, a snapshot run, is not walked')
  assert.equal((await explorer.pipelineSize('other')).bytes, 5, 'a pipeline with no tail run reads the index')
  assert.match(storedTitle(demo), /^Up to 40 B; 2 newer runs may share files with older ones\./)
  assert.match(storedTitle(demo), /held runs/)
  const other = await explorer.pipelineSize('other')
  assert.equal(other.upTo, false)
  assert.doesNotMatch(storedTitle(other), /newer|Up to/)
})

test('a refresh that finds a new deletion Claim sizes the pipeline again, without the run it hides', async () => {
  const now = Date.now()
  let entry = null
  const member = await buildMember({ now, extra: ({ put, runs, at }) => {
    const del = put({ kind: 'Claim', schema: 1, asserted_by: 'test', subject: CID.parse(runs.R2.completion), verb: 'delete', attribute: null,
      value: null, supersedes: [], timestamp: '2026-09-05T10:00:00.000Z' })
    entry = entryName(at.R3 + 2, 'claim', del)
    return { log: [entry], rows: (db) => db.exec("INSERT INTO pipeline_stats VALUES ('demo', 1, 20, 2, 0)") }
  } })
  const { explorer } = await openSnapshots({ 'index/v6.sqlite': member.snapshot },
    { blocks: member.blocks, log: member.log.filter(n => n !== entry), now })
  assert.equal((await explorer.pipelineSize('demo')).bytes, 40, '20 B indexed, plus R2 and R3 (B and C)')
  await explorer.refreshTail(async () => ({ names: member.log, readable: true }))
  assert.equal((await explorer.pipelineSize('demo')).bytes, 30, 'R2 is hidden now: plus R3 alone (C)')
})

test('a tail run hidden by a delete Claim adds nothing to its pipeline\'s Stored (final fix wave item 3)', async () => {
  const now = Date.now()
  const member = await buildMember({ now, extra: () => ({ log: [], rows: (db) => {
    db.exec("INSERT INTO pipeline_stats VALUES ('demo', 1, 20, 2, 0)")
    db.exec('INSERT INTO store_stats VALUES (20, 2, 20, 0, 20, 0, 0, 0)')
  } }) })
  const { explorer } = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  explorer.tailClaims = [{ cid: 'bafyreidelr2', entry: null, error: null, value: { subject: member.runs.R2.completion, verb: 'delete', attribute: null,
    value: null, supersedes: [], asserted_by: 'test', timestamp: 't' } }]
  const demo = await explorer.pipelineSize('demo')
  // 20 B from the snapshot, plus R3 alone (C, 10 B).
  assert.deepEqual([demo.bytes, demo.blocks, demo.partial, demo.upTo, demo.newer], [30, 3, false, true, 1])
  assert.match(storedTitle(demo), /^Up to 30 B; 1 newer run may share files with older ones\./)
  // The store's own missing sizes make it partial as well; "up to" wins.
  assert.equal(sizeText({ ...demo, partial: true }), 'up to 30 B')
})

test('samePrincipal compares trimmed, lower-cased principals; a missing one matches nothing', () => {
  assert.equal(samePrincipal(' Ada@Example.org ', 'ada@example.org'), true)
  assert.equal(samePrincipal('ada@example.org', 'ana@example.org'), false)
  assert.equal(samePrincipal(null, 'anonymous'), false)
  assert.equal(samePrincipal(' ', ''), false)
  assert.equal(samePrincipal(null, null), false)
})

test('the roblab fixture opens directly too (helpers for later tasks)', async () => {
  const db = await snapshotDb(await roblabSnapshot())
  assert.deepEqual(await db.query('SELECT count(*) AS n FROM root'), [{ n: 5 }])
})

// Fix round 2 (F3): a lean snapshot (meta storage_omitted, F2) has every
// table and no storage rows. The page has no figures there, and says why.
test('a lean snapshot: storeStats is null, storageOmitted gives the reason, and the sizes walk as P0 walks them', async () => {
  for (const reason of ['over_cap', 'out_of_memory']) {
    const { explorer, fetchFn } = await openSnapshots({ 'index/v6.sqlite': await roblabLeanSnapshot(reason, { cap: 67_108_864 }) })
    assert.equal(explorer.storageOmitted, reason)
    assert.equal(explorer.snapshotCapBytes, 67_108_864)
    assert.equal(explorer.storage, false, 'no storage figures to read')
    assert.equal(await explorer.storeStats(), null)
    assert.equal(await explorer.rootStorage(R.distraught_murdock.cid), null)
    assert.equal(await explorer.itemStoredSize('item-1'), null)
    for (const empty of [explorer.asserterStats(), explorer.storageRuns(), explorer.pinnedBy('x'), explorer.kindStats(), explorer.pinRoots()])
      assert.deepEqual(await empty, [])
    assert.deepEqual(fetchFn.asked, [])
  }
  const { explorer: noCap } = await openSnapshots({ 'index/v6.sqlite': await roblabLeanSnapshot('over_cap') })
  assert.equal(noCap.snapshotCapBytes, null)
  const { explorer: full } = await openRoblab()
  assert.equal(full.storageOmitted, null)
  assert.equal(full.storage, true)
  // A lean member's runs, collections and pipelines are walked, as P0 walks them, never read as 0 B from empty tables.
  const now = Date.now()
  const member = await buildMember({ now, lean: true })
  const walked = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  walked.fetchFn.asked.length = 0
  assert.deepEqual(await walked.explorer.runSize(member.runs.R1.completion), { bytes: 20, blocks: 2, partial: false })
  assert.ok(walked.fetchFn.asked.includes(member.item.A), 'walked to the item')
  const size = await walked.explorer.collectionSize(member.runs.R1.collection)
  assert.deepEqual([size.bytes, size.items, size.kinds], [20, 2, [['bam', 2, 20]]])
  const demo = await walked.explorer.pipelineSize('demo')
  assert.notEqual(demo.source, 'snapshot')
  assert.deepEqual([demo.bytes, demo.blocks, demo.runs], [30, 3, 3], 'A, B and C over R1, R2 and R3')
})

test('storageStale: a run or Selection logged since the snapshot makes its figures stale too; storageChanges says which', async () => {
  const { explorer } = await openRoblab()
  assert.equal(explorer.storageStale(), false)
  assert.deepEqual(explorer.storageChanges(), { claims: false, logged: false })
  explorer.tailSelections = [{ cid: 'bafyreinewselection', entry: null, error: null, value: { members: [] } }]
  assert.equal(explorer.storageStale(), true, 'a new Selection')
  assert.deepEqual(explorer.storageChanges(), { claims: false, logged: true })
  explorer.tailSelections = []
  explorer.tailClaims = [{ cid: 'bafyreipin', entry: null, error: null, value: { subject: 'x', verb: 'add', attribute: 'pin', value: null,
    supersedes: [], asserted_by: 'ana@x', timestamp: 't' } }]
  assert.deepEqual(explorer.storageChanges(), { claims: true, logged: false })
  // buildMember logs R2 and R3 after the snapshot.
  const now = Date.now()
  const member = await buildMember({ now })
  const { explorer: tail } = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  assert.ok(tail.staleCount > 0)
  assert.equal(tail.storageStale(), true, 'a new run')
  assert.deepEqual(tail.storageChanges(), { claims: false, logged: true })
})

// Fix round 3 (G2, item 4): #/storage draws Durability from the Claim state for the runs a tail Claim names.
test('tailClaimStates: the current Claim state of the subjects a tail Claim names, and nothing for the rest', async () => {
  const sad = ROBLAB.runs.sad_yonath.cid
  const dm = ROBLAB.runs.distraught_murdock.cid
  const { explorer, queries } = await openRoblab()
  assert.deepEqual([...(await explorer.tailClaimStates([sad, dm])).keys()], [], 'no tail Claims, no states')
  assert.equal(queries.length, 0, 'and no query')
  explorer.tailClaims = [{ cid: 'bafyreipin', entry: null, error: null, value: { subject: sad, verb: 'add', attribute: 'pin', value: 'keep',
    supersedes: [], asserted_by: 'ana@x', timestamp: '2026-10-25T09:00:00Z' } }]
  const states = await explorer.tailClaimStates([sad, dm, sad])
  assert.deepEqual([...states.keys()], [sad])
  assert.equal(states.get(sad).pinned, true)
})
