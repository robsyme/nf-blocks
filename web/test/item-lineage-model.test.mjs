// Explorer.itemLineage and Explorer.usedBy over a snapshot holding two consumer runs and a tail consumer run.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { loadSqlite, makeDb, snapshotDb } from './helpers.mjs'
import { blockFetch, buildMember, entryName, rawCid } from './fixture.mjs'

const cid = (name) => rawCid(name)
const PEOPLE = cid('people-collection').toString()
const FRIENDS = cid('friends-selection').toString()
const CONSUMER = cid('consumer-completion').toString()
const CONSUMER_2 = cid('consumer-2-completion').toString()
const INTROS = cid('intros-collection').toString()
const CIRCLES = cid('circle-collection').toString()
const INTROS_2 = cid('intros-2-collection').toString()
const OLD_RUN = cid('old-completion').toString()
const PEOPLE_ITEMS = ['Dev', 'Ana', 'Bo', 'Cy'].map(n => cid(`people-${n}`).toString())
const PEOPLE_DEV = PEOPLE_ITEMS[0]
const INTRO_DEV = cid('intro-Dev').toString()
const CIRCLE = cid('circle').toString()
const OLD_ITEM = cid('old-item').toString()
const DAT = cid('Dev_Patel.dat').toString()

/** One run's `run_input` rows, for positions `names`. */
function inputRows(completion, names) {
  return names.map((name, position) =>
    `INSERT INTO run_input(completion_cid, role, name, address, size, sample, position) VALUES ('${completion}', 'task', '${name}', '${DAT}', 10, NULL, ${position})`)
}

const originRows = (completion, items) => items.map((item, position) =>
  `INSERT INTO input_origin(completion_cid, position, item_cid, collection_cid, selection_cid) VALUES ('${completion}', ${position}, '${item}', '${PEOPLE}', '${FRIENDS}')`)

const runRow = (completion, name) =>
  `INSERT INTO run(completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES ('${completion}', 'consumer', '${name}', 'succeeded', 0, '2026-09-01T10:00:00.000Z')`

const statements = [
  readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'),
  runRow(CONSUMER, 'consumer_run'), runRow(CONSUMER_2, 'consumer_run_2'), runRow(OLD_RUN, 'old_run'),
  // The consumers' outputs, and the Selection `friends`, which belongs to no run.
  ...[[INTROS, CONSUMER, 'intros', INTRO_DEV], [CIRCLES, CONSUMER, 'circle', CIRCLE], [INTROS_2, CONSUMER_2, 'intros', INTRO_DEV]].flatMap(([c, run, output, item]) => [
    `INSERT INTO collection(collection_cid, kind, completion_cid, output_name) VALUES ('${c}', 'output', '${run}', '${output}')`,
    `INSERT INTO collection_item(collection_cid, item_cid) VALUES ('${c}', '${item}')`]),
  `INSERT INTO collection(collection_cid, kind, completion_cid, output_name) VALUES ('${FRIENDS}', 'selection', NULL, NULL)`,
  `INSERT INTO collection_item(collection_cid, item_cid) VALUES ('${FRIENDS}', '${PEOPLE_DEV}')`,
  // CONSUMER: intros Dev read people Dev; circle read all four.
  ...inputRows(CONSUMER, ['Dev_Patel.dat', 'Ana.dat', 'Bo.dat', 'Cy.dat']),
  ...originRows(CONSUMER, PEOPLE_ITEMS),
  `INSERT INTO item_input(completion_cid, item_cid, positions) VALUES ('${CONSUMER}', '${INTRO_DEV}', '[0]')`,
  `INSERT INTO item_input(completion_cid, item_cid, positions) VALUES ('${CONSUMER}', '${CIRCLE}', '[0,1,2,3]')`,
  // CONSUMER_2 makes the same intros item from the same people item.
  ...inputRows(CONSUMER_2, ['Dev_Patel.dat']),
  ...originRows(CONSUMER_2, [PEOPLE_DEV]),
  `INSERT INTO item_input(completion_cid, item_cid, positions) VALUES ('${CONSUMER_2}', '${INTRO_DEV}', '[0]')`,
]

/** A tail consumer run whose ItemLineage names INTRO_DEV, and a tail run whose item_lineage is null. */
function tailRuns({ put, at }) {
  const manifest = (name) => put({ kind: 'RunManifest', schema: 1, asserted_by: 'test', pipeline: 'consumer', repository: null, revision: null,
    commit_id: null, run_name: name, nf_run_hash: `hash-${name}`, session_id: 's', resumed: false,
    nextflow_version: '26.04.6', params: {}, config: {}, script: null, started_at: '2026-09-04T00:00:00.000Z' })
  const completion = (name, itemLineage, inputSet) => put({ kind: 'RunCompletion', schema: 3, asserted_by: 'test', run: manifest(name), collections: [],
    input_set: inputSet, status: 'succeeded', exit_status: 0, possibly_incomplete: false, started_at: '2026-09-04T00:00:00.000Z',
    finished_at: '2026-09-04T01:00:00.000Z', anomalies: { unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 }, error: null,
    providers: {}, item_lineage: itemLineage })
  const inputSet = put({ kind: 'InputSet', schema: 2, asserted_by: 'test', inputs: [
    { role: 'task', name: 'Dev_Patel.dat', address: cid('Dev_Patel.dat'), size: 10, sample: null, reason: null,
      from: [{ item: cid('people-Dev'), collection: cid('people-collection'), selection: cid('friends-selection') }] },
    { role: 'task', name: 'Ana.dat', address: cid('Dev_Patel.dat'), size: 10, sample: null, reason: null }] })
  const lineage = put({ kind: 'ItemLineage', schema: 1, asserted_by: 'test', input_set: inputSet,
    items: [{ item: cid('intro-Dev'), inputs: [0], tasks: [] }] })
  const tail = completion('tail_run', lineage, inputSet)
  const none = completion('no_lineage_run', null, null)
  return { tail: tail.toString(), none: none.toString(),
    log: [entryName(at.R3 + 10, 'run', tail.toString()), entryName(at.R3 + 20, 'run', none.toString())] }
}

async function open() {
  const sqlite3 = await loadSqlite()
  const snapshot = makeDb(sqlite3, statements)
  let tail
  const member = await buildMember({ extra: (parts) => { tail = tailRuns(parts); return { log: tail.log, rows: null } } })
  const ex = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => snapshotDb(snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }),
    listFn: async () => ({ names: member.log, readable: true }),
  })
  return { ex, tail, member }
}

test('snapshot: an intros item names its datafile and the upstream people item', async () => {
  const { ex } = await open()
  const rows = await ex.itemLineage(INTRO_DEV, CONSUMER)
  assert.deepEqual(rows.map(r => [r.completion_cid, r.source]), [[CONSUMER, 'snapshot']])
  assert.deepEqual(rows[0].inputs.map(i => [i.position, i.role, i.name, i.address, i.size, i.sample,
    i.origins.map(o => [o.item_cid, o.collection_cid, o.selection_cid])]),
  [[0, 'task', 'Dev_Patel.dat', DAT, 10, null, [[PEOPLE_DEV, PEOPLE, FRIENDS]]]])
})

test('snapshot: circle names all four inputs', async () => {
  const { ex } = await open()
  const [run] = await ex.itemLineage(CIRCLE, CONSUMER)
  assert.deepEqual(run.inputs.map(i => i.position), [0, 1, 2, 3])
  assert.deepEqual(run.inputs.map(i => i.origins[0].item_cid), PEOPLE_ITEMS)
})

test('without a completion every run that recorded lineage for the item answers, sorted', async () => {
  const { ex, tail } = await open()
  assert.deepEqual((await ex.itemLineage(INTRO_DEV)).map(r => r.completion_cid), [CONSUMER, CONSUMER_2, tail.tail].sort())
})

test('with a completion only that run answers', async () => {
  const { ex } = await open()
  assert.deepEqual((await ex.itemLineage(INTRO_DEV, CONSUMER_2)).map(r => r.completion_cid), [CONSUMER_2])
})

test('a tail run reads its ItemLineage and InputSet blocks', async () => {
  const { ex, tail } = await open()
  const [run] = await ex.itemLineage(INTRO_DEV, tail.tail)
  assert.equal(run.source, 'tail')
  assert.deepEqual(run.inputs.map(i => [i.position, i.name, i.origins]), [[0, 'Dev_Patel.dat', [{ item_cid: PEOPLE_DEV, collection_cid: PEOPLE, selection_cid: FRIENDS }]]])
  assert.equal(run.inputs[0].address, DAT)
  assert.equal(run.inputs[0].origins[0].item_cid, PEOPLE_DEV)
})

test('a run without lineage, or with item_lineage null, answers []', async () => {
  const { ex, tail } = await open()
  assert.deepEqual(await ex.itemLineage(OLD_ITEM, OLD_RUN), [])
  assert.deepEqual(await ex.itemLineage(INTRO_DEV, OLD_RUN), [])
  assert.deepEqual(await ex.itemLineage(INTRO_DEV, tail.none), [])
  assert.deepEqual(await ex.itemLineage(OLD_ITEM), [])
})

test('a tail run whose lineage block is unreadable is left out', async () => {
  const { ex, tail, member } = await open()
  const bad = new Map(member.blocks)
  const lineage = [...bad].find(([, bytes]) => Buffer.from(bytes).includes(Buffer.from('ItemLineage')))[0]
  bad.set(lineage, new Uint8Array([0xa0]))
  const broken = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(makeDb(await loadSqlite(), statements)),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(bad) }), listFn: async () => ({ names: member.log, readable: true }) })
  assert.deepEqual(await broken.itemLineage(INTRO_DEV, tail.tail), [])
  assert.deepEqual((await broken.itemLineage(INTRO_DEV)).map(r => r.completion_cid), [CONSUMER, CONSUMER_2].sort())
})

test('usedBy lists each derived item with its collection, the via and the Selection', async () => {
  const { ex } = await open()
  const rows = await ex.usedBy(PEOPLE_DEV)
  assert.deepEqual(rows.map(r => [r.item_cid, r.via_cid, r.selection_cid]).sort(),
    [[CIRCLE, PEOPLE, FRIENDS], [INTRO_DEV, PEOPLE, FRIENDS], [INTRO_DEV, PEOPLE, FRIENDS]].sort())
})

test('usedBy keeps one row per consumer run and the collection holding the derived item', async () => {
  const { ex } = await open()
  const rows = await ex.usedBy(PEOPLE_DEV)
  assert.deepEqual(rows.map(r => [r.completion_cid, r.item_cid, r.collection_cid]), [
    [CONSUMER, CIRCLE, CIRCLES], [CONSUMER, INTRO_DEV, INTROS], [CONSUMER_2, INTRO_DEV, INTROS_2]].sort((a, b) =>
    a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[2] < b[2] ? -1 : a[2] > b[2] ? 1 : 0))
  assert.deepEqual(Object.keys(rows[0]).sort(), ['collection_cid', 'completion_cid', 'item_cid', 'selection_cid', 'via_cid'])
  assert.deepEqual(await ex.usedBy(OLD_ITEM), [])
})
