// The P4 model (plan 1.2 to 1.5): projects, project_stats, memberships, run_input and handoffs from the
// snapshot and the Store Log tail, and a snapshot without the project tables.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import { Explorer } from '../src/model.js'
import { BlockError, BlockFetcher } from '../src/blocks.js'
import { snapshotDb } from './helpers.mjs'
import { block, blockFetch, buildMember, entryName, rawCid } from './fixture.mjs'
import { SIZE_CONCURRENCY } from '../src/config.js'

const NOW = Date.parse('2026-10-25T12:00:00.000Z')
const AT = '2026-10-25T10:00:00.000Z'
const claim = (subject, verb, attribute, value, supersedes = [], { by = 'ana@x', timestamp = AT } = {}) =>
  ({ kind: 'Claim', schema: 1, asserted_by: by, subject, verb, attribute, value, supersedes, timestamp })
const project = (name, createdAt, by = 'ana@x') =>
  ({ kind: 'Project', schema: 1, asserted_by: by, name, description: `about ${name}`, created_at: createdAt })
const linkText = (cid) => `{"/":"${cid}"}`

/**
 * P1 (snapshot, renamed in the snapshot and again in the tail) holds R1 and
 * item B in the snapshot; item A's snapshot membership is removed in the
 * tail, and the tail adds R2 (a tail run) and a tail Selection S. P2 is
 * tail-only. P3 is in the snapshot and deleted in the tail. Px is a Project
 * block no log entry announces. R4 is a tail run with an InputSet.
 */
async function open({ log = null, inputSetStored = true } = {}) {
  const ids = {}
  const member = await buildMember({
    now: NOW,
    extra: ({ put, runs, item }) => {
      ids.P1 = put(project('Liver study', '2026-10-20T10:00:00.000Z'))
      ids.P2 = put(project('Kidney', '2026-10-24T10:00:00.000Z', 'ben@x'))
      ids.P3 = put(project('Old', '2026-10-01T10:00:00.000Z'))
      ids.Px = put(project('Elsewhere', '2026-10-02T10:00:00.000Z'))
      const R1 = CID.parse(runs.R1.completion)
      const R2 = CID.parse(runs.R2.completion)
      ids.MR1 = put(claim(R1, 'add', 'project', ids.P1))
      ids.MB = put(claim(item.B, 'add', 'project', ids.P1, [], { by: 'ben@x' }))
      ids.MA = put(claim(item.A, 'add', 'project', ids.P1))
      ids.MP2 = put(claim(R1, 'add', 'project', ids.P2))
      ids.N1 = put(claim(ids.P1, 'set', 'name', 'Liver'))
      // The tail.
      ids.DA = put(claim(item.A, 'del', 'project', null, [ids.MA]))
      ids.MR2 = put(claim(R2, 'add', 'project', ids.P1, [], { timestamp: '2026-10-25T11:00:00.000Z' }))
      ids.S = put({ kind: 'Selection', schema: 1, asserted_by: 'ana@x', derived_from: [], members: [{ item: { address: item.C, via: [] } }] })
      ids.MS = put(claim(ids.S, 'add', 'project', ids.P1))
      ids.N2 = put(claim(ids.P1, 'set', 'name', 'Liver, batch 2', [ids.N1]))
      ids.D3 = put(claim(ids.P3, 'delete', null, null))
      ids.SA = put(claim(ids.P1, 'add', 'sample', { pipeline: 'demo', id: 'A' }))
      // R4: a tail run with an InputSet.
      const reads = rawCid('reads-A')
      ids.reads = reads
      const inputSet = (inputSetStored ? put : (v) => block(v).cid)({ kind: 'InputSet', schema: 1, asserted_by: 'ana@x', inputs: [
        { role: 'samplesheet', name: 'samplesheet.csv', address: rawCid('sheet'), size: 40, sample: null, reason: null },
        { role: 'task', name: 'A_R1.fastq.gz', address: reads, size: 100, sample: 'A', reason: null },
        { role: 'task', name: 'big.bam', address: null, size: null, sample: 'A', reason: 'unaddressed' }] })
      const m4 = put({ kind: 'RunManifest', schema: 1, asserted_by: 'ana@x', pipeline: 'demo', repository: null, revision: null,
        commit_id: null, run_name: 'R4', nf_run_hash: 'hash-R4', session_id: 's', resumed: false, nextflow_version: '26.04.6',
        params: {}, config: {}, script: null, started_at: '2026-10-25T09:00:00.000Z' })
      ids.R4 = put({ kind: 'RunCompletion', schema: 1, asserted_by: 'ana@x', run: m4, collections: [], input_set: inputSet, status: 'succeeded',
        exit_status: 0, possibly_incomplete: false, started_at: '2026-10-25T09:00:00.000Z', finished_at: '2026-10-25T09:30:00.000Z',
        anomalies: { unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 }, error: null })
      ids.H1 = put({ kind: 'Handoff', schema: 1, asserted_by: 'ana@x', template: 'nf-core/differentialabundance', selection: ids.S,
        project: ids.P1, files: [], params: {}, timestamp: '2026-10-25T11:30:00.000Z' })
      ids.Hs = put({ kind: 'Handoff', schema: 1, asserted_by: 'ben@x', template: 'nf-core/differentialabundance', selection: ids.S,
        project: ids.P1, files: [], params: {}, timestamp: '2026-10-24T10:00:00.000Z' })
      ids.Hx = block({ kind: 'Handoff', never: 'stored' }).cid
      const t = (n) => NOW - 100_000 + n * 1000
      return {
        log: [entryName(t(1), 'project', ids.P2), entryName(t(2), 'claim', ids.DA), entryName(t(3), 'claim', ids.MR2),
          entryName(t(4), 'selection', ids.S), entryName(t(5), 'claim', ids.MS), entryName(t(6), 'claim', ids.N2),
          entryName(t(7), 'claim', ids.D3), entryName(t(8), 'claim', ids.SA), entryName(t(9), 'run', ids.R4),
          entryName(t(10), 'handoff', ids.H1),
          // Announced, and already in the snapshot's handoff table: not read again.
          entryName(t(12), 'handoff', ids.Hs),
          // Announced, and no Handoff block: listed as unreadable.
          entryName(t(13), 'handoff', ids.Hx),
          // Announced, and already in the snapshot: not read again.
          entryName(t(11), 'project', ids.P1)],
        rows: (db) => {
          const row = (sql, bind) => db.exec({ sql, bind })
          for (const [p, v] of [['P1', project('Liver study', '2026-10-20T10:00:00.000Z')], ['P3', project('Old', '2026-10-01T10:00:00.000Z')]])
            row('INSERT INTO project VALUES (?, ?, ?, ?, ?)', [ids[p].toString(), v.name, v.description, v.asserted_by, v.created_at])
          row('INSERT INTO project_stats VALUES (?, 1000, 200, 300, 50, 2)', [ids.P1.toString()])
          const snapshotClaim = (c, subject, verb, attribute, value, by = 'ana@x') => {
            row('INSERT INTO claim VALUES (?, ?, ?, ?, ?, ?, ?)', [ids[c].toString(), subject.toString(), verb, attribute, value, AT, by])
            row('INSERT INTO claim_current VALUES (?, ?, ?, ?, 0)', [subject.toString(), attribute, value, ids[c].toString()])
          }
          snapshotClaim('MR1', R1, 'add', 'project', linkText(ids.P1))
          snapshotClaim('MB', item.B, 'add', 'project', linkText(ids.P1), 'ben@x')
          snapshotClaim('MA', item.A, 'add', 'project', linkText(ids.P1))
          snapshotClaim('MP2', R1, 'add', 'project', linkText(ids.P2))
          snapshotClaim('N1', ids.P1, 'set', 'name', 'Liver')
          row("INSERT INTO run_input VALUES (?, 'task', 'A_R1.fastq.gz', ?, 100, 'A')", [runs.R1.completion, ids.reads.toString()])
          row("INSERT INTO run_input VALUES (?, 'task', 'B_R1.fastq.gz', ?, 100, 'B')", [runs.R1.completion, rawCid('reads-B').toString()])
          row("INSERT INTO run_input VALUES (?, 'task', 'other.fastq.gz', ?, 100, 'Q')", ['bafyother', rawCid('reads-B').toString()])
          row("INSERT INTO handoff VALUES (?, 'nf-core/differentialabundance', 'bafysel', ?, 'ben@x', '2026-10-24T10:00:00.000Z')", [ids.Hs.toString(), ids.P1.toString()])
          row("INSERT INTO handoff VALUES ('bafyhother', 'nf-core/differentialabundance', 'bafysel', NULL, 'ben@x', '2026-10-23T10:00:00.000Z')", [])
        },
      }
    },
  })
  const fetchFn = blockFetch(member.blocks)
  const explorer = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn }), listFn: async () => ({ names: log ?? member.log, readable: true }), now: () => NOW })
  return { explorer, member, fetchFn, ids: Object.fromEntries(Object.entries(ids).map(([k, v]) => [k, v.toString()])) }
}

test('projects: snapshot rows and tail Project blocks, newest first, named by their current name Claims; a deleted one is hidden', async () => {
  const { explorer, ids } = await open()
  assert.deepEqual(explorer.tailProjects.map(t => t.cid), [ids.P2])
  const { rows, hiddenCount } = await explorer.projects()
  assert.deepEqual(rows.map(r => [r.cid, r.name, r.source]), [[ids.P2, 'Kidney', 'tail'], [ids.P1, 'Liver, batch 2', 'snapshot']])
  assert.equal(hiddenCount, 1)
  const p1 = rows[1]
  assert.deepEqual([p1.blockName, p1.description, p1.createdBy, p1.createdAt], ['Liver study', 'about Liver study', 'ana@x', '2026-10-20T10:00:00.000Z'])
  assert.deepEqual(p1.state.nameClaims, [ids.N2])
  const deleted = await explorer.projects({ showDeleted: true })
  assert.deepEqual(deleted.rows.map(r => r.cid), [ids.P3])
})

test('project: from the snapshot row, a tail block, or a block no entry announces; with its Claims and state', async () => {
  const { explorer, ids } = await open()
  const p1 = await explorer.project(ids.P1)
  assert.deepEqual([p1.cid, p1.name, p1.blockName, p1.source], [ids.P1, 'Liver, batch 2', 'Liver study', 'snapshot'])
  assert.deepEqual(p1.samples.map(s => s.ref), [{ pipeline: 'demo', id: 'A' }])
  assert.equal(p1.policy.retention.value, 'none')
  assert.deepEqual(p1.claims.map(c => c.cid).sort(), [ids.N1, ids.N2, ids.SA].sort())
  assert.equal((await explorer.project(ids.P2)).source, 'tail')
  const px = await explorer.project(ids.Px)
  assert.deepEqual([px.name, px.createdBy, px.source], ['Elsewhere', 'ana@x', 'block'])
  await assert.rejects(explorer.project(block({ kind: 'Project', nothing: 1 }).cid.toString()), (e) => e instanceof BlockError)
  await assert.rejects(explorer.project(ids.S), (e) => e instanceof BlockError && e.code === 'schema_invalid')
})

test('projectMembers: snapshot and tail memberships with their kinds and claim tags; a tail del removes one', async () => {
  const { explorer, ids, member } = await open()
  const members = await explorer.projectMembers(ids.P1)
  assert.deepEqual(members.map(m => [m.subject, m.kind, m.claim.cid]).sort(), [
    [member.runs.R1.completion, 'run', ids.MR1],
    [member.runs.R2.completion, 'run', ids.MR2],
    [member.item.B, 'item', ids.MB],
    [ids.S, 'selection', ids.MS],
  ].sort())
  const b = members.find(m => m.subject === member.item.B)
  assert.deepEqual(b.claim, { cid: ids.MB, asserted_by: 'ben@x', timestamp: AT })
  assert.equal(b.hidden, false)
  assert.deepEqual((await explorer.projectMembers(ids.P2)).map(m => m.subject), [member.runs.R1.completion])
  assert.deepEqual(await explorer.projectMembers(ids.P3), [])
})

test('projectMemberships lists every current membership; projectsOf gives each subject its projects', async () => {
  const { explorer, ids, member } = await open()
  const all = await explorer.projectMemberships()
  assert.deepEqual(all.map(m => [m.subject, m.project]).sort(), [
    [member.runs.R1.completion, ids.P1], [member.runs.R1.completion, ids.P2], [member.runs.R2.completion, ids.P1],
    [member.item.B, ids.P1], [ids.S, ids.P1]].sort())
  const of = await explorer.projectsOf([member.runs.R1.completion, member.item.A])
  assert.deepEqual(of.get(member.runs.R1.completion).map(m => m.project).sort(), [ids.P1, ids.P2].sort())
  assert.deepEqual(of.get(member.item.A), [])
})

test('subjectKinds: runs, Selections and items from the snapshot and the tail, else the block\'s kind, else null', async () => {
  const { explorer, ids, member } = await open()
  const kinds = await explorer.subjectKinds([member.runs.R1.completion, ids.R4, member.item.A, member.item.C, ids.S, ids.P1, 'bafynothing'])
  assert.deepEqual([...kinds], [[member.runs.R1.completion, 'run'], [ids.R4, 'run'], [member.item.A, 'item'], [member.item.C, 'item'],
    [ids.S, 'selection'], [ids.P1, null], ['bafynothing', null]])
})

test('subjectKinds reads the blocks it has to, SIZE_CONCURRENCY at a time', async () => {
  const { explorer } = await open()
  let running = 0
  let most = 0
  explorer.blocks = { get: async () => {
    running++
    most = Math.max(most, running)
    await new Promise(r => setTimeout(r, 2))
    running--
    return { value: { kind: 'OutputItem' } }
  } }
  const unknown = Array.from({ length: 3 * SIZE_CONCURRENCY }, (_, i) => `bafyunknown${i}`)
  const kinds = await explorer.subjectKinds(unknown)
  assert.deepEqual([...kinds.values()], unknown.map(() => 'item'))
  assert.equal(most, SIZE_CONCURRENCY)
})

test('projects with includeDeleted: every row, current and deleted, told apart by state.hidden', async () => {
  const { explorer, ids } = await open()
  const { rows, hiddenCount } = await explorer.projects({ includeDeleted: true })
  assert.deepEqual(rows.map(r => [r.cid, r.state.hidden]), [[ids.P2, false], [ids.P1, false], [ids.P3, true]])
  assert.equal(hiddenCount, 0)
})

test('projectStatsOf: every listed project\'s row in one read; none for a project without a row', async () => {
  const { explorer, ids } = await open()
  const stats = await explorer.projectStatsOf([ids.P1, ids.P2, ids.P1])
  assert.deepEqual([...stats], [[ids.P1, { reachable: 1000, pinned: 200, held: 300, releasing: 50, roots: 2 }]])
  assert.deepEqual(await explorer.projectStatsOf([]), new Map())
})

test('projectStats: the snapshot\'s row for this store, else null', async () => {
  const { explorer, ids } = await open()
  assert.deepEqual(await explorer.projectStats(ids.P1), { reachable: 1000, pinned: 200, held: 300, releasing: 50, roots: 2 })
  assert.equal(await explorer.projectStats(ids.P2), null)
})

test('runInputs: snapshot rows, and a tail run\'s InputSet block; sharedInputRows: task rows whose address two runs read', async () => {
  const { explorer, ids, member } = await open()
  const r1 = await explorer.runInputs([member.runs.R1.completion])
  assert.deepEqual(r1.map(r => [r.role, r.name, r.sample, r.source]), [['task', 'A_R1.fastq.gz', 'A', 'snapshot'], ['task', 'B_R1.fastq.gz', 'B', 'snapshot']])
  const r4 = await explorer.runInputs([ids.R4])
  assert.deepEqual(r4, [
    { completion_cid: ids.R4, role: 'samplesheet', name: 'samplesheet.csv', address: rawCid('sheet').toString(), size: 40, sample: null, source: 'tail' },
    { completion_cid: ids.R4, role: 'task', name: 'A_R1.fastq.gz', address: ids.reads, size: 100, sample: 'A', source: 'tail' },
    { completion_cid: ids.R4, role: 'task', name: 'big.bam', address: null, size: null, sample: 'A', source: 'tail' }])
  assert.deepEqual(await explorer.runInputs([member.runs.R2.completion]), [])
  const shared = await explorer.sharedInputRows()
  assert.deepEqual(shared.map(r => [r.completion_cid, r.sample]).sort(), [[member.runs.R1.completion, 'B'], ['bafyother', 'Q']].sort())
})

test('handoffs: snapshot rows and tail Handoff blocks, newest first, by project or all', async () => {
  const { explorer, ids } = await open()
  const all = await explorer.handoffs()
  // Hs is announced in the tail too, and read from its row only (handoffsKnown).
  assert.deepEqual(explorer.tailHandoffs.map(t => t.cid).sort(), [ids.H1, ids.Hx].sort())
  assert.deepEqual(all.map(h => [h.cid, h.source]), [[ids.H1, 'tail'], [ids.Hs, 'snapshot'], ['bafyhother', 'snapshot']])
  assert.deepEqual(explorer.unreadableHandoffs().map(h => [h.cid, h.error.code]), [[ids.Hx, 'block_missing']])
  assert.deepEqual(all[0], { cid: ids.H1, template: 'nf-core/differentialabundance', selection: ids.S, project: ids.P1,
    assertedBy: 'ana@x', at: '2026-10-25T11:30:00.000Z', source: 'tail' })
  assert.deepEqual((await explorer.handoffs({ project: ids.P1 })).map(h => h.cid), [ids.H1, ids.Hs])
})

test('a write the page makes shows after refreshTail: a new membership Claim joins the project', async () => {
  const { explorer, ids, member } = await open()
  const extra = block(claim(CID.parse(member.item.C), 'add', 'project', CID.parse(ids.P2)))
  member.blocks.set(extra.cid.toString(), extra.bytes)
  await explorer.refreshTail(async () => ({ names: [...member.log, entryName(NOW - 1000, 'claim', extra.cid)], readable: true }))
  assert.deepEqual((await explorer.projectMembers(ids.P2)).map(m => m.subject).sort(), [member.runs.R1.completion, member.item.C].sort())
})

test('a tail run whose InputSet block the store cannot give has no inputs, and raises no error', async () => {
  const { explorer, ids } = await open({ inputSetStored: false })
  assert.deepEqual(await explorer.runInputs([ids.R4]), [])
})

test('projectHistory: added and removed memberships of one project, a removal in the snapshot included; another project\'s are left out', async () => {
  const ids = {}
  const member = await buildMember({
    now: NOW,
    extra: ({ put, runs, item }) => {
      const R1 = CID.parse(runs.R1.completion)
      const R2 = CID.parse(runs.R2.completion)
      ids.P = put(project('History', '2026-10-20T10:00:00.000Z'))
      ids.Q = put(project('Other', '2026-10-20T10:00:00.000Z'))
      ids.MR1 = put(claim(R1, 'add', 'project', ids.P))
      ids.DR1 = put(claim(R1, 'del', 'project', null, [ids.MR1], { by: 'ben@x', timestamp: '2026-10-25T10:30:00.000Z' }))
      ids.MB = put(claim(CID.parse(item.B.toString()), 'add', 'project', ids.P))
      ids.MQ = put(claim(R1, 'add', 'project', ids.Q))
      ids.MR2 = put(claim(R2, 'add', 'project', ids.P))
      return {
        log: [entryName(NOW - 50_000, 'claim', ids.MR2)],
        rows: (db) => {
          const row = (sql, bind) => db.exec({ sql, bind })
          const add = (c, subject, verb, value, current) => {
            row('INSERT INTO claim VALUES (?, ?, ?, ?, ?, ?, ?)', [ids[c].toString(), subject.toString(), verb, 'project', value, AT, 'ana@x'])
            if (current) row('INSERT INTO claim_current VALUES (?, ?, ?, ?, 0)', [subject.toString(), 'project', value, ids[c].toString()])
          }
          add('MR1', R1, 'add', linkText(ids.P), false)
          add('DR1', R1, 'del', null, true)
          row('INSERT INTO claim_supersedes VALUES (?, ?)', [ids.DR1.toString(), ids.MR1.toString()])
          add('MB', item.B, 'add', linkText(ids.P), true)
          add('MQ', R1, 'add', linkText(ids.Q), true)
        },
      }
    },
  })
  const explorer = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }), listFn: async () => ({ names: member.log, readable: true }), now: () => NOW })
  const history = await explorer.projectHistory(ids.P.toString())
  assert.deepEqual(history.map(r => [r.subject, r.kind, r.claim.verb, r.claim.cid]).sort(), [
    [member.runs.R1.completion, 'run', 'add', ids.MR1.toString()],
    [member.runs.R1.completion, 'run', 'del', ids.DR1.toString()],
    [member.runs.R2.completion, 'run', 'add', ids.MR2.toString()],
    [member.item.B, 'item', 'add', ids.MB.toString()],
  ].sort())
  assert.deepEqual((await explorer.projectHistory(ids.Q.toString())).map(r => r.claim.cid), [ids.MQ.toString()])
})

test('itemCollections: an item\'s output collection from the snapshot, else a tail run\'s blocks; an unknown item has none', async () => {
  const { explorer, member } = await open()
  const found = await explorer.itemCollections([member.item.A, member.item.C, 'bafynothing'])
  assert.equal(found.get(member.item.A), member.runs.R1.collection)
  assert.ok([member.runs.R2.collection, member.runs.R3.collection].includes(found.get(member.item.C)))
  assert.equal(found.has('bafynothing'), false)
  assert.deepEqual(await explorer.itemCollections([]), new Map())
})

test('projectHistory: a tail del of a tail add shows as added, then removed', async () => {
  const { explorer, ids, member } = await open()
  const add = block(claim(CID.parse(member.item.C), 'add', 'project', CID.parse(ids.P2), [], { timestamp: '2026-10-25T11:40:00.000Z' }))
  const del = block(claim(CID.parse(member.item.C), 'del', 'project', null, [add.cid], { by: 'ben@x', timestamp: '2026-10-25T11:50:00.000Z' }))
  for (const b of [add, del]) member.blocks.set(b.cid.toString(), b.bytes)
  await explorer.refreshTail(async () => ({ names: [...member.log, entryName(NOW - 2000, 'claim', add.cid), entryName(NOW - 1000, 'claim', del.cid)], readable: true }))
  const history = (await explorer.projectHistory(ids.P2)).filter(r => r.subject === member.item.C).sort((a, b) => (a.claim.timestamp < b.claim.timestamp ? -1 : 1))
  assert.deepEqual(history.map(r => [r.kind, r.claim.verb, r.claim.cid, r.claim.source]),
    [['item', 'add', add.cid.toString(), 'tail'], ['item', 'del', del.cid.toString(), 'tail']])
  assert.deepEqual((await explorer.projectMembers(ids.P2)).map(m => m.subject), [member.runs.R1.completion])
})
