import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as dagJson from '@ipld/dag-json'
import { CID } from 'multiformats/cid'
import { writer, WriteError } from '../src/write.js'
import { block } from './fixture.mjs'

const S = block({ s: 1 }).cid
const N = block({ n: 1 }).cid

function fakeFetch(respond) {
  const calls = []
  const fn = async (url, init) => {
    calls.push({ url, init, body: dagJson.decode(init.body) })
    return respond(url, init)
  }
  fn.calls = calls
  return fn
}

const ok = (value) => new Response(dagJson.encode(value), { status: 200, headers: { 'Content-Type': 'application/vnd.ipld.dag-json' } })

test('a rename posts the DAG-JSON Claim with the token, superseding what was shown', async () => {
  const fetchFn = fakeFetch(() => ok({ address: N, block: {}, entry: 'e', written: true }))
  const w = writer({ endpoint: 'http://h/api/put', token: 't0k', fetchFn, now: () => new Date('2026-09-25T10:00:00.000Z') })
  const r = await w.rename(S.toString(), 'tumour, "batch 2"', [N.toString()])
  const [call] = fetchFn.calls
  assert.equal(call.url, 'http://h/api/put')
  assert.equal(call.init.method, 'POST')
  assert.equal(call.init.headers['Content-Type'], 'application/vnd.ipld.dag-json')
  assert.equal(call.init.headers['X-NF-Blocks-Token'], 't0k')
  assert.deepEqual(call.body, { kind: 'Claim', subject: S, verb: 'set', attribute: 'name', value: 'tumour, "batch 2"', supersedes: [N], timestamp: '2026-09-25T10:00:00.000Z' })
  assert.equal(r.address, N.toString())
})

test('delete and undo supersede the deletion Claims shown; a dry run asks with ?dry_run=true', async () => {
  const fetchFn = fakeFetch(() => ok({ address: S, exists: false, names: [] }))
  const w = writer({ endpoint: 'http://h/api/put', token: 't', fetchFn, now: () => new Date('2026-09-25T10:00:00.000Z') })
  await w.remove(S.toString(), [])
  await w.undo(S.toString(), [N.toString()])
  await w.selection([{ item: { address: N, via: [] } }], { dryRun: true })
  assert.equal(fetchFn.calls[0].body.verb, 'delete')
  assert.equal(fetchFn.calls[0].body.attribute, null)
  assert.deepEqual(fetchFn.calls[1].body.supersedes, [N])
  assert.equal(fetchFn.calls[1].body.verb, 'del')
  assert.equal(fetchFn.calls[2].url, 'http://h/api/put?dry_run=true')
  assert.deepEqual(fetchFn.calls[2].body, { kind: 'Selection', members: [{ item: { address: N, via: [] } }], derived_from: [] })
})

test('a dry run decodes name_claims (DAG-JSON links) to CID strings', async () => {
  const fetchFn = fakeFetch(() => ok({ address: S, exists: true, here: false, names: ['a', 'b'], name_claims: [N, S] }))
  const w = writer({ endpoint: 'http://h/api/put', token: 't', fetchFn })
  const r = await w.selection([{ item: { address: N, via: [] } }], { dryRun: true })
  assert.deepEqual(r.name_claims, [N.toString(), S.toString()])
})

test('a dry run decodes deletion_claims (DAG-JSON links) to CID strings', async () => {
  const fetchFn = fakeFetch(() => ok({ address: S, exists: true, here: false, names: [], name_claims: [], deletion: 'deleted', deletion_claims: [N] }))
  const w = writer({ endpoint: 'http://h/api/put', token: 't', fetchFn })
  const r = await w.selection([{ item: { address: N, via: [] } }], { dryRun: true })
  assert.deepEqual(r.deletion_claims, [N.toString()])
})

test('release, restore, pin and unpin send the Claims put accepts', async () => {
  const RUN = S.toString()
  const ITEM = N.toString()
  const CLAIM = block({ c: 1 }).cid.toString()
  const fetchFn = fakeFetch(() => ok({ address: S, written: true }))
  const w = writer({ endpoint: 'http://h/api/put', token: 't', fetchFn, now: () => new Date('2026-09-30T10:00:00.000Z') })
  await w.release(RUN, [])
  await w.restore(RUN, [CLAIM])
  await w.pin(ITEM, 'figure 3')
  await w.unpin(ITEM, CLAIM)
  assert.deepEqual(fetchFn.calls.map(c => [c.body.verb, c.body.attribute, c.body.value, c.body.supersedes.map(String)]), [
    ['set', 'retain', 'lineage', []],
    ['del', 'retain', null, [CLAIM]],
    ['add', 'pin', 'figure 3', []],
    ['del', 'pin', null, [CLAIM]],
  ])
})

test('a refusal becomes a WriteError with its code and where; a plain-text refusal keeps its status', async () => {
  const w = writer({ endpoint: 'http://h/api/put', token: 't', fetchFn: fakeFetch(() => new Response(
    dagJson.encode({ error: 'stale_supersedes', message: 'claim is already superseded', at: '/supersedes/0' }),
    { status: 400, headers: { 'Content-Type': 'application/vnd.ipld.dag-json' } })) })
  await assert.rejects(w.rename(S.toString(), 'x', [N.toString()]), (e) => e instanceof WriteError && e.code === 'stale_supersedes' && e.at === '/supersedes/0' && e.status === 400)
  const plain = writer({ endpoint: 'http://h/api/put', token: 'wrong', fetchFn: fakeFetch(() => new Response('refused', { status: 403 })) })
  await assert.rejects(plain.remove(S.toString(), []), (e) => e instanceof WriteError && e.code === 'forbidden' && e.status === 403)
})

// ---- P4: projects and hand-offs (plan 1.1, 1.2, 1.5) ----

const P = block({ project: 1 }).cid
const RUN = block({ run: 1 }).cid
const RUN2 = block({ run: 2 }).cid
const C1 = block({ claim: 1 }).cid
const C2 = block({ claim: 2 }).cid
const AT = '2026-10-25T10:00:00.000Z'
const projectWriter = (fetchFn) => writer({ endpoint: 'http://h/api/put', token: 't', fetchFn, now: () => new Date(AT) })

test('project posts the Project kind with created_at from the clock; a dry run asks with ?dry_run=true', async () => {
  const fetchFn = fakeFetch(() => ok({ address: P, written: true }))
  const w = projectWriter(fetchFn)
  const r = await w.project({ name: 'Liver study', description: '' })
  await w.project({ name: 'x', description: 'y' }, { dryRun: true })
  assert.deepEqual(fetchFn.calls[0].body, { kind: 'Project', name: 'Liver study', description: '', created_at: AT })
  assert.equal(fetchFn.calls[0].url, 'http://h/api/put')
  assert.equal(fetchFn.calls[1].url, 'http://h/api/put?dry_run=true')
  assert.equal(r.address, P.toString())
})

test('membership: add project carries the Project as a CID link; remove supersedes the membership Claims', async () => {
  const fetchFn = fakeFetch(() => ok({ address: C1, written: true }))
  const w = projectWriter(fetchFn)
  await w.addToProject(RUN.toString(), P.toString())
  await w.removeFromProject(RUN.toString(), [C1.toString(), C2.toString()])
  const [add, del] = fetchFn.calls.map(c => c.body)
  assert.deepEqual(add, { kind: 'Claim', subject: RUN, verb: 'add', attribute: 'project', value: P, supersedes: [], timestamp: AT })
  assert.ok(add.value instanceof CID)
  // On the wire the value is a DAG-JSON link, as the index keeps it.
  assert.match(new TextDecoder().decode(fetchFn.calls[0].init.body), new RegExp(`"value":\\{"/":"${P}"\\}`))
  assert.deepEqual(del, { kind: 'Claim', subject: RUN, verb: 'del', attribute: 'project', value: null, supersedes: [C1, C2], timestamp: AT })
})

test('samples, annotations and same_sample are Claims on the Project; edits supersede, removals are del', async () => {
  const fetchFn = fakeFetch(() => ok({ address: C1, written: true }))
  const w = projectWriter(fetchFn)
  const project = P.toString()
  const ref = { pipeline: 'nf-core/rnaseq', id: 'WT_1' }
  await w.addSample(project, ref)
  await w.removeSample(project, [C1.toString()])
  await w.annotate(project, ref, 'condition', 'treated')
  await w.annotate(project, ref, 'condition', 'control', [C1.toString()])
  await w.clearAnnotation(project, [C2.toString()])
  await w.sameSample(project, { ...ref, run: RUN.toString() }, { pipeline: 'nf-core/rnaseq', id: 'WT1', run: RUN2.toString() }, true)
  await w.sameSample(project, { ...ref, run: RUN.toString() }, { ...ref, run: RUN2.toString() }, false, [C1.toString()])
  await w.undoSameSample(project, [C2.toString()])
  const rows = fetchFn.calls.map(c => c.body)
  assert.ok(rows.every(r => r.kind === 'Claim' && r.subject.equals(P) && r.timestamp === AT))
  assert.deepEqual(rows.map(r => [r.verb, r.attribute, r.value, r.supersedes.map(String)]), [
    ['add', 'sample', { pipeline: 'nf-core/rnaseq', id: 'WT_1' }, []],
    ['del', 'sample', null, [C1.toString()]],
    ['add', 'annotation', { sample: { pipeline: 'nf-core/rnaseq', id: 'WT_1' }, name: 'condition', value: 'treated' }, []],
    ['add', 'annotation', { sample: { pipeline: 'nf-core/rnaseq', id: 'WT_1' }, name: 'condition', value: 'control' }, [C1.toString()]],
    ['del', 'annotation', null, [C2.toString()]],
    ['add', 'same_sample', { a: { pipeline: 'nf-core/rnaseq', id: 'WT_1', run: RUN }, b: { pipeline: 'nf-core/rnaseq', id: 'WT1', run: RUN2 }, same: true }, []],
    ['add', 'same_sample', { a: { pipeline: 'nf-core/rnaseq', id: 'WT_1', run: RUN }, b: { pipeline: 'nf-core/rnaseq', id: 'WT_1', run: RUN2 }, same: false }, [C1.toString()]],
    ['del', 'same_sample', null, [C2.toString()]],
  ])
  assert.ok(rows[5].value.a.run instanceof CID)
})

test('policies: default store and retention are set Claims, cleared by del; people are add and del', async () => {
  const fetchFn = fakeFetch(() => ok({ address: C1, written: true }))
  const w = projectWriter(fetchFn)
  const project = P.toString()
  await w.setPolicy(project, 'default_store', 'lab', [])
  await w.setPolicy(project, 'retention', 'pin_selections', [C1.toString()])
  await w.clearPolicy(project, 'default_store', [C2.toString()])
  await w.addPerson(project, 'ana@example.org')
  await w.removePerson(project, [C1.toString()])
  assert.deepEqual(fetchFn.calls.map(c => [c.body.verb, c.body.attribute, c.body.value, c.body.supersedes.map(String)]), [
    ['set', 'policy.default_store', 'lab', []],
    ['set', 'policy.retention', 'pin_selections', [C1.toString()]],
    ['del', 'policy.default_store', null, [C2.toString()]],
    ['add', 'policy.people', 'ana@example.org', []],
    ['del', 'policy.people', null, [C1.toString()]],
  ])
  assert.throws(() => w.setPolicy(project, 'people', 'x', []), /not a set policy/)
})

const json = (value, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })
const HANDOFF = {
  template: 'nf-core/differentialabundance', selection: RUN.toString(), project: P.toString(),
  files: [{ name: 'samplesheet.csv', text: 'sample,condition\nA,treated\n' }],
  params: { input: { file: 'samplesheet.csv' }, matrix: 'cas://bafkreia/salmon.merged.gene_counts.tsv' },
}

test('handoff posts plain JSON to api/handoff beside api/put, with the token and a timestamp', async () => {
  const calls = []
  const fetchFn = async (url, init) => { calls.push({ url, init }); return json({ address: 'bafyh', files: [], params: {}, entry: 'e', written: true }) }
  const w = projectWriter(fetchFn)
  const r = await w.handoff(HANDOFF)
  await w.handoff(HANDOFF, { dryRun: true })
  assert.equal(calls[0].url, 'http://h/api/handoff')
  assert.equal(calls[1].url, 'http://h/api/handoff?dry_run=true')
  assert.equal(calls[0].init.method, 'POST')
  assert.equal(calls[0].init.headers['Content-Type'], 'application/json')
  assert.equal(calls[0].init.headers['X-NF-Blocks-Token'], 't')
  assert.deepEqual(JSON.parse(calls[0].init.body), { ...HANDOFF, timestamp: AT })
  assert.deepEqual(r, { address: 'bafyh', files: [], params: {}, entry: 'e', written: true })
  // A request with no project sends null.
  await w.handoff({ ...HANDOFF, project: undefined })
  assert.equal(JSON.parse(calls[2].init.body).project, null)
})

test('a handoff refusal is a WriteError from its plain JSON body; a plain-text one keeps its status', async () => {
  const w = projectWriter(async () => json({ error: 'not_found', message: 'no selection bafy' }, 400))
  await assert.rejects(w.handoff(HANDOFF), (e) => e instanceof WriteError && e.code === 'not_found' && e.message === 'no selection bafy' && e.status === 400)
  const plain = projectWriter(async () => new Response('refused', { status: 403 }))
  await assert.rejects(plain.handoff(HANDOFF), (e) => e instanceof WriteError && e.code === 'forbidden' && e.status === 403)
  const gone = projectWriter(async () => { throw new Error('ECONNREFUSED') })
  await assert.rejects(gone.handoff(HANDOFF), (e) => e instanceof WriteError && e.code === 'write_failed')
})

test('a handoff answer labelled JSON that does not parse is a WriteError write_failed', async () => {
  const garbled = (status) => projectWriter(async () => new Response('{"address": ', { status, headers: { 'Content-Type': 'application/json' } }))
  for (const status of [200, 400])
    await assert.rejects(garbled(status).handoff(HANDOFF), (e) => e instanceof WriteError && e.code === 'write_failed' && e.status === status)
})
