// Project state from Claim rows (P4 plan 1.2): membership, samples, annotations, same_sample and policies.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { annotations, decodeValue, membershipsOf, policyState, projectState, sameSampleClaims, sampleAnnotations, sampleRefs } from '../src/projects.js'

const P = 'bafyreiproject1'
const Q = 'bafyreiproject2'
const link = (cid) => `{"/":"${cid}"}`
let n = 0
/** A Claim row as Explorer.claimsFor gives it: a non-string value as DAG-JSON text. */
const claim = (verb, attribute, value, { supersedes = [], by = 'ana@x', at = '2026-10-25T10:00:00.000Z', cid = `c${++n}`, subject = P } = {}) =>
  ({ cid, subject, verb, attribute, value, supersedes, asserted_by: by, timestamp: at })
const ref = (id, pipeline = 'nf-core/rnaseq') => `{"id":"${id}","pipeline":"${pipeline}"}`

test('decodeValue reads DAG-JSON text, links as cid strings; a plain string or null stays as it is', () => {
  assert.equal(decodeValue(link(P)), P)
  assert.deepEqual(decodeValue('{"a":{"id":"A","pipeline":"p","run":{"/":"bafyrun"}},"b":{"id":"B","pipeline":"p","run":{"/":"bafyrun2"}},"same":true}'),
    { a: { id: 'A', pipeline: 'p', run: 'bafyrun' }, b: { id: 'B', pipeline: 'p', run: 'bafyrun2' }, same: true })
  assert.deepEqual(decodeValue({ sample: { id: 'A', pipeline: 'p' } }), { sample: { id: 'A', pipeline: 'p' } })
  assert.equal(decodeValue('not json'), undefined)
  assert.equal(decodeValue(null), null)
})

test('membership: the current add project Claims of each subject, with who and when; a del superseding one removes it', () => {
  const run = 'bafyrun1'
  const sel = 'bafysel1'
  const add1 = claim('add', 'project', link(P), { subject: run, cid: 'm1' })
  const add2 = claim('add', 'project', link(Q), { subject: run, cid: 'm2', by: 'ben@x' })
  const add3 = claim('add', 'project', link(P), { subject: sel, cid: 'm3' })
  const del3 = claim('del', 'project', null, { subject: sel, cid: 'm4', supersedes: ['m3'] })
  const name = claim('set', 'name', 'x', { subject: run, cid: 'm5' })
  const rows = membershipsOf(new Map([[run, [add1, add2, name]], [sel, [add3, del3]]]))
  assert.deepEqual(rows, [
    { subject: run, project: P, claim: { cid: 'm1', asserted_by: 'ana@x', timestamp: '2026-10-25T10:00:00.000Z' }, hidden: false },
    { subject: run, project: Q, claim: { cid: 'm2', asserted_by: 'ben@x', timestamp: '2026-10-25T10:00:00.000Z' }, hidden: false },
  ])
})

test('membership of a deleted subject is still listed, marked hidden', () => {
  const run = 'bafyrun1'
  const rows = membershipsOf(new Map([[run, [claim('add', 'project', link(P), { subject: run }), claim('delete', null, null, { subject: run })]]]))
  assert.equal(rows.length, 1)
  assert.equal(rows[0].hidden, true)
})

test('sample refs: current add sample Claims, one entry per SampleRef with every Claim that adds it', () => {
  const a1 = claim('add', 'sample', ref('A'), { cid: 's1' })
  const a2 = claim('add', 'sample', ref('A'), { cid: 's2', by: 'ben@x' })
  const b = claim('add', 'sample', ref('B'), { cid: 's3' })
  const delB = claim('del', 'sample', null, { cid: 's4', supersedes: ['s3'] })
  const refs = sampleRefs([a1, a2, b, delB])
  assert.deepEqual(refs.map(r => [r.ref, r.claims.map(c => c.cid)]), [[{ pipeline: 'nf-core/rnaseq', id: 'A' }, ['s1', 's2']]])
})

test('annotations: one per (sample, name); two current Claims with different values conflict, the same value twice does not', () => {
  const ann = (id, name, value, opts) => claim('add', 'annotation', `{"name":"${name}","sample":${ref(id)},"value":"${value}"}`, opts)
  const c1 = ann('A', 'condition', 'treated', { cid: 'a1' })
  const c2 = ann('A', 'condition', 'control', { cid: 'a2', by: 'ben@x' })
  const c3 = ann('B', 'condition', 'control', { cid: 'a3' })
  const c4 = ann('B', 'condition', 'control', { cid: 'a4' })
  const c5 = ann('B', 'batch', '1', { cid: 'a5' })
  const c6 = ann('B', 'batch', '2', { cid: 'a6', supersedes: ['a5'] })
  const c7 = ann('C', 'batch', '1', { cid: 'a7' })
  const c8 = claim('del', 'annotation', null, { cid: 'a8', supersedes: ['a7'] })
  const list = annotations([c1, c2, c3, c4, c5, c6, c7, c8])
  assert.deepEqual(list.map(a => [a.sample.id, a.name, a.value, a.values, a.conflicted, a.claims.map(c => c.cid)]), [
    ['A', 'condition', null, ['control', 'treated'], true, ['a1', 'a2']],
    ['B', 'batch', '2', ['2'], false, ['a6']],
    ['B', 'condition', 'control', ['control'], false, ['a3', 'a4']],
  ])
})

test('sampleAnnotations joins a sample\'s annotations through any of its SampleRefs, conflicting across refs too', () => {
  const ann = (id, name, value, pipeline) => claim('add', 'annotation', `{"name":"${name}","sample":${ref(id, pipeline)},"value":"${value}"}`)
  const list = annotations([ann('A', 'condition', 'treated'), ann('a1', 'condition', 'control', 'nf-core/sarek'), ann('A', 'batch', '1'), ann('Z', 'batch', '9')])
  const byName = sampleAnnotations([{ pipeline: 'nf-core/rnaseq', id: 'A' }, { pipeline: 'nf-core/sarek', id: 'a1' }], list)
  assert.deepEqual([...byName.keys()], ['batch', 'condition'])
  assert.equal(byName.get('batch').value, '1')
  assert.equal(byName.get('condition').conflicted, true)
  assert.deepEqual(byName.get('condition').values, ['control', 'treated'])
  assert.equal(byName.get('condition').claims.length, 2)
  assert.deepEqual(byName.get('condition').entries.map(e => e.value).sort(), ['control', 'treated'], 'each current value with its Claim, for a conflicted cell')
  assert.deepEqual(byName.get('condition').entries.map(e => e.claim), byName.get('condition').claims)
})

test('same_sample: the current Claims\' values with run links as cid strings', () => {
  const v = '{"a":{"id":"A","pipeline":"p","run":{"/":"bafyr1"}},"b":{"id":"A","pipeline":"p","run":{"/":"bafyr2"}},"same":false}'
  const split = claim('add', 'same_sample', v, { cid: 'x1' })
  const gone = claim('add', 'same_sample', v.replace('false', 'true'), { cid: 'x2' })
  const undo = claim('del', 'same_sample', null, { cid: 'x3', supersedes: ['x2'] })
  assert.deepEqual(sameSampleClaims([split, gone, undo]), [
    { a: { pipeline: 'p', id: 'A', run: 'bafyr1' }, b: { pipeline: 'p', id: 'A', run: 'bafyr2' }, same: false,
      claim: { cid: 'x1', asserted_by: 'ana@x', timestamp: '2026-10-25T10:00:00.000Z' } }])
})

test('policies: default store and retention as set groups, people as a set; none by default', () => {
  assert.deepEqual(policyState([]), {
    defaultStore: { value: null, values: [], conflicted: false, claims: [] },
    retention: { value: 'none', values: [], conflicted: false, claims: [] },
    people: [],
  })
  const s1 = claim('set', 'policy.default_store', 'lab', { cid: 'p1' })
  const s2 = claim('set', 'policy.default_store', 'org-s3', { cid: 'p2', by: 'ben@x' })
  const r1 = claim('set', 'policy.retention', 'pin_selections', { cid: 'p3' })
  const ppl1 = claim('add', 'policy.people', 'ana@x', { cid: 'p4' })
  const ppl2 = claim('add', 'policy.people', 'ben@x', { cid: 'p5' })
  const ppl3 = claim('del', 'policy.people', null, { cid: 'p6', supersedes: ['p5'] })
  const ppl4 = claim('add', 'policy.people', 'cat@x', { cid: 'p7' })
  const s = policyState([s1, s2, r1, ppl1, ppl2, ppl3, ppl4])
  assert.equal(s.defaultStore.value, null)
  assert.equal(s.defaultStore.conflicted, true)
  assert.deepEqual(s.defaultStore.values, ['lab', 'org-s3'])
  assert.deepEqual(s.defaultStore.claims.map(c => c.cid), ['p1', 'p2'])
  assert.equal(s.retention.value, 'pin_selections')
  assert.deepEqual(s.retention.claims.map(c => c.cid), ['p3'])
  assert.deepEqual(s.people.map(p => [p.principal, p.claims.map(c => c.cid)]), [['ana@x', ['p4']], ['cat@x', ['p7']]])
  // Clearing the default store: a del superseding it leaves no value and no conflict.
  const cleared = policyState([s1, claim('del', 'policy.default_store', null, { cid: 'p8', supersedes: ['p1'] })])
  // The del stays current, so it is listed: the next set supersedes it.
  assert.deepEqual(cleared.defaultStore, { value: null, values: [], conflicted: false,
    claims: [{ cid: 'p8', asserted_by: 'ana@x', timestamp: '2026-10-25T10:00:00.000Z' }] })
  // Turning retention off is a set superseding the last.
  const off = policyState([r1, claim('set', 'policy.retention', 'none', { cid: 'p9', supersedes: ['p3'] })])
  assert.equal(off.retention.value, 'none')
  assert.deepEqual(off.retention.claims.map(c => c.cid), ['p9'])
})

test('projectState: the name from current name Claims, falling back to the block; deletion; and every part', () => {
  const block = { name: 'Liver study', description: 'd', asserted_by: 'ana@x', created_at: '2026-10-25T09:00:00.000Z' }
  const plain = projectState(block, [])
  assert.equal(plain.name, 'Liver study')
  assert.equal(plain.hidden, false)
  assert.deepEqual(plain.nameClaims, [])
  const renamed = projectState(block, [claim('set', 'name', 'Liver, batch 2', { cid: 'n1' }), claim('add', 'sample', ref('A'))])
  assert.equal(renamed.name, 'Liver, batch 2')
  assert.deepEqual(renamed.nameClaims, ['n1'])
  assert.equal(renamed.samples.length, 1)
  assert.equal(renamed.policy.retention.value, 'none')
  const gone = projectState(block, [claim('delete', null, null, { cid: 'd1' })])
  assert.equal(gone.hidden, true)
  assert.deepEqual(gone.deletionClaims, ['d1'])
})

test('policies: clear, then set superseding what the clear left, is one value and no conflict', () => {
  for (const [attribute, value, fallback] of [['policy.default_store', 'org', null], ['policy.retention', 'pin_selections', 'none']]) {
    const set1 = claim('set', attribute, value, { cid: `${attribute}-1` })
    const del = claim('del', attribute, null, { cid: `${attribute}-2`, supersedes: [set1.cid] })
    const key = attribute === 'policy.default_store' ? 'defaultStore' : 'retention'
    const cleared = policyState([set1, del])[key]
    assert.equal(cleared.value, fallback)
    assert.deepEqual(cleared.claims.map(c => c.cid), [del.cid])
    const set2 = claim('set', attribute, value, { cid: `${attribute}-3`, supersedes: cleared.claims.map(c => c.cid) })
    const after = policyState([set1, del, set2])[key]
    assert.deepEqual([after.value, after.values, after.conflicted, after.claims.map(c => c.cid)], [value, [value], false, [set2.cid]])
  }
})

test('policies: the same value set twice is one value; two values, or a value beside a clear, conflict', () => {
  const r1 = claim('set', 'policy.retention', 'pin_selections', { cid: 'r1' })
  const r2 = claim('set', 'policy.retention', 'pin_selections', { cid: 'r2', by: 'ben@x' })
  const twice = policyState([r1, r2]).retention
  assert.deepEqual([twice.value, twice.values, twice.conflicted, twice.claims.map(c => c.cid)], ['pin_selections', ['pin_selections'], false, ['r1', 'r2']])
  const off = claim('set', 'policy.retention', 'none', { cid: 'r3' })
  const two = policyState([r1, off]).retention
  assert.deepEqual([two.value, two.values, two.conflicted], [null, ['none', 'pin_selections'], true])
  const s1 = claim('set', 'policy.default_store', 'org', { cid: 's1' })
  const clear = claim('del', 'policy.default_store', null, { cid: 's2', by: 'ben@x' })
  const beside = policyState([s1, clear]).defaultStore
  assert.deepEqual([beside.value, beside.conflicted, beside.claims.map(c => c.cid)], [null, true, ['s1', 's2']])
})
