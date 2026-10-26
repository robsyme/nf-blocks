// Sample identity (P4 plan 1.6): sampleGroups pinned by fixtures/sample-vectors.json, and the input rows suggestions share.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { inputAddresses, matchRefs, occurrenceKey, runsSharingInputs, sampleGroups } from '../src/sampleid.js'

const vectors = JSON.parse(readFileSync(new URL('./fixtures/sample-vectors.json', import.meta.url), 'utf8'))
const keyText = (o) => `${o.run}/${o.id}`

for (const v of vectors) {
  test(`sample groups: ${v.name}`, () => {
    const groups = sampleGroups(v.occurrences, { inputs: v.inputs, claims: v.claims })
    assert.deepEqual(groups.map(g => ({ label: g.label, basis: g.basis, occurrences: g.occurrences.map(keyText) })), v.expect)
  })
}

test('a group lists its occurrences\' SampleRefs once each, for annotations', () => {
  const [g] = sampleGroups([
    { pipeline: 'p', id: 'A', run: 'r1' }, { pipeline: 'p', id: 'A', run: 'r2' }, { pipeline: 'q', id: 'a', run: 'r3' }],
  { claims: [{ a: { pipeline: 'p', id: 'A', run: 'r1' }, b: { pipeline: 'q', id: 'a', run: 'r3' }, same: true }] })
  assert.deepEqual(g.refs, [{ pipeline: 'p', id: 'A' }, { pipeline: 'q', id: 'a' }])
})

test('an occurrence appears once however often it is given', () => {
  const groups = sampleGroups([{ pipeline: 'p', id: 'A', run: 'r1' }, { pipeline: 'p', id: 'A', run: 'r1' }])
  assert.equal(groups.length, 1)
  assert.equal(groups[0].occurrences.length, 1)
})

test('inputAddresses keeps task rows for their own sample, without shared references or unhashed files', () => {
  const rows = [
    { completion_cid: 'r1', role: 'task', address: 'x', sample: 'A' },
    { completion_cid: 'r1', role: 'task', address: null, sample: 'A' },
    { completion_cid: 'r1', role: 'task', address: 'ref', sample: 'A' },
    { completion_cid: 'r1', role: 'task', address: 'ref', sample: 'B' },
    { completion_cid: 'r1', role: 'task', address: 'y', sample: null },
    { completion_cid: 'r1', role: 'samplesheet', address: 's', sample: 'A' },
  ]
  const by = inputAddresses(rows)
  assert.deepEqual([...by.get(occurrenceKey({ run: 'r1', id: 'A' }))], ['x'])
  assert.equal(by.has(occurrenceKey({ run: 'r1', id: 'B' })), false)
})

test('matchRefs finds each SampleRef in the runs of its pipeline and keeps the ones no run has', () => {
  const runs = [{ run: 'r1', pipeline: 'p', ids: ['A', 'B'] }, { run: 'r2', pipeline: 'p', ids: ['A'] }, { run: 'r3', pipeline: 'q', ids: ['A'] }]
  const { occurrences, unmatched } = matchRefs([{ pipeline: 'p', id: 'A' }, { pipeline: 'p', id: 'Z' }], runs)
  assert.deepEqual(occurrences, [{ pipeline: 'p', id: 'A', run: 'r1' }, { pipeline: 'p', id: 'A', run: 'r2' }])
  assert.deepEqual(unmatched, [{ pipeline: 'p', id: 'Z' }])
})

test('runsSharingInputs groups runs by shared input files, skipping shared references', () => {
  const rows = [
    { completion_cid: 'r1', role: 'task', address: 'x1', sample: 'A' },
    { completion_cid: 'r2', role: 'task', address: 'x1', sample: 'A' },
    { completion_cid: 'r1', role: 'task', address: 'x2', sample: 'B' },
    { completion_cid: 'r2', role: 'task', address: 'x2', sample: 'B' },
    { completion_cid: 'r3', role: 'task', address: 'x2', sample: 'B' },
    { completion_cid: 'r4', role: 'task', address: 'idx', sample: 'C' },
    { completion_cid: 'r4', role: 'task', address: 'idx', sample: 'D' },
    { completion_cid: 'r5', role: 'task', address: 'idx', sample: 'E' },
    { completion_cid: 'r6', role: 'samplesheet', address: 'x1', sample: 'A' },
    { completion_cid: 'r7', role: 'task', address: 'solo', sample: 'F' },
  ]
  assert.deepEqual(runsSharingInputs(rows), [{ runs: ['r1', 'r2', 'r3'], files: 2 }])
})
