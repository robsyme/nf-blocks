// web/test/slots.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { fileSlots } from '../src/slots.js'
import { block, rawCid } from './fixture.mjs'

const leaf = (name, address, size = 10, reason = null) => ({ kind: 'Leaf', name, address, size, reason })
const bam = rawCid('bam'); const bai = rawCid('bai'); const st = rawCid('stats')

test('record: one column per top-level key, dotted paths, list positions in brackets', () => {
  const slots = fileSlots({ id: 'WT_REP1', meta: { id: 'WT_REP1' }, bam: leaf('a.bam', bam), bai: leaf('a.bai', bai),
    samtools: { stats: leaf('a.stats', st) }, ballgown: [leaf('e.ctab', bam), leaf('t.ctab', bai)] })
  assert.deepEqual(slots.map(s => [s.column, s.path]), [
    ['bam', 'bam'], ['bai', 'bai'], ['samtools', 'samtools.stats'], ['ballgown', 'ballgown[0]'], ['ballgown', 'ballgown[1]']])
  assert.deepEqual(slots[0], { column: 'bam', path: 'bam', name: 'a.bam', size: 10, address: bam.toString(), dir: false, reason: null })
})

test('tuple: one column per position that holds a Leaf; the meta map holds none', () => {
  const slots = fileSlots([{ sample: 'A', lane: 1 }, leaf('A.bam', bam), [leaf('A.bai', bai)]])
  assert.deepEqual(slots.map(s => [s.column, s.path]), [['[1]', '[1]'], ['[2]', '[2][0]']])
})

test('a Leaf inside a tuple meta map gives position [0] a column (Review Focus 3)', () => {
  assert.deepEqual(fileSlots([{ sample: 'A', ref: leaf('ref.fa', bam) }, leaf('A.bam', bai)]).map(s => s.path), ['[0].ref', '[1]'])
})

test('a bare Leaf is column [0]', () => {
  assert.deepEqual(fileSlots(leaf('x.txt', bam)).map(s => [s.column, s.path]), [['[0]', '[0]']])
})

test('empty lists, nulls and scalars give no slot (Review Focus 1)', () => {
  assert.deepEqual(fileSlots({ id: 'x', wig: [], junction: null, n: 3 }), [])
  assert.deepEqual(fileSlots(undefined), [])
})

test('an address-less Leaf is a slot with its reason; a dag-cbor address, as object or text, is a directory (Review Focus 2)', () => {
  const [declined] = fileSlots({ json_info: leaf(null, null, null, 'declined') })
  assert.deepEqual(declined, { column: 'json_info', path: 'json_info', name: null, size: null, address: null, dir: false, reason: 'declined' })
  const dir = block({ kind: 'DirectoryManifest' }).cid
  for (const given of [dir, dir.toString()]) {
    const [s] = fileSlots({ qualimap: leaf('qc', given) })
    assert.equal(s.dir, true)
    assert.equal(s.address, dir.toString())
  }
})
