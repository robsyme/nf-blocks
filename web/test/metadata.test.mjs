import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Float, typedDecode } from '../src/typed.js'
import { attrRows, javaDouble, matches, metadataView, predicateRow, PredicateError } from '../src/metadata.js'

// Each right-hand side is Java 21's Double.toString, checked with `java` on 2026-09-25.
const JAVA = [[30.0, '30.0'], [1e10, '1.0E10'], [1e-4, '1.0E-4'], [0.001, '0.001'], [1234567, '1234567.0'],
  [12345678.9, '1.23456789E7'], [-0, '-0.0'], [0, '0.0'], [1.5, '1.5'], [1e7, '1.0E7'], [9999999, '9999999.0'],
  [0.1 + 0.2, '0.30000000000000004'], [1e21, '1.0E21'], [5e-324, '4.9E-324'], [1.7976931348623157e308, '1.7976931348623157E308'],
  [100, '100.0'], [2.5e-3, '0.0025'], [123456.789, '123456.789'], [-1.5e10, '-1.5E10']]

test('javaDouble is Double.toString', () => {
  for (const [x, java] of JAVA) assert.equal(javaDouble(x), java, String(x))
})

// {"a": 30 (unsigned int), "b": 30.0 (float64)}, by hand: an encoder would write 30.0 as an int.
const INT_AND_FLOAT = Uint8Array.from([0xa2, 0x61, 0x61, 0x18, 0x1e, 0x61, 0x62, 0xfb, 0x40, 0x3e, 0, 0, 0, 0, 0, 0])

test('typedDecode keeps a float a float, even when it is integral', () => {
  const v = typedDecode(INT_AND_FLOAT)
  assert.equal(v.a, 30)
  assert.ok(v.b instanceof Float)
  assert.equal(v.b.value, 30)
})

test('attribute rows are what Index.ingestItem writes (Review Focus 3)', () => {
  const value = [{ ...typedDecode(INT_AND_FLOAT), big: new Float(1e10), meta: { strand: 'fwd', tags: ['x', 'y'] }, ok: true,
    none: null, file: { kind: 'Leaf', name: 'A.bam', address: null, size: null, provider: null, reason: 'declined' } }, { other: 1 }]
  const rows = attrRows(metadataView(value)).map(r => [r.path, r.type, r.value, r.truncated]).sort()
  assert.deepEqual(rows, [
    ['a', 'int', '30', 0], ['b', 'float', '30.0', 0], ['big', 'float', '1.0E10', 0],
    ['meta.strand', 'string', 'fwd', 0], ['meta.tags', 'string', 'x', 0], ['meta.tags', 'string', 'y', 0],
    ['none', 'null', null, 0], ['ok', 'bool', 'true', 0],
  ].sort())
})

test('a string past 1024 UTF-8 bytes is stored as its digest', () => {
  const [row] = attrRows({ long: 'a'.repeat(1025) })
  assert.deepEqual(row, { path: 'long', type: 'string', value: 'sha256:4a82297889eb505cf6b5cbdf69977afab4632d6557539782f657bd7dc78091a5', truncated: 1 })
})

test('the metadata view is the item if a map, else its first non-Leaf map', () => {
  assert.deepEqual(metadataView({ s: 1 }), { s: 1 })
  assert.deepEqual(metadataView([{ kind: 'Leaf' }, 'x', { s: 2 }]), { s: 2 })
  assert.equal(metadataView({ kind: 'Leaf' }), null)
  assert.equal(metadataView('scalar'), null)
})

test('predicates normalise the way MetadataView.scalar types a value', () => {
  assert.deepEqual(predicateRow('lane', 'int', '007'), { path: 'lane', type: 'int', value: '7', truncated: 0 })
  assert.deepEqual(predicateRow('depth', 'float', '30'), { path: 'depth', type: 'float', value: '30.0', truncated: 0 })
  assert.deepEqual(predicateRow('ok', 'bool', 'true'), { path: 'ok', type: 'bool', value: 'true', truncated: 0 })
  assert.deepEqual(predicateRow('x', 'null', ''), { path: 'x', type: 'null', value: null, truncated: 0 })
  assert.throws(() => predicateRow('lane', 'int', '1.5'), PredicateError)
  assert.throws(() => predicateRow('ok', 'bool', 'yes'), PredicateError)
  assert.throws(() => predicateRow('x', 'date', '1'), PredicateError)
})

test('matching needs every predicate, and a truncated row never matches', () => {
  const rows = [{ path: 's', type: 'string', value: 'A', truncated: 0 }, { path: 'n', type: 'int', value: '1', truncated: 0 },
    { path: 'l', type: 'string', value: 'sha256:00', truncated: 1 }]
  assert.equal(matches(rows, [predicateRow('s', 'string', 'A'), predicateRow('n', 'int', '1')]), true)
  assert.equal(matches(rows, [predicateRow('s', 'string', 'A'), predicateRow('n', 'int', '2')]), false)
  assert.equal(matches(rows, [{ path: 'l', type: 'string', value: 'sha256:00', truncated: 0 }]), false)
  assert.equal(matches(rows, []), true)
})
