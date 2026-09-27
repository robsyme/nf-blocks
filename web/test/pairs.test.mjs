import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { Float } from '../src/typed.js'
import { filterHref, labelPaths, labelText, pairsNode, pairsOf, pillsOf, valueText } from '../src/pairs.js'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { loadSqlite, makeDb, snapshotDb } from './helpers.mjs'

const leaf = (name) => ({ kind: 'Leaf', name, address: null, size: null, provider: null, reason: 'declined' })
const preview = (view, files = []) => ({ pairs: pairsOf(view), files })

test('pairsOf: one entry per path, sorted by path, a list\'s values together, floats typed as the index types them', () => {
  const pairs = pairsOf({ sample: 'A', lane: 1, depth: new Float(30), library: { kit: 'truseq' }, ids: ['x', 'y'],
    ok: true, none: null, reads: leaf('A.bam') })
  assert.deepEqual(pairs, [
    { path: 'depth', type: 'float', values: ['30.0'], types: ['float'] },
    { path: 'ids', type: 'string', values: ['x', 'y'], types: ['string', 'string'] },
    { path: 'lane', type: 'int', values: ['1'], types: ['int'] },
    { path: 'library.kit', type: 'string', values: ['truseq'], types: ['string'] },
    { path: 'none', type: 'null', values: [null], types: ['null'] },
    { path: 'ok', type: 'bool', values: ['true'], types: ['bool'] },
    { path: 'sample', type: 'string', values: ['A'], types: ['string'] },
  ])
})

test('pairsOf: a path whose values differ in type is mixed, each value keeping its own; a truncated value is left out', () => {
  assert.deepEqual(pairsOf({ tags: ['2', 2], long: 'a'.repeat(1025) }),
    [{ path: 'tags', type: 'mixed', values: ['2', '2'], types: ['string', 'int'] }])
})

test('an item with no Meta Map has no pairs and no pills, and its label is its file names (Review Focus 1)', () => {
  const bare = preview(null, ['A.bam', 'A.bam.bai'])
  assert.deepEqual(bare.pairs, [])
  assert.equal(pairsNode(bare.pairs, null), null)
  assert.deepEqual(labelPaths([bare]), [])
  assert.equal(labelText(bare, labelPaths([bare])), 'A.bam, A.bam.bai')
  assert.equal(labelText(preview(null), []), '')
})

test('an item whose Meta Map holds only lists keeps its pills, but its label is still its file names (Review Focus 1)', () => {
  const ids = preview({ ids: ['a', 'b', 'c'] }, ['ids.txt'])
  assert.equal(ids.pairs.length, 1)
  assert.deepEqual(labelPaths([ids]), [])
  assert.equal(labelText(ids, labelPaths([ids])), 'ids.txt')
})

test('the label: string paths first, then most distinct values, ties by path, list-valued paths skipped (ticket 10 Q3)', () => {
  const gate = ['A', 'B', 'C'].map(s => preview({ sample: s, lane: 1, library: { kit: 'truseq' }, ids: ['x', 'y'] }, [`${s}.bam`]))
  const paths = labelPaths(gate)
  assert.deepEqual(paths, ['sample', 'library.kit', 'lane'])
  assert.equal(labelText(gate[2], paths), 'C · truseq · 1')
  assert.deepEqual(labelPaths(gate, 1), ['sample'])
  const people = [['Femi', 'Adeyemi', 'Lagos', 'Africa'], ['Ana', 'Silva', 'Lisbon', 'Europe'], ['Bo', 'Chen', 'Lagos', 'Africa']]
    .map(([firstName, lastName, city, region]) => preview({ person: { firstName, lastName, location: { city, region }, age: 40 } }))
  const named = labelPaths(people)
  assert.deepEqual(named, ['person.firstName', 'person.lastName', 'person.location.city'])
  assert.equal(labelText(people[0], named), 'Femi · Adeyemi · Lagos')
  // A path that is a list in any loaded preview is not a label path, even where it is a single value.
  assert.deepEqual(labelPaths([preview({ tag: 'x' }), preview({ tag: ['x', 'y'] })]), [])
})

test('a string that reads as a number is shown quoted; null is shown as null', () => {
  assert.equal(valueText('string', '2'), '"2"')
  assert.equal(valueText('string', '-1.5e3'), '"-1.5e3"')
  assert.equal(valueText('string', '2a'), '2a')
  assert.equal(valueText('string', ''), '')
  assert.equal(valueText('int', '2'), '2')
  assert.equal(valueText('float', '1.0E10'), '1.0E10')
  assert.equal(valueText('null', null), 'null')
})

test('a pill: the last path segment as its key, a list\'s values in one pill, the value classed by type', () => {
  const [kit] = pillsOf(pairsOf({ library: { kit: 'truseq' } }), null)
  assert.deepEqual([kit.path, kit.key], ['library.kit', 'kit'])
  const [ids] = pillsOf(pairsOf({ ids: ['x', 'y'] }), null)
  assert.deepEqual(ids.values.map(v => v.text), ['x', 'y'])
  const classes = pillsOf(pairsOf({ a: 1, b: new Float(2.5), c: true, d: false, e: null, f: 's' }), null).map(p => p.values[0].cls)
  assert.deepEqual(classes, ['pv-num', 'pv-num', 'pv-true', 'pv-false', 'pv-null', 'pv-str'])
  const [off] = pillsOf(pairsOf({ ok: false }), null)
  assert.deepEqual(off.values[0], { text: 'false', type: 'bool', cls: 'pv-false', href: null, title: 'ok is false' })
})

test('a filter link adds its pair to the current conditions once, and needs a run and an output', () => {
  const target = { completion: 'run1', output: 'my reads', where: [['sample', 'string', 'A']] }
  const at = (where) => `#/items/run1/my%20reads?where=${encodeURIComponent(JSON.stringify(where))}`
  assert.equal(filterHref(target, 'lane', 'int', '1'), at([['sample', 'string', 'A'], ['lane', 'int', '1']]))
  assert.equal(filterHref(target, 'sample', 'string', 'A'), at([['sample', 'string', 'A']]))
  assert.equal(filterHref(null, 'lane', 'int', '1'), null)
  assert.equal(filterHref({ completion: null, output: 'x', where: [] }, 'lane', 'int', '1'), null)
  const [none] = pillsOf(pairsOf({ none: null }), { completion: 'r', output: 'o', where: [] })
  assert.deepEqual(none.values[0], { text: 'null', type: 'null', cls: 'pv-null',
    href: `#/items/r/o?where=${encodeURIComponent('[["none","null",null]]')}`, title: 'list the items of o in this run where none is null' })
})

test('the string "2" and the integer 2 draw differently, and each filter link finds only its own item (Review Focus 5)', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, [readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'),
    "INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES ('run1', 'p', 'R1', 'succeeded', 0, '2026-09-01T00:00:00.000Z')",
    "INSERT INTO collection(collection_cid, kind, completion_cid, output_name) VALUES ('coll', 'output', 'run1', 'reads')",
    "INSERT INTO collection_item(collection_cid, item_cid) VALUES ('coll', 'itemStr'), ('coll', 'itemInt')",
    "INSERT INTO item_attr VALUES ('itemStr', 'n', 'string', '2', 0), ('itemInt', 'n', 'int', '2', 0)"])
  const ex = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(bytes),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: async () => new Response('', { status: 404 }) }),
    listFn: async () => ({ names: [], readable: true }) })
  const target = { completion: 'run1', output: 'reads', where: [] }
  const [str] = pillsOf(pairsOf({ n: '2' }), target)
  const [int] = pillsOf(pairsOf({ n: 2 }), target)
  assert.deepEqual([str.key, str.values[0].text, str.values[0].cls], ['n', '"2"', 'pv-str'])
  assert.deepEqual([int.key, int.values[0].text, int.values[0].cls], ['n', '2', 'pv-num'])
  const whereOf = (href) => JSON.parse(decodeURIComponent(href.match(/^#\/items\/run1\/reads\?where=(.*)$/)[1]))
  assert.deepEqual(whereOf(str.values[0].href), [['n', 'string', '2']])
  assert.deepEqual(whereOf(int.values[0].href), [['n', 'int', '2']])
  assert.deepEqual((await ex.items('run1', 'reads', whereOf(str.values[0].href))).items, ['itemStr'])
  assert.deepEqual((await ex.items('run1', 'reads', whereOf(int.values[0].href))).items, ['itemInt'])
})
