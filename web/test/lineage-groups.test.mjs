// web/test/lineage-groups.test.mjs
// groupLineage over plain objects: inputs and their Origin reads as run groups, lines and rows, with no DOM.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { countsText, groupLineage, itemsText, originKey } from '../src/lineage.js'

const label = (run_name, completion, output = 'people') => ({ run_name, output, completion, status: 'succeeded' })
const FAB = label('fabulous_bartik', 'run-fab')
const SLEEPY = label('sleepy_williams', 'run-sleepy')
const KEEN = label('keen_hopper', 'run-keen')
const LABELS = { 'coll-fab': FAB, 'coll-sleepy': SLEEPY, 'coll-keen': KEEN, 'coll-fab-2': label('fabulous_bartik', 'run-fab', 'friends') }
const from = (item_cid, collection_cid = 'coll-fab', selection_cid = 'sel-community') => ({ item_cid, collection_cid, selection_cid })
const input = (position, name, origins = []) => ({ position, role: 'path', name, address: `bafk-${name}`, size: 10, sample: null, origins })
/** The reads readOrigin would make: every item held in the open member `lab`, each collection labelled from LABELS. */
const readsOf = (inputs) => new Map(inputs.flatMap(i => i.origins ?? []).map(o => [originKey(o), { itemCid: o.item_cid, collection: o.collection_cid,
  selection: o.selection_cid, holder: 'lab', reader: null, title: o.item_cid, label: LABELS[o.collection_cid] ?? null, names: ['Community'] }]))
/** Each group's key with its lines' keys and their rows' positions (a leaf row by its name). */
const shape = ({ groups }) => groups.map(g => [g.key, g.lines.map(l => [l.key, l.rows.map(r => r.position ?? r.leaf)])])

test('one upstream run: one group, one line, a row per position, the items counted', () => {
  const inputs = ['Amina', 'Dev', 'Femi', 'Greta'].map((n, i) => input(i, `${n}.dat`, [from(n)]))
  const out = groupLineage(inputs, readsOf(inputs))
  assert.deepEqual(shape(out), [['run:run-fab', [['coll-fab:sel-community', [0, 1, 2, 3]]]]])
  assert.equal(out.groups[0].kind, 'run')
  assert.equal(out.groups[0].completion, 'run-fab')
  assert.equal(out.groups[0].label, FAB)
  assert.equal(out.groups[0].lines[0].items, 4)
  assert.deepEqual(out.groups[0].lines[0].names, ['Community'])
  assert.deepEqual([out.runs, out.inputs], [1, 4])
})

test('groups by the lowest position they hold, rows by position; a row under two runs names the other', () => {
  const inputs = [input(2, 'Bo.dat', [from('Bo')]), input(0, 'Cy.dat', [from('Cy', 'coll-sleepy')]), input(1, 'Ana.dat', [from('Ana'), from('Ana', 'coll-sleepy')])]
  const out = groupLineage(inputs, readsOf(inputs))
  assert.deepEqual(shape(out), [['run:run-sleepy', [['coll-sleepy:sel-community', [0, 1]]]], ['run:run-fab', [['coll-fab:sel-community', [1, 2]]]]])
  const ana = (g) => out.groups[g].lines[0].rows.find(r => r.position === 1)
  assert.deepEqual(ana(0).also, [{ completion: 'run-fab', label: FAB, holder: 'lab' }])
  assert.deepEqual(ana(1).also, [{ completion: 'run-sleepy', label: SLEEPY, holder: 'lab' }])
  assert.deepEqual(out.groups[0].lines[0].rows[0].also, [])
  assert.deepEqual([out.runs, out.inputs], [2, 3])
})

test('an input three runs name lists the two others, in group order', () => {
  const inputs = [input(0, 'Dev.dat', [from('Dev')]), input(1, 'Ana.dat', [from('Ana'), from('Ana', 'coll-sleepy'), from('Ana', 'coll-keen')])]
  const out = groupLineage(inputs, readsOf(inputs))
  assert.deepEqual(out.groups.map(g => g.key), ['run:run-fab', 'run:run-sleepy', 'run:run-keen'], 'a tie on position keeps Origin order')
  assert.deepEqual(out.groups[0].lines[0].rows[1].also.map(a => a.completion), ['run-sleepy', 'run-keen'])
  assert.deepEqual(out.groups[2].lines[0].rows[0].also.map(a => a.completion), ['run-fab', 'run-sleepy'])
})

test('two outputs of one run make two lines, and neither row names its own run', () => {
  const inputs = [input(0, 'Dev.dat', [from('Dev'), from('Dev', 'coll-fab-2')])]
  const out = groupLineage(inputs, readsOf(inputs))
  assert.deepEqual(shape(out), [['run:run-fab', [['coll-fab:sel-community', [0]], ['coll-fab-2:sel-community', [0]]]]])
  assert.deepEqual(out.groups[0].lines.flatMap(l => l.rows.map(r => r.also)), [[], []])
  assert.equal(out.groups[0].lines[1].label.output, 'friends')
  assert.deepEqual([out.runs, out.inputs], [1, 1])
})

test('two positions reading one item count it once', () => {
  const inputs = [input(0, 'Dev.dat', [from('Dev')]), input(1, 'again.dat', [from('Dev')])]
  const out = groupLineage(inputs, readsOf(inputs))
  assert.equal(out.groups[0].lines[0].items, 1)
  assert.deepEqual(out.groups[0].lines[0].rows.map(r => r.input.name), ['Dev.dat', 'again.dat'])
})

test('no collection goes in a query group by its Selection, an unreadable run label in a collection group, both after the run groups; an Origin naming neither goes in query:-', () => {
  const inputs = [input(0, 'Q.dat', [from('Q', null, 'sel-friends')]), input(1, 'U.dat', [from('U', 'coll-gone')]),
    input(2, 'F.dat', [from('F')]), input(3, 'N.dat', [from('N', null, null)])]
  const out = groupLineage(inputs, readsOf(inputs))
  assert.deepEqual(shape(out), [['run:run-fab', [['coll-fab:sel-community', [2]]]], ['query:sel-friends', [['-:sel-friends', [0]]]],
    ['collection:coll-gone', [['coll-gone:sel-community', [1]]]], ['query:-', [['-:-', [3]]]]])
  assert.deepEqual(out.groups.map(g => g.kind), ['run', 'query', 'collection', 'query'])
  assert.equal(out.groups[1].selection, 'sel-friends')
  assert.deepEqual(out.groups[1].names, ['Community'])
  assert.deepEqual([out.runs, out.inputs], [1, 4])
})

test('an Origin with no read is grouped as nothing read', () => {
  const inputs = [input(0, 'Dev.dat', [from('Dev')])]
  const out = groupLineage(inputs, new Map())
  assert.deepEqual(shape(out), [['collection:coll-fab', [['coll-fab:sel-community', [0]]]]])
  const { read } = out.groups[0].lines[0].rows[0]
  assert.deepEqual([read.itemCid, read.holder, read.label, read.title, read.names], ['Dev', null, null, null, []])
  assert.equal(out.runs, 0)
})

test('Origin-less inputs and never-published leaves make the last group; a leaf named like such an input is that row', () => {
  const inputs = [input(0, 'a.dat', [from('A')]), { ...input(1, 'genome.fa'), origins: undefined }, input(2, 'b.dat')]
  const out = groupLineage(inputs, readsOf(inputs), ['b.dat', 'extra.txt', 'a.dat'])
  assert.deepEqual(shape(out), [['run:run-fab', [['coll-fab:sel-community', [0]]]], ['path', [['path', [1, 2, 'extra.txt', 'a.dat']]]]])
  const rows = out.groups[1].lines[0].rows
  assert.deepEqual(rows.map(r => r.leaf), [null, 'b.dat', 'extra.txt', 'a.dat'])
  assert.deepEqual(rows.map(r => r.input?.name ?? null), ['genome.fa', 'b.dat', null, null])
  assert.equal(out.groups[1].kind, 'path')
  assert.equal(out.groups[1].lines[0].items, 0)
  assert.deepEqual([out.runs, out.inputs], [1, 3], 'leaves are not inputs')
})

test('no inputs and no leaves: no groups; leaves alone: the path group, no inputs', () => {
  assert.deepEqual(groupLineage([], new Map()), { groups: [], runs: 0, inputs: 0 })
  const out = groupLineage([], new Map(), ['in.fq'])
  assert.deepEqual(shape(out), [['path', [['path', ['in.fq']]]]])
  assert.equal(out.inputs, 0)
})

test('countsText', () => {
  assert.equal(countsText(1, 4), '1 upstream run · 4 inputs')
  assert.equal(countsText(2, 9), '2 upstream runs · 9 inputs')
  assert.equal(countsText(1, 1), '1 upstream run · 1 input')
  assert.equal(countsText(0, 2), '2 inputs')
  assert.equal(countsText(0, 1), '1 input')
  assert.equal(countsText(0, 0), null)
})

test('itemsText never says of 0', () => {
  assert.equal(itemsText(4, 9), '4 of 9 items')
  assert.equal(itemsText(1, 1), '1 of 1 item')
  assert.equal(itemsText(4, null), '4 items')
  assert.equal(itemsText(1, null), '1 item')
  assert.equal(itemsText(2, undefined), '2 items')
  assert.equal(itemsText(3, 0), '3 items')
})
