import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { extractSchema } from '../schema-gen.mjs'
import { validBlock, validLeaf, validLeafV1 } from '../src/schema.js'
import { buildMember } from './fixture.mjs'
import * as dagCbor from '@ipld/dag-cbor'

test('DESIGN.md §6 parses to 23 types', () => {
  const schema = extractSchema(readFileSync(new URL('../../DESIGN.md', import.meta.url), 'utf8'))
  assert.equal(Object.keys(schema.types).length, 23)
})

test('every fixture block is valid, and broken ones are not', async () => {
  const { blocks, runs } = await buildMember()
  for (const bytes of blocks.values()) assert.equal(validBlock(dagCbor.decode(bytes)), true)
  const rc = dagCbor.decode(blocks.get(runs.R1.completion))
  assert.equal(validBlock({ ...rc, status: 'done' }), false)
  assert.equal(validBlock((({ finished_at, ...rest }) => rest)(rc)), false)
  assert.equal(validBlock({ ...rc, kind: 'Nope' }), false)
})

test('a schema-2 Leaf has no provider; a schema-1 Leaf keeps its own', () => {
  assert.equal(validLeaf({ kind: 'Leaf', name: 'a', address: null, size: null, reason: 'declined' }), true)
  assert.equal(validLeaf({ kind: 'Leaf', name: 'a', address: null, size: null, provider: 'head-node', reason: 'declined' }), false)
  assert.equal(validLeafV1({ kind: 'Leaf', name: 'a', address: null, size: null, provider: 'head-node', reason: 'declined' }), true)
  assert.equal(validLeafV1({ kind: 'Leaf', name: 'a', address: null, size: null, provider: 'laptop', reason: null }), false)
})

test('a RunManifest config is text, or a map in a block written before 2026-09-27', async () => {
  const { blocks, runs } = await buildMember()
  const rc = dagCbor.decode(blocks.get(runs.R1.completion))
  const manifest = dagCbor.decode(blocks.get(rc.run.toString()))
  assert.equal(validBlock({ ...manifest, config: "process {\n    ext.args = { \"--x ${task.cpus}\" }\n}\n" }), true)
  assert.equal(validBlock({ ...manifest, config: { process: { cpus: 2 } } }), true)
  assert.equal(validBlock({ ...manifest, config: 7 }), false)
  assert.equal(validBlock({ ...manifest, config: null }), false)
})

test('an OutputCollection with an index validates, and one without still does', async () => {
  const m = await buildMember()
  const collection = m.valueOfKind('OutputCollection')
  assert.equal(validBlock(collection), true)
  const file = collection.items[0]                        // any real CID will do as the index file's address
  const leaf = { kind: 'Leaf', name: 'index.json', address: file, size: 312, reason: null }
  assert.equal(validBlock({ ...collection, index: { leaf, path: 'multiqc/index.json' } }), true)
  const missing = { kind: 'Leaf', name: 'index.csv', address: null, size: null, reason: 'never_published' }
  assert.equal(validBlock({ ...collection, index: { leaf: missing, path: 'bad/index.csv' } }), true)
  assert.equal(validBlock({ ...collection, index: { path: 'no-leaf' } }), false)
})

test('a RunCompletion anomalies map may carry unjoined, and still validates without it', async () => {
  const m = await buildMember()
  const completion = m.valueOfKind('RunCompletion')
  assert.equal(validBlock(completion), true)
  assert.equal(validBlock({ ...completion, anomalies: { ...completion.anomalies, unjoined: 2 } }), true)
})
