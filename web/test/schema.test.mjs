import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { extractSchema } from '../schema-gen.mjs'
import { validBlock, validLeaf } from '../src/schema.js'
import { buildMember } from './fixture.mjs'
import * as dagCbor from '@ipld/dag-cbor'

test('DESIGN.md §6 parses to 19 types', () => {
  const schema = extractSchema(readFileSync(new URL('../../DESIGN.md', import.meta.url), 'utf8'))
  assert.equal(Object.keys(schema.types).length, 19)
})

test('every fixture block is valid, and broken ones are not', async () => {
  const { blocks, runs } = await buildMember()
  for (const bytes of blocks.values()) assert.equal(validBlock(dagCbor.decode(bytes)), true)
  const rc = dagCbor.decode(blocks.get(runs.R1.completion))
  assert.equal(validBlock({ ...rc, status: 'done' }), false)
  assert.equal(validBlock((({ finished_at, ...rest }) => rest)(rc)), false)
  assert.equal(validBlock({ ...rc, kind: 'Nope' }), false)
  assert.equal(validLeaf({ kind: 'Leaf', name: 'a', address: null, size: null, provider: null, reason: 'declined' }), true)
  assert.equal(validLeaf({ kind: 'Leaf', name: 'a', address: null, size: null, provider: 'laptop', reason: null }), false)
})
