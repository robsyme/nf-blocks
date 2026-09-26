// views.js needs a DOM (`document`) to build its nodes, which this Node test
// runner does not have, so only the pure helpers extracted for the page
// minors (ticket 11) are unit-tested here; the rendering itself is covered
// by the Gate's browser tier.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { copyOutcome, copyText } from '../src/views.js'

test('copyText: a via-less member copies its bare address; a via\'d one copies cas://<via>/<address>', () => {
  assert.equal(copyText('item1', '-'), 'item1')
  assert.equal(copyText('item1', 'coll1'), 'cas://coll1/item1')
})

test('copyOutcome: "Copied" on a successful write, "Copy failed" on a rejection or a missing clipboard', async () => {
  assert.equal(await copyOutcome(undefined, 'x'), 'Copy failed')
  assert.equal(await copyOutcome({ writeText: async () => {} }, 'x'), 'Copied')
  assert.equal(await copyOutcome({ writeText: async () => { throw new Error('nope') } }, 'x'), 'Copy failed')
})
