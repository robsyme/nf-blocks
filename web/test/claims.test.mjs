import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { claimState } from '../src/claims.js'

const vectors = JSON.parse(readFileSync(new URL('./fixtures/claim-vectors.json', import.meta.url), 'utf8'))

for (const v of vectors) {
  test(`claim state: ${v.name}`, () => {
    const s = claimState(v.claims)
    assert.deepEqual(s.current.map(c => c.cid), v.expect.current)
    assert.deepEqual(s.names, v.expect.names)
    assert.equal(s.nameConflicted, v.expect.nameConflicted)
    assert.equal(s.deletion, v.expect.deletion)
    assert.equal(s.hidden, v.expect.deletion === 'deleted')
  })
}
