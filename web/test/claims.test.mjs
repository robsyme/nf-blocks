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
    if ('retain' in v.expect) {
      assert.equal(s.retain, v.expect.retain)
      assert.equal(s.released, v.expect.retain === 'released')
    }
    if ('pins' in v.expect) assert.deepEqual(s.pins.map(p => p.note), v.expect.pins)
    if ('pinned' in v.expect) assert.equal(s.pinned, v.expect.pinned)
  })
}

const RETAIN = { cid: 'bafyreirel', verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [],
  asserted_by: 'ada.lovelace@example.org', timestamp: '2026-10-24T11:00:00Z' }

test('releaseClaim is the current retain claim of a released subject, else null', () => {
  const released = claimState([RETAIN])
  assert.equal(released.releaseClaim.cid, RETAIN.cid)
  assert.equal(released.releaseClaim.asserted_by, 'ada.lovelace@example.org')
  assert.equal(released.releaseClaim.timestamp, '2026-10-24T11:00:00Z')
  assert.equal(claimState([]).releaseClaim, null)
  const restored = claimState([RETAIN, { cid: 'bafyreiundo', verb: 'del', attribute: 'retain', value: null, supersedes: [RETAIN.cid] }])
  assert.equal(restored.releaseClaim, null)
  const conflicted = claimState([RETAIN, { ...RETAIN, cid: 'bafyreirel2' }])
  assert.equal(conflicted.releaseClaim, null)
})
