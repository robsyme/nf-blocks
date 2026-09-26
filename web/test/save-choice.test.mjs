import { test } from 'node:test'
import assert from 'node:assert/strict'
import { saveChoice, namingRequest } from '../src/save-choice.js'

const dry = (o) => ({ address: 'bafyS', exists: false, here: false, names: [], name_claims: [], ...o })

test('a Selection nobody holds is a new save that supersedes nothing', () => {
  assert.deepEqual(saveChoice(dry({})), { state: 'new', names: [], prefill: '', supersedes: [] })
})

test('held here keeps the open-it-to-rename path', () => {
  assert.equal(saveChoice(dry({ exists: true, here: true, names: ['a'] })).state, 'here')
})

test('held only elsewhere with one name: prefilled, superseding that Claim', () => {
  assert.deepEqual(saveChoice(dry({ exists: true, names: ['from-shared'], name_claims: ['bafyN'] })),
    { state: 'elsewhere', names: ['from-shared'], prefill: 'from-shared', supersedes: ['bafyN'] })
})

test('held only elsewhere in conflict: no prefill, every current name Claim superseded', () => {
  const c = saveChoice(dry({ exists: true, names: ['a', 'b'], name_claims: ['bafy1', 'bafy2'] }))
  assert.equal(c.prefill, '')
  assert.deepEqual(c.names, ['a', 'b'])
  assert.deepEqual(c.supersedes, ['bafy1', 'bafy2'])
})

test('a cleared name writes no Claim; an edited one still supersedes', () => {
  const c = saveChoice(dry({ exists: true, names: ['x'], name_claims: ['bafyN'] }))
  assert.equal(namingRequest(c, '   '), null)
  assert.deepEqual(namingRequest(c, ' mine '), { name: 'mine', supersedes: ['bafyN'] })
  assert.deepEqual(namingRequest(saveChoice(dry({})), 'first'), { name: 'first', supersedes: [] })
})
