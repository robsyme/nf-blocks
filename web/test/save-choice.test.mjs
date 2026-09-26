import { test } from 'node:test'
import assert from 'node:assert/strict'
import { saveChoice, namingRequest, restoreRequest } from '../src/save-choice.js'

const dry = (o) => ({ address: 'bafyS', exists: false, here: false, names: [], name_claims: [], ...o })

test('a Selection nobody holds is a new save that supersedes nothing', () => {
  assert.deepEqual(saveChoice(dry({})), { state: 'new', names: [], prefill: '', supersedes: [], deletion: 'none', restore: [] })
})

test('held here keeps the open-it-to-rename path', () => {
  assert.equal(saveChoice(dry({ exists: true, here: true, names: ['a'] })).state, 'here')
})

test('held only elsewhere with one name: prefilled, superseding that Claim', () => {
  assert.deepEqual(saveChoice(dry({ exists: true, names: ['from-shared'], name_claims: ['bafyN'] })),
    { state: 'elsewhere', names: ['from-shared'], prefill: 'from-shared', supersedes: ['bafyN'], deletion: 'none', restore: [] })
})

test('held only elsewhere in conflict: no prefill, every current name Claim superseded', () => {
  const c = saveChoice(dry({ exists: true, names: ['a', 'b'], name_claims: ['bafy1', 'bafy2'] }))
  assert.equal(c.prefill, '')
  assert.deepEqual(c.names, ['a', 'b'])
  assert.deepEqual(c.supersedes, ['bafy1', 'bafy2'])
})

test('held only elsewhere with a conflicted name_claims group: no prefill', () => {
  assert.deepEqual(saveChoice(dry({ exists: true, names: ['a'], name_claims: ['bafy1', 'bafy2'] })),
    { state: 'elsewhere', names: ['a'], prefill: '', supersedes: ['bafy1', 'bafy2'], deletion: 'none', restore: [] })
})

test('a missing here reads as held here, so an older server never looks elsewhere', () => {
  const { here, ...rest } = dry({ exists: true, names: ['a'] })
  assert.equal(saveChoice(rest).state, 'here')
})

test('a cleared name writes no Claim; an edited one still supersedes', () => {
  const c = saveChoice(dry({ exists: true, names: ['x'], name_claims: ['bafyN'] }))
  assert.equal(namingRequest(c, '   '), null)
  assert.deepEqual(namingRequest(c, ' mine '), { name: 'mine', supersedes: ['bafyN'] })
  assert.deepEqual(namingRequest(saveChoice(dry({})), 'first'), { name: 'first', supersedes: [] })
})

test('held only elsewhere and deleted there: restore supersedes the deletion', () => {
  const c = saveChoice(dry({ exists: true, names: ['n'], name_claims: ['bafyN'], deletion: 'deleted', deletion_claims: ['bafyD'] }))
  assert.equal(c.state, 'elsewhere')
  assert.equal(c.deletion, 'deleted')
  assert.deepEqual(c.restore, ['bafyD'])
  assert.deepEqual(restoreRequest(c), { supersedes: ['bafyD'] })
})

test('a conflicted deletion elsewhere restores over every current deletion Claim', () => {
  const c = saveChoice(dry({ exists: true, deletion: 'conflicted', deletion_claims: ['bafy1', 'bafy2'] }))
  assert.deepEqual(restoreRequest(c), { supersedes: ['bafy1', 'bafy2'] })
})

test('restoring does not depend on the name: a cleared name still restores', () => {
  const c = saveChoice(dry({ exists: true, names: ['n'], name_claims: ['bafyN'], deletion: 'deleted', deletion_claims: ['bafyD'] }))
  assert.equal(namingRequest(c, ''), null)
  assert.deepEqual(restoreRequest(c), { supersedes: ['bafyD'] })
})

test('held only elsewhere and live: a plain copy, nothing to restore', () => {
  const c = saveChoice(dry({ exists: true, names: ['n'], name_claims: ['bafyN'] }))
  assert.equal(c.deletion, 'none')
  assert.equal(restoreRequest(c), null)
})

test('held here and deleted elsewhere: the here path carries the deletion, and never restores', () => {
  const c = saveChoice(dry({ exists: true, here: true, deletion: 'deleted', deletion_claims: ['bafyD'] }))
  assert.equal(c.state, 'here')
  assert.equal(c.deletion, 'deleted')
  assert.equal(restoreRequest(c), null)
})
