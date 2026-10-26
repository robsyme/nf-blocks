// web/test/storename.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { storeKey, storeName } from '../src/storename.js'

const members = [{ alias: 'roblab', base: 'roblab/', writable: true }, { alias: 'org-s3', base: 'org/' }, { alias: 'ana-hpc', base: 'ana/' },
  { alias: 'fourth', base: 'four/' }]

test('storeName: the member alias, else the last part of the store URL, else its host; never the whole URL', () => {
  assert.equal(storeName({ base: 'http://localhost:8771/roblab/', member: 'roblab', members }), 'roblab')
  assert.equal(storeName({ base: 'http://localhost:8771/stores/my%20lab/', member: null, members: null }), 'my lab')
  assert.equal(storeName({ base: 'http://localhost:8771/', member: null, members: null }), 'localhost:8771')
  assert.equal(storeName({ base: 'not a url', member: null, members: null }), 'this store')
})

test('storeKey: a store colour by the member\'s place in members.json; the one store read is a', () => {
  const store = { base: 'http://x/roblab/', member: 'org-s3', members }
  assert.equal(storeKey(store), 'b', 'the open member by default')
  assert.equal(storeKey(store, 'roblab'), 'a')
  assert.equal(storeKey(store, 'ana-hpc'), 'c')
  assert.equal(storeKey(store, 'fourth'), 'a', 'the three colours repeat')
  assert.equal(storeKey(store, 'unknown'), 'a')
  assert.equal(storeKey({ base: 'http://x/', member: null, members: null }), 'a')
})

test('storeKey: a fourth member and beyond cycle a, b, c', () => {
  const many = Array.from({ length: 7 }, (_, i) => ({ alias: `m${i}`, base: `m${i}/` }))
  const store = { base: 'http://x/', member: 'm0', members: many }
  assert.deepEqual(many.map(m => storeKey(store, m.alias)), ['a', 'b', 'c', 'a', 'b', 'c', 'a'])
})
