import { test } from 'node:test'
import assert from 'node:assert/strict'
import { blockPath, verifies } from '../src/cid.js'

const EMPTY_MAP = 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'   // DESIGN.md §3 vector
const HELLO = 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'

test('DESIGN.md §3 vectors verify, and a changed byte does not', () => {
  assert.equal(verifies(EMPTY_MAP, Uint8Array.of(0xa0)), true)
  assert.equal(verifies(HELLO, new TextEncoder().encode('hello\n')), true)
  assert.equal(verifies(HELLO, new TextEncoder().encode('hellO\n')), false)
  assert.equal(verifies(EMPTY_MAP, new TextEncoder().encode('hello\n')), false)
})

test('hashing does not need crypto.subtle (a page on plain http from a LAN host)', () => {
  // Node defines subtle on Crypto.prototype; an own property shadows it until deleted.
  Object.defineProperty(globalThis.crypto, 'subtle', { value: undefined, configurable: true })
  try {
    assert.equal(globalThis.crypto.subtle, undefined)
    assert.equal(verifies(EMPTY_MAP, Uint8Array.of(0xa0)), true)
  } finally {
    delete globalThis.crypto.subtle
  }
})

test('the block path is the last two characters of the cid', () => {
  assert.equal(blockPath(EMPTY_MAP), `blocks/ua/${EMPTY_MAP}`)
})
